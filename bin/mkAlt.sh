#!/bin/sh
#PBS -S /bin/sh
#PBS -N mkAltSky_@FILTER@_@ID@
#PBS -o @IDENT@.out            
#PBS -j oe
#PBS -l nodes=1:ppn=7,walltime=48:00:00
#-----------------------------------------------------------------------------
# module: mkAltSky script
# wrapper for mkAltSky.py: for each frame, combine images obtained close in time
# using a median filter to remove the stars in order to build a pure sky image
# that will later be subtracted from the source image.
##
# 17.aug/19: 
# - created from subSky.sh in order to remove the (simple) sky subtraction; 
# - also adapted to work on "remote" nodes
# 10.may.21:
# - misc adaptation and add'l checks for DR5
# - including increase of ppn to 7
# 14.apr.23:
# - minor fixes for DR6
#-----------------------------------------------------------------------------
set -u  
# paths
export PATH="~/bin:$PATH:/softs/dfits/bin:/softs/astromatic/bin"
export PYTHONPATH="/home/moneti/uvis/python:/home/moneti/uvis/python_lib"

# add python and UltraVista scripts and libs
module () {  eval $(/usr/bin/modulecmd bash $*); }
module purge; module load intelpython/2

uvis=/home/moneti/softs/uvis-pipe # top UltraVista code dir
bindir=$uvis/bin                  # pipeline modules
pydir=$uvis/python                # python scripts
confdir=$uvis/config              # config dir
errcode=0

ec() { echo "$(date "+[%d.%h.%y %T"]) $1 "; }    # echo with date
mycd() { \cd $1; ec " --> $PWD"; }               # cd with message
sdate=$(date "+%s")

#-----------------------------------------------------------------------------
# Setup
#-----------------------------------------------------------------------------
module=mkAltSky                      # w/o .sh extension

# check  if run via shell or via qsub: 
if [[ "$0" =~ "$module" ]]; then
    echo "$module: running as shell script "
    list=$1
    WRK=$WRK
    FILTER=$FILTER
	if [[ "${@: -1}" =~ 'dry' ]]; then dry=T; else dry=F; fi
else
    echo "$module: running via qsub (from pipeline)"
    dry=@DRY@
    list=@LIST@
    FILTER=@FILTER@
    WRK=@WRK@
fi

#-----------------------------------------------------------------------------
# The REAL work ... done in temporary workdir
#-----------------------------------------------------------------------------

datadir=$WRK/images              # reference dir
rhost=$(echo $WRK | cut -c 2-4)  # host of $WRK

# build work dir name: 
dirname=$(echo $list | cut -d\. -f1)
whost=$(hostname)   #; echo "DEBUG: ref/work hosts: $rhost  $whost"

if [[ $whost == 'n08' ]] || [[ $whost == 'n17' ]]; then
    workdir=/${whost}data/${dirname}_$FILTER     # node with small scratch
else                        
    workdir=/scratch/${dirname}_$FILTER          # other node
fi


if [ ! -d $datadir ];       then echo "ERROR: $WRK/images not found ... quitting"; exit 5; fi
if [ ! -s $datadir/$list ]; then echo "ERROR: $list not found in $WRK/images ... quitting"; exit 5; fi

ec "## Working on $(hostname); data on $rhost; work dir is $workdir"

mycd $datadir
nl=$(cat $datadir/$list | wc -l)
skylist=${list%.lst}.skylst

echo "=================================================================="
ec " >>>>  Begin mkAltSky on $list $nl images and with $skylist <<<<"
echo "------------------------------------------------------------------"

case $FILTER in
	N | Y | J  ) delta=40 ;;
	H | K      ) delta=20 ;;
esac   
		
args=" --inmask-suffix _mask.fits  --outweight-suffix _mask.fits  -n 20  --pass2  -d 1000.0  -t $delta "
conf=" --config-path $confdir  --script-path $confdir  " 

comm="python $pydir/mkAltSky.py -l $list -S $skylist  $args $conf "
logfile=$(echo $list | cut -d\. -f1)

#-----------------------------------------------------------------------------
# run these in a separate subdir to avoid problems with parallel runs
#-----------------------------------------------------------------------------

if [ -d $workdir ]; then rm -rf $workdir; fi
mkdir $workdir
if [ $? -ne 0 ]; then ec "ERROR: could not build $workdir - quitting"; exit 1; fi
mycd $workdir

ec "## Working on $(hostname); data on $rhost; work dir is $workdir"
ec "## Command line is:"
ec "% $comm "
ec ""

###  check available space - see pswarp_multi.sh for example
###  hopefully not needed when using bigscratch nodes

ec "## Link the needed data and config files... "
cp $datadir/$list $datadir/$skylist .
ln -s /n08data/UltraVista/DR6/bpms/bpm_comb_20190420.fits zeros.fits
ln -s $confdir/swarp238.conf ./swarp.conf
cp $confdir/bgsub.param $confdir/bgsub.conf $confdir/gauss_3.0_7x7.conv .

for f in $(cat $skylist); do 
	r=${f%.fits}
	ln -s $datadir/Masks/${r}_mask.fits .
	ln -s $datadir/weights/${r}_weight.fits .
	ln -s $datadir/withSky/${r}_withSky.fits ${r}.fits
done

#-----------------------------------------------------------------------------
# Check links:
ec "## Input file links are like:"
imroot=$(head -1 $list | cut -d \. -f1)
ls -lh ${imroot}*.* | tr -s ' ' | cut -d ' ' -f9-12 

nims=$(ls -L v20*_0????.fits | wc -l)
nmks=$(ls -L v20*_0????_mask.fits   | wc -l)
nwgs=$(ls -L v20*_0????_weight.fits | wc -l)

ec "## Build links to $nims images, $nwgs weights, $nmks masks"
if [[ $nims -ne $nwgs ]] || [[ $nims -ne $nmks ]] ; then
	ec "PROBLEM: $nims, $nwgs, $nmks not all equal ... quitting"
	exit 5
fi
#-----------------------------------------------------------------------------

echo "% $comm >> $logfile.log "
if [ $dry == 'T' ]; then
    echo " ## DRY MODE - do nothing ## "
	exit 0
else
	ec "### Begin work ... "
    $comm >> $logfile.log 2>> $logfile.err
    # to remove blank lines and unneeded warnings from error logs
	strings $logfile.err | grep -v FITS\ header > x ; mv x $logfile.err   

	# check products
	nima=$(cat $list | wc -l)
	nski=$(grep skip\  $logfile.log | wc -l)
	ec "# Skipped $nski files with too few nearby skies"
	
	nalt=$(ls v20*_alt.fits 2> /dev/null | wc -l)     # num done
	nexp=$(($nima - $nski))                           # num expected
	if [ $nalt -ne $nexp ]; then     ### PROBLEM   
		echo "PROBLEM: found only $nalt files of $nima expected - check logs"
		echo "PROBLEM: mv products back to images/; keep workdir $workdir"
		edate=$(date "+%s"); dt=$(($edate - $sdate))
		echo " >>>> $module.sh finished - walltime: $dt sec  <<<<"
		echo "------------------------------------------------------------------"
		errcode=2
	else                             ### SUCCESS
		echo "# Found $nalt _alt files - move them back to images/ and clean up"
	    # mv products back to images/
#		[[ -e $datadir/mkAlt ]] || mkdir $datadir/mkAlt
		mv v20*_*_[ac]??.fits   $datadir/mkAlt
		mv $logfile.log  $logfile.err  $datadir  # leave them in $datadir for now
		rm v20*_0????.fits v20*_weight.fits v20*_mask.fits      # the links
		rm -f missfits.xml bgsub.xml *.con? *.param zeroes.fits    # other stuff
		errcode=0
	fi
fi

cd $datadir
# remove workdir if all went well
if [ $errcode -eq 0 ]; then rm -rf $workdir; fi

#-----------------------------------------------------------------------------
# and finish up
#-----------------------------------------------------------------------------
edate=$(date "+%s"); dt=$(($edate - $sdate))
echo "   #####  $module.sh finished - walltime: $dt sec  #####"
echo "------------------------------------------------------------------"

exit $errcode
#-----------------------------------------------------------------------------

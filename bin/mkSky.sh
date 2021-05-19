#!/bin/sh
#PBS -S /bin/sh
#PBS -N mkSky_@FILTER@_@ID@
#PBS -o @IDENT@.out            
#PBS -j oe
#PBS -l nodes=1:ppn=9,walltime=28:00:00
#-----------------------------------------------------------------------------
# module: mkSky script
# for each frame, combine images obtained close in time using a median filter
# to remove the stars in order to build a pure sky image that will later be 
# subtracted from the source image.
#
# set ppn=5 to limit num jobs running in parallel to 8.  This also leaves 3
# nodes free for other stuff (interactive work).  
#
# 17.aug/??: 
# - created from subSky.sh and in order to remove the (simple) sky subtraction; 
# - also adapted to work on "remote" nodes
# 10.may.21:
# - misc adaptation and add'l checks for DR5
# - including increase of ppn to 7
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

ec() { echo "$(date "+[%d.%h.%y %T"]) $1 "; }    # echo with date
mycd() { \cd $1; ec " --> $PWD"; }               # cd with message
sdate=$(date "+%s")

#-----------------------------------------------------------------------------
# Setup
#-----------------------------------------------------------------------------
module=mkSky                      # w/o .sh extension

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
# Work ....
#-----------------------------------------------------------------------------

datadir=$WRK/images              # reference dir
rhost=$(echo $WRK | cut -c 2-4)  # host of $WRK

# build work dir name: 
dirname=$(echo $list | cut -d\. -f1)
whost=$(hostname)   #; echo "DEBUG: ref/work hosts: $rhost  $whost"

if [[ "$rhost" =~ $whost ]]; then  # on local node
	workdir=$WRK/images/${dirname}_$FILTER
	remoteRun=0
else                               # on remote (bigscratch) node
	workdir=/scratch/${dirname}_$FILTER
	remoteRun=1
fi

if [ ! -d $datadir ];       then echo "ERROR: $WRK/images not found ... quitting"; exit 5; fi
if [ ! -s $datadir/$list ]; then echo "ERROR: $list not found in $WRK/images ... quitting"; exit 5; fi

ec "## Working on $(hostname); data on $rhost; work dir is $workdir"

mycd $datadir
nl=$(cat $list | wc -l)
skylist=${list%.lst}.skylst

echo "=================================================================="
ec " >>>>  Begin mkSky on $list $nl images and with $skylist <<<<"
echo "------------------------------------------------------------------"

case $FILTER in
	N | NB118 | Y | J  ) delta=40 ;;
	H | K | Ks | T | S ) delta=20 ;;
	Q | R | P          ) delta=40 ;;   # the test filters
esac   
		
args=" --inmask-suffix _mask.fits  --outweight-suffix _mask.fits  -n 20  --pass2  -d 1000.0  -t $delta "
conf=" --config-path $confdir  --script-path $confdir  " 

comm="python $pydir/mkSky.py -l $list -S $skylist  $args $conf "
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
###  hopefully not needed when using bigscraatch nodes

ec "## Link the needed data and config files... "
cp $datadir/$list $datadir/$skylist .
ln -s $datadir/zeroes.fits .
cp $confdir/bgsub.param $confdir/swarp.conf .
cp $confdir/bgsub.conf $confdir/gauss_3.0_7x7.conv .

for f in $(cat $skylist); do 
	r=${f%.fits}
	ln -s $datadir/Masks/${r}_mask.fits .
	ln -s $datadir/weights/${r}_weight.fits .
	ln -s $datadir/withSky/${r}_withSky.fits ${r}.fits
#	ln -s $datadir/heads/${r}.head .   ### Don't want these
done

#-----------------------------------------------------------------------------
# Check links:
ec "## Input file links are like:"
imroot=$(head -1 $list | cut -d \. -f1)
ls -lh ${imroot}*.* | tr -s ' ' | cut -d ' ' -f9-12 

nims=$(ls -L v20*_0????.fits | wc -l)
nmks=$(ls -L v20*_0????_mask.fits   | wc -l)
nwgs=$(ls -L v20*_0????_weight.fits | wc -l)

ec "## Build links to $nims images, $nwgs weights, $nmks heads"
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
	ec "## Begin work ... "
    $comm >> $logfile.log 2>> $logfile.err
	strings  $logfile.err > x ; mv x $logfile.err   # to remove blank lines

	# check products
	nima=$(cat mkSky_??.lst | wc -l)
	nski=$(grep skip\  $logfile.log | wc -l)
	echo "# Skipped $nski files with too few nearby skies"
	
	nsky=$(ls v20*_sky.fits 2> /dev/null | wc -l)  
	nexp=$(($nima - $nski))
	if [ $nsky -ne $nexp ]; then 
		echo "PROBLEM: found only $nsky files of $nima expected - check logs"
		echo "PROBLEM: mv products back to images/; keep workdir $workdir"
		edate=$(date "+%s"); dt=$(($edate - $sdate))
		echo " >>>> $module.sh finished - walltime: $dt sec  <<<<"
		echo "------------------------------------------------------------------"
		errcode=2
#		mv  mkSky_??.log mkSky_??.err  $datadir/
	else
		echo " >> Found $nsky _sky files - move them back to images/ and clean up"
	    # mv products back to images/
		mv v20*_*_sky.fits  $logfile.log  $logfile.err  $datadir/ 
		rm v20*_0????.fits v20*_weight.fits v20*_mask.fits      # the links
		rm missfits.xml bgsub.xml *.con? *.param zeroes.fits    # other stuff
		errcode=0
	fi
fi

cd $datadir
if [ $errcode -eq 0 ]; then rm -rf $workdir; fi

#-----------------------------------------------------------------------------
# and finish up
#-----------------------------------------------------------------------------
edate=$(date "+%s"); dt=$(($edate - $sdate))
echo " >>>> $module.sh finished - walltime: $dt sec  <<<<"
echo "------------------------------------------------------------------"
exit $errcode

#-----------------------------------------------------------------------------

#!/bin/sh
#PBS -S /bin/sh
#PBS -N subSky_@FILTER@_@ID@
#PBS -o @IDENT@.out            
#PBS -j oe
#PBS -l nodes=1:ppn=5,walltime=40:00:00
#-----------------------------------------------------------------------------
# module: subSky wrapper for subSky.py
# requires: intelpython, astropy.io.fits, uvis scripts and libs
# Purpose: actual sky subtraction and destriping
#
# set ppn=5 to limit num jobs running in parallel to 8.  This also leaves 3
# nodes free for other stuff (interactive work).  
#-----------------------------------------------------------------------------
set -u  
# paths
export PATH="~/bin:$PATH:/softs/dfits/bin:/softs/astromatic/bin"
export PYTHONPATH="/home/moneti/uvis/python:/home/moneti/uvis/python_lib"

# add python and UltraVista scripts and libs
module () {  eval $(/usr/bin/modulecmd bash $*); }
module purge; module load intelpython/2

#-----------------------------------------------------------------------------
# Some variables and functions
#-----------------------------------------------------------------------------

ec() { echo "$(date "+[%d.%h.%y %T"]) $1 "; }    # echo with date
ecn() { echo -n "$(date "+[%d.%h.%y %T"]) $1 "; }    # echo with date
mycd() { \cd $1; echo " --> $PWD"; }               # cd with message
sdate=$(date "+%s")

uvis=/home/moneti/softs/uvis-pipe    # top UltraVista code dir
bindir=$uvis/bin                     # pipeline modules
pydir=$uvis/python                   # python scripts
confdir=$uvis/config                 # config dir

#-----------------------------------------------------------------------------
# Setup
#-----------------------------------------------------------------------------
module=subSky                     # w/o .sh extension

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
# The real work ....
#-----------------------------------------------------------------------------

datadir=$WRK/images            # reference dir
rhost=$(echo $WRK | cut -c 2-4)  # host of WRK

# build work dir: 
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

nl=$(cat $list | wc -l)
echo "=================================================================="
echo " >>>>  Begin subSky with $list of $nl images  <<<<"
echo "------------------------------------------------------------------"

mycd $datadir
echo "## Working on $(hostname); data on $rhost; work dir is $workdir"


logfile=$(echo $list | cut -d\. -f1).log
rm -rf $logfile.log     # any previous logs
ecn "-----------------------------------------------------------------------------" > $logfile
ecn "    logfile of subSky.sh  " >> $logfile
ecn "-----------------------------------------------------------------------------" >> $logfile

case $FILTER in
	NB118 | Y | J  ) delta=40 ;;
	H | Ks | T | S ) delta=20 ;;
	Q | R | P      ) delta=40 ;;   # the test filters
esac   
		
# command line
osuff="_clean"       # w/o .fits extension
args=" --inmask-suffix _mask.fits "
conf=" --config-path $confdir/config  --script-path $confdir  --outname-suffix $osuff "
comm="python $pydir/subSky.py -l $list $args $conf "

# run these in a separate subdir to avoid problems with parallel runs
if [ ! -d $workdir ]; then mkdir $workdir; else rm -rf $workdir/*; fi
mycd $workdir
ec "# For list $list, working in $(hostname):$PWD"

# copy or link needed files
cp $datadir/$list .
for f in $(cat $list); do 
	r=${f%.fits}
	ln -s $datadir/withSky/${r}_withSky.fits ${r}.fits
	ln -s $datadir/weights/${r}_weight.fits .
	ln -s $datadir/mkSky/${r}_sky.fits .
	ln -s $datadir/Masks/${r}_mask.fits .

	ln -s $datadir/${r}_clean.fits .    # output file link
	touch $datadir/${r}_clean.fits      # don't leave dangling link
done
cp $confdir/bgsub.param $confdir/swarp.conf .
cp $confdir/bgsub.conf $confdir/gauss_3.0_7x7.conv .

#-----------------------------------------------------------------------------
# Check links:
ec "## Input file links are like:"
imroot=$(head -1 $list | cut -d \. -f1)
ls -lh ${imroot}*.* | tr -s ' ' | cut -d ' ' -f9-12 

nims=$(ls -L v20*_0????.fits | wc -l)           # with sky images
nwgs=$(ls -L v20*_0????_weight.fits | wc -l)    # updated weights
nsky=$(ls -L v20*_0????_sky.fits  | wc -l)      # skies to subtract
nmks=$(ls -L v20*_0????_mask.fits | wc -l)      # masks

ec "## Built links to $nims images, $nwgs weights, $nsky skies, $nmks masks"
if [[ $nims -ne $nwgs ]] || [[ $nims -ne $nsky ]] || [[ $nims -ne $nmks ]]; then
	ec "PROBLEM: $nims, $nwgs, $nsky, $nmks not all equal ... quitting"
	exit 5
fi
#-----------------------------------------------------------------------------

ec "## Command line is: "
ec "% $comm >> $logfile "

if [ $dry == 'T' ]; then
    echo " ## DRY MODE - do nothing ## "
	exit 0
else
	# do the work
    $comm | grep -v ^$ > $logfile 2>&1    # removing blank lines

	# check products
	nsub=$(ls v20*_clean.fits 2> /dev/null | wc -l)
	if [ $nsub -eq 0 ]; then 
		echo "!!! BIG PROBLEM: no outputs found"
		edate=$(date "+%s"); dt=$(($edate - $sdate))
		echo " >>>> $module.sh finished - walltime: $dt sec  <<<<"
		echo "------------------------------------------------------------------"
		exit 3
	fi

	echo " >> Found $nsub _clean files - move them back to images/ and clean up"
	# mv products back to images/
#	mv v20*_*_sub.fits v20*_*_bgcln.fits v20*_*_clean.fits  $logfile  $datadir
#	mv v20*_*_clean.fits  $logfile  $datadir
#	rm v20*_*_sub.fits v20*_*_bgcln.fits    # intermediate products
	rm v20*_0????.fits v20*_weight.fits v20*_mask.fits  v20*_*_sky.fits   # links
	rm bgsub.xml *.con? *.param  # zeroes.fits
	exit 0
fi

cd $datadir
rm -rf $workdir
	
#-----------------------------------------------------------------------------
# and finish up
#-----------------------------------------------------------------------------
edate=$(date "+%s"); dt=$(($edate - $sdate))
echo " >>>> $module.sh finished - walltime: $dt sec  <<<<"
echo "------------------------------------------------------------------"
exit 0

#-----------------------------------------------------------------------------

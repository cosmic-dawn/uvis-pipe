#!/bin/sh
#PBS -S /bin/sh
#PBS -N noSky_@FILTER@_@ID@
#PBS -o @IDENT@.out            
#PBS -j oe
#PBS -l nodes=@NODE@:ppn=4,walltime=48:00:00
#-----------------------------------------------------------------------------
# module: noSky 
# requires: intelpython, astropy.io.fits, uvis scripts and libs
# from .sky.fits files, find regions where no sky could be determined and update
# zeroes files (initially a link) to include them.  Used in sky cleaning
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
sdate=$(date "+%s")

module=noSky                   # w/o .sh extension
uvis=/home/moneti/softs/uvis-pipe            # top UltraVista code dir
bindir=$uvis/bin
pydir=$uvis/python                # python scripts
confdir=$uvis/config              # config dir

# check  if run via shell or via qsub: 
if [[ "$0" =~ "$module" ]]; then
    echo "$module: running as shell script "
        list=$1
        WRK=$WRK
        FILTER=$FILTER
        if [ $# -eq 2 ]; then dry=1; else dry=0; fi
else
    echo "$module: running via qsub (from pipeline)"
        dry=@DRY@
        list=@LIST@
        FILTER=@FILTER@
        WRK=@WRK@
fi

#-----------------------------------------------------------------------------
# do the real work ....
#-----------------------------------------------------------------------------

cd $WRK/images

if [ $? -ne 0 ]; then echo "ERROR: $WRK/images not found ... quitting"; exit 5; fi
if [ ! -s $list ]; then echo "ERROR: $list not found in $WRK/images ... quitting"; exit 5; fi

nl=$(cat $list | wc -l)
echo "=================================================================="
ec " >>>>  Begin noSky on $list with $nl entries  <<<<"
echo "------------------------------------------------------------------"

comm="python $pydir/noSky.py -l $list -z zeroes.fits "
logfile=$(echo $list | cut -d\. -f1)
rm -f $logfile.log $logfile.err    # previous logs

echo "% $comm >> $logfile "
if [ $dry -ne 1 ]; then
    $comm > $logfile.log 2> $logfile.err

    # check products
	nmsk=$(ls v20*_nosky.fits 2> /dev/null | wc -l)
	if [ $nmsk -lt $nl ]; then 
		ec "!!! BIG PROBLEM: not enough masks found"
	else
		ec " >>> Built $nmsk nosky masks  <<<<"
	fi
	if [ ! -s noSky_$logfile.err ]; then rm $logfile.err; fi
fi

edate=$(date "+%s"); dt=$(($edate - $sdate))
echo " >>>> $module.sh finished - walltime: $dt sec  <<<<"
echo "------------------------------------------------------------------"
exit 0

#!/bin/sh
#PBS -S /bin/sh
#PBS -N upWgts_@FILTER@_@ID@
#PBS -o @IDENT@.out            
#PBS -j oe
#PBS -l nodes=1:ppn=5,walltime=20:00:00
#-----------------------------------------------------------------------------
# module: updateWeights
# requires: intelpython, astropy.io.fits, uvis scripts and libs
# from .sky.fits files, find regions where no sky could be determined and set
# then to zero in the input weight file
#
# executed locally
#-----------------------------------------------------------------------------
set -u  
# paths
export PATH="~/bin:$PATH:/softs/dfits/bin:/softs/astromatic/bin"
export PYTHONPATH="/home/moneti/uvis/python:/home/moneti/uvis/python_lib"

# add python and UltraVista scripts and libs
module () { eval $(/usr/bin/modulecmd bash $*); }
module purge; module load intelpython/2

#-----------------------------------------------------------------------------
# Some variables and functions
#-----------------------------------------------------------------------------

ec() { echo "$(date "+[%d.%h.%y %T"]) $1 "; }    # echo with date
sdate=$(date "+%s")

module=updateWeight               # w/o .sh extension
uvis=/home/moneti/softs/uvis-pipe            # top UltraVista code dir
bindir=$uvis/bin
pydir=$uvis/python                # python scripts
confdir=$uvis/config              # config dir
errcode=0

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
# The REAL work ... done in images dir - no need for temp subdirs
#-----------------------------------------------------------------------------

cd $WRK/images

if [ $? -ne 0 ]; then echo "ERROR: $WRK/images not found ... quitting"; exit 5; fi
if [ ! -s $list ]; then echo "ERROR: $list not found in $WRK/images ... quitting"; exit 5; fi

nl=$(cat $list | wc -l)
echo "=================================================================="
ec " >>>>  Begin updateWeights on $list with $nl entries  <<<<"
echo "------------------------------------------------------------------"

comm="python $pydir/updateWeights.py -l $list"  # -z zeroes.fits "
logfile=$(echo $list | cut -d\. -f1)
rm -f $logfile.log $logfile.err                 # rm previous logs

echo "% $comm \> $logfile.log 2\> $logfile.err"
if [ $dry -ne 1 ]; then
    $comm  > $logfile.log 2> $logfile.err

	# Check logfile - should be same length as input list
	nn=$(cat $logfile.log | wc -l)
	nl=$(cat $list | wc -l)
	if [ $nn -ne $nl ]; then
		ec "!!! ERROR: $logfile.log shorter than input list"
		errcode=7
	fi
		
	nerr=$(grep ERROR $logfile.log | wc -l)
	if [ $nerr -ne 0 ]; then
		ec "!!! ERROR: $nerr errors found in $logfile.log"
		errcode=8
	fi

	nerr=$(cat $logfile.err 2> /dev/null | wc -l)
	if [ -s $logfile.err ]; then   #exists and size != 0
		ec "!!! ERROR: Foound other errors - see $logfile.err"
		errcode=9
	else
		rm $logfile.err
	fi
fi

edate=$(date "+%s"); dt=$(($edate - $sdate))
echo " >>>> $module.sh finished - walltime: $dt sec  <<<<"
echo "------------------------------------------------------------------"
exit $errcode

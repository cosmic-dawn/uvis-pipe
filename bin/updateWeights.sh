#!/bin/sh
#PBS -S /bin/sh
#PBS -N upWgts_@FILTER@_@ID@
#PBS -o @IDENT@.out            
#PBS -j oe
#PBS -l nodes=1:ppn=7,walltime=20:00:00
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
ec " >>>>  Begin updateWeights on $list with $nl entries  <<<<"
echo "------------------------------------------------------------------"

comm="python $pydir/updateWeights.py -l $list"  # -z zeroes.fits "
logfile=$(echo $list | cut -d\. -f1)
rm -f $logfile.log $logfile.err                 # rm previous logs

echo "% $comm \> $logfile.log 2\> $logfile.err"
if [ $dry -ne 1 ]; then
    $comm  > $logfile.log 2> $logfile.err

	nskip=$(grep ERROR $logfile.log | wc -l)
	if [ $nskip -ne 0 ]; then
		ec "!!! ERROR: $nskip _sky files incomplete -  see $logfile.skip"
	fi

    # check products
	nn=$(ls v20*_weight.fits 2> /dev/null | wc -l)
	if [ $nn -lt $nl ]; then 
		ec "!!! PROBLEM: not enough masks found"
	else
		ec " >>> Built $nn new weight files  <<<<"
	fi
	if [ ! -s updateWeights_$logfile.err ]; then rm $logfile.err; fi
fi

edate=$(date "+%s"); dt=$(($edate - $sdate))
echo " >>>> $module.sh finished - walltime: $dt sec  <<<<"
echo "------------------------------------------------------------------"
exit 0

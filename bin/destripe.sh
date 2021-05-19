#!/bin/sh
#PBS -S /bin/sh
#PBS -N destripe_@FILTER@_@ID@
#PBS -o @IDENT@.out            
#PBS -j oe
#PBS -l nodes=@NODE@:ppn=4,walltime=24:00:00
#-----------------------------------------------------------------------------
# module: destripe 
# requires: intelpython, astropy.io.fits, uvis scripts and libs
#
#
#-----------------------------------------------------------------------------
set -u  
# paths
#export PATH="~/bin:$PATH:/softs/dfits/bin:/softs/astromatic/bin"
export PYTHONPATH="/home/moneti/uvis/python:/home/moneti/uvis/python_lib"

# add python and UltraVista scripts and libs
module () {  eval $(/usr/bin/modulecmd bash $*); }
module purge; module load intelpython/2

#-----------------------------------------------------------------------------
# Some variables and functions
#-----------------------------------------------------------------------------

ec() { echo "$(date "+[%d.%h.%y %T"]) $1 "; }    # echo with date
sdate=$(date "+%s")

module=destripe                   # w/o .sh extension
uvis=/home/moneti/softs/uvis-pipe            # top UltraVista code dir
bindir=$uvis/bin
pydir=$uvis/python                # python scripts
confdir=$uvis/config              # config dir

# check  if run via shell or via qsub: 
if [[ "$0" =~ "$module" ]]; then
    ec "This is $module: running as shell script "
    list=$1
    WRK=$WRK
    FILTER=$FILTER
	debug=" "; dry=0
    if [ $# -eq 2 ]; then debug="-d"; else dry=1; fi
	
	pipemode=0
	osuff="_bs07-5_dd"
else
    ec "This is $module: running via qsub (from pipeline)"
    dry=@DRY@
    list=@LIST@
    FILTER=@FILTER@
    WRK=@WRK@
	pipemode=1
	osuff=@OSUFF@
fi

#-----------------------------------------------------------------------------
# do the real work ....
#-----------------------------------------------------------------------------

if [ $? -ne 0 ]; then echo "ERROR: $WRK/images not found ... quitting"; exit 5; fi
if [ $pipemode -eq 1 ]; then cd $WRK/images; fi
if [ ! -s $list ]; then echo "ERROR: $list not found in $WRK/images ... quitting"; exit 5; fi

nl=$(cat $list | wc -l)
logfile=$(echo $list | cut -d\. -f1)
rm -f $logfile.log $logfile.err    # previous logs
comm="python $pydir/destripe.py -l $list -o $osuff"

ec "=================================================================="
ec "# Begin destripe on $list with $nl entries "
ec "# Input images are like:"
ec "  $(ls -lh $(head -1 $list) | tr -s ' ' | cut -d' ' -f9-11)"
ec "# command line is:"
ec "% $comm " #  >> $logfile.log 2>> $logfile.err "
ec "------------------------------------------------------------------"

if [ $dry -ne 1 ]; then
    $comm  >> $logfile.log 2>> $logfile.err

    # check products
	nn=$(ls v20*$osuff.fits 2> /dev/null | wc -l)
	if [ $nn -lt $nl ]; then 
		ec "!!! BIG PROBLEM: not enough $osuff found"
	else
		ec "# DONE - Built $nn $osuff files"
	fi
	if [ ! -s $logfile.err ]; then rm $logfile.err; fi
else
	ec "# exit dry mode"; exit 0
fi

edate=$(date "+%s"); dt=$(($edate - $sdate))
ec " >>>> $module.sh finished - walltime: $dt sec  <<<<"
ec "------------------------------------------------------------------"
exit 0

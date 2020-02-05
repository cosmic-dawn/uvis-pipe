#!/bin/sh
#PBS -S /bin/sh
#PBS -N FlagSat_@FILTER@_@ID@
#PBS -o FlagSat_@ID@.out
#PBS -j oe
#PBS -l nodes=1:ppn=2,walltime=12:00:00
#-----------------------------------------------------------------------------
# FlagSat: flag saturation in ldacs
# requires: intelpython, astropy.io.fits, uvis scripts and libs
#-----------------------------------------------------------------------------

set -u  
umask 022
ec()  { echo    "$(date "+[%d.%h.%y %T"]) $1 " ; }
ecn() { echo -n "$(date "+[%d.%h.%y %T"]) $1 " ; }
mycd() { \cd $1; ec " --> $PWD"; }               # cd with message

# paths
module () {  eval $(/usr/bin/modulecmd bash $*); }
module purge; module load intelpython/2

#-----------------------------------------------------------------------------
# Some variables and functions
#-----------------------------------------------------------------------------

sdate=$(date "+%s")

uvis=/home/moneti/softs/uvis-pipe      # top UltraVista code dir
pydir=$uvis/python
bindir=$uvis/bin
confdir=$uvis/config
export PYTHONPATH=$pydir

#-----------------------------------------------------------------------------

module=FlagSat

# check  if run via shell or via qsub:
if [[ "$0" =~ "$module" ]]; then
	 echo "$module: running as shell script "
	 list=$1
	 WRK=$WRK
	 FILTER=$FILTER
	 if [[ "${@: -1}" =~ 'dry' ]] || [ "${@: -1}" == 'test' ]; then dry=T; else dry=F; fi
else
    echo "$module: running via qsub (from pipeline)"
	dry=0
	WRK=@WRK@
	list=@LIST@
	FILTER=@FILTER@
fi

#-----------------------------------------------------------------------------
cd $WRK/images

if [ $? -ne 0 ]; then echo "ERROR: $WRK/images not found ... quitting"; exit 5; fi
if [ ! -s $list ]; then echo "ERROR: $list not found in $WRK/images ... quitting"; exit 5; fi

nl=$(cat $list | wc -l)
echo "=================================================================="
ec " >>>>  Begin FlatSat $list with $nl entries  <<<<"
echo "------------------------------------------------------------------"

for f in $(cat $list); do
   bdate=$(date "+%s")
   comm="python $pydir/flag_saturation.py -c $f --noplot "
   echo "% $comm "
   if [ $dry -ne 1 ]; then
       $comm >> FlagSat_@ID@.log
   fi
done

#-----------------------------------------------------------------------------

edate=$(date "+%s"); dt=$(($edate - $bdate))
echo " >>>> FlagSat finished - walltime: $dt sec  <<<<"
echo "------------------------------------------------------------------"
echo ""
exit 0
#-----------------------------------------------------------------------------

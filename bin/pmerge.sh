#!/bin/sh
#PBS -S /bin/sh
#PBS -N merge_@FILTER@
#PBS -o @IDENT@.out            
#PBS -j oe 
#PBS -l nodes=1:ppn=6,walltime=08:00:00
#-----------------------------------------------------------------------------
# pmerge:  pmerge script - to merge substacks
# requires: intelpython, astropy.io.fits, uvis scripts and libs
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
ec() { if [ $dry == 'T' ]; then echo "[TEST MODE] $1";
    else echo "$(date "+[%h.%d %T]") $1 "; fi; } 
sdate=$(date "+%s")

module=pmerge                   # w/o .sh extension
uvis=/home/moneti/uvis            # top UltraVista code dir
bindir=$uvis/bin
pydir=$uvis/python                # python scripts
confdir=$uvis/config              # config dir

# check  if run via shell or via qsub: 
if [[ "$0" =~ "$module" ]]; then
    echo "$module: running as shell script "
    list=pmerge.lst
    WRK=$WRK
    FILTER=$FILTER
    if [[ "${@: -1}" == 'dry' ]]; then dry='T'; else dry='F'; fi
    stout=@STOUT@
    pass=1
else
    echo "$module: running via qsub (from pipeline)"
    dry=@DRY@
    list=@LIST@
    FILTER=@FILTER@
    WRK=@WRK@
    stout=@STOUT@
    pass=@PASS@
fi

verb=" -VERBOSE_TYPE LOG"
tail=$(echo ${list%.lst} | cut -d\_ -f2)

#-----------------------------------------------------------------------------
# do the real work ....
#-----------------------------------------------------------------------------

bdate=$(date "+%s")
cd $WRK/images

if [ $? -ne 0 ]; then echo "ERROR: $WRK/images not found ... quitting"; exit 5; fi
if [ ! -s $list ]; then echo "ERROR: $list not found in $WRK/images ... quitting"; exit 5; fi

stout=$stout.fits
wtout=${stout%.fits}_weight.fits
logfile=$WRK/pmerge.log

# Command to produce the stack and its weight ...
args=" -c $confdir/config/swarp238.conf  -COMBINE_BUFSIZE 8192 -RESAMPLE N  -WEIGHT_SUFFIX _weight.fits \
       -WEIGHT_TYPE MAP_WEIGHT  -COMBINE_TYPE WEIGHTED  -IMAGEOUT_NAME $stout  -WEIGHTOUT_NAME $wtout \
       -WRITE_XML N   -SUBTRACT_BACK N  "
ec "# stack: $stout"
ec "# args:  $args"

comm="swarp @$list $args $verb"


ec "#------------------------------------------------------------------" | tee -a $logfile 
ec " >>>>  Merge $(cat $list | wc -l) substacks from $list  <<<<"        | tee -a $logfile 
ec "#------------------------------------------------------------------" | tee -a $logfile 

ec "% $comm " 
if [ $dry == 'F' ]; then 
	$comm >> $logfile 2>&1; ec ""
else
	echo $comm ; ec ""
fi

# For pass 1, build mask file; Command is mask_for_stack.py ...
if [ $pass -eq 1 ]; then
    ec "#------------------------------------------------------------------" | tee -a $logfile 
    ec " >>>>  Build mask etc. for $stout  <<<<"                             | tee -a $logfile 
    ec "#------------------------------------------------------------------" | tee -a $logfile 
    
    # add the --extendedobj option to use back_size 512 / back_filtersize 5 in order to
    # improve mask of bright star haloes - AM 24.jun.18
    # Threshold of 1.0 seems ok for N and Y, not sure for others ....
    
    case $FILTER in
        N | Y | NB | NB118 ) thr=1. ;;
        J         ) thr=0.7 ;;
        H         ) thr=0.5 ;;
        K | Ks    ) thr=0.9 ;;
    esac
    
	#  --script-path $confdir/c_script 
    args=" --conf-path $confdir --extendedobj --threshold $thr "
    comm="python $pydir/mask_for_stack.py -I $stout -W $wtout $args "
    ec "% $comm "  | tee -a $logfile  ; ec ""
    if [ $dry == 'F' ]; then 
		$comm >> $logfile 2>&1 ; ec ""
	fi
fi

edate=$(date "+%s"); dt=$(($edate - $sdate))
echo "------------------------------------------------------------------"
echo " >>>> pmerge finished - walltime: $dt sec  <<<<"
echo "------------------------------------------------------------------"
exit 0

#!/bin/bash 
#-----------------------------------------------------------------------------
# Run SExtractor and scamp on paw stacks
# - adapted from psacmp.sh and qFits.sh
# - requires: astromatic suite, intelpython, astropy.io.fits, etc.
#-----------------------------------------------------------------------------
set -u 
export PATH="/softs/astromatic/bin:$PATH"  #echo $PATH
export PYTHONPATH="/home/moneti/uvis/python:/home/moneti/uvis/python_lib" 

module() { eval $(/usr/bin/modulecmd bash $*); }
module purge ; module load inteloneapi/2021.1 intelpython/3-2019.4 cfitsio
export LD_LIBRARY_PATH=/lib64:${LD_LIBRARY_PATH}

#-----------------------------------------------------------------------------
# other functions
#-----------------------------------------------------------------------------

ec() { echo "$(date "+[%d.%h.%y %T"]) $1 "; }    # echo with date

#-----------------------------------------------------------------------------
# Other settings and variables
#-----------------------------------------------------------------------------

dry='F'
if [[ "${@: -1}" == 'dry' ]];  then dry='T'; fi
if [[ "${@: -1}" == 'test' ]]; then dry='T'; fi

module=pselfcal                   # w/o .sh extension
uvis=/home/moneti/softs/uvis-pipe # top UltraVista code dir
bindir=$uvis/bin
pydir=$uvis/python                # python scripts
confdir=$uvis/config              # config dir
scripts=$uvis/scripts             # other scripts dir (awk ...)

#-----------------------------------------------------------------------------------------------
case  $FILTER in   # 
   N | P ) magzero=29.14 ;;
   Y | Q ) magzero=29.39 ;;
   J | R ) magzero=29.10 ;;
   H | S ) magzero=28.62 ;;
   K | T ) magzero=28.16 ;;
   * ) ec "# ERROR: invalid filter $FILTER"; exit 3 ;;
esac   

#-----------------------------------------------------------------------------------------------

version="2.10.0"   # avec Gaia-EDR3, mais tjrs sans support des Proper Motions
myscamp="/softs/astromatic/scamp/${version}-gnu/bin/scamp" 

#-----------------------------------------------------------------------------------------------
# some variables ... later to be set automatically ... maybe
resol='lr'
#-----------------------------------------------------------------------------------------------

cd $WRK/images; echo "--> $PWD"            # temprary dir with paw substacks

ls UVISTA-DR5_${FILTER}_paw?_${resol}.fits > pawlist
list=pawlist
npaws=$(cat $list | wc -l)
ec "## Found $npaws paw stacks to treat "
#echo "HERE"; exit
#-----------------------------------------------------------------------------
# 1. Run SExtractor on each image
#-----------------------------------------------------------------------------

for f in $(cat $list); do
	root=${f%.fits}
	logfile=selfcal_se_${root:13:4}.log
	if [ -e $logfile ]; then rm $logfile; fi
	echo "Sextractor $f ==> $root.ldac > $logfile"
	touch $root.ldac; touch $logfile
done

#-----------------------------------------------------------------------------
# 1. Run scamp with production of merged catalog on ldacs
#-----------------------------------------------------------------------------

sconf=$confdir/scamp_dr5.conf   # new one for DR5
logfile=selfcal_scamp.log 
if [ -e $logfile ]; then rm -f $logfile; fi

ptag=$FILTER
verb="-VERBOSE_TYPE NORMAL"

args=" -c $sconf  -MAGZERO_OUT $magzero  -ASTRINSTRU_KEY OBJECT "
catal="-ASTREFCAT_NAME GAIA-EDR3_1000+0211_r61.cat"  # use a local reference catalogue
extra="-XML_NAME $module.xml -MOSAIC_TYPE SAME_CRVAL -MERGEDOUTCAT_TYPE ASCII -MERGEDOUTCAT_NAME"
pname="-CHECKPLOT_NAME fgroups_${ptag},referr2d_${ptag},referr1d_${ptag},interr2d_${ptag},interr1d_${ptag},photerr_${ptag}"

# build command line
comm="$myscamp @$list  $args  $catal  $extra $pname $verb"


#ec "# Using $list with $nldacs files; and $naheads ahead files "
ec "# Filter is $FILTER; magzero = $magzero" 
ec "# Using $myscamp  ==> $($myscamp -v)"
ec "# Scamp config file is $sconf"
ec "# logfile is $logfile"
ec "# Command line is:"
ec "    $comm"
ec ""
if [[ $dry == 'T' ]]; then
	echo "[---DRY---] Working directory is $WRK/images"
	echo "[---DRY---] Input files are like $(tail -1 $list)"
    echo "[---DRY---] >>  Dry-run of $0 finished .... << "
	ec "#-----------------------------------------------------------------------------"
#	for f in $(cat $list); do rm $f .; done
	exit 0
else
	for f in $(cat $list); do ln -sf ldacs/$f .; done
fi

#-----------------------------------------------------------------------------

bdate=$(date "+%s.%N")

if [ $pipemode -eq 0 ]; then
	ec " shell mode" > $logfile
	if [ $nldacs -lt 125 ]; then 
		$comm  2>&1  | tee -a $logfile   # output also to screen if few files
	else 
		$comm >> $logfile 2>&1           # otherwise to logfile only
	fi
	if [ $? -ne 0 ]; then ec "Problem ... " ; exit 5; fi
else    # qsub mode
	ec "  $comm"  >> $logfile
	ec " "    >> $logfile
	$comm     >> $logfile 2>&1     # output to logfile in pipeline mode
	if [ $? -ne 0 ]; then ec "Problem ... " ; tail $logfile ; exit 5; fi
fi

#-----------------------------------------------------------------------------
nerr=$(grep Error $logfile | wc -l);
if [ $nerr -ge 1 ]; then
    grep Error $logfile
    ec "# PROBLEM: $nerr errors found in $logfile ... quitting"
	exit 5 
fi

# check for warnings ==> pscamp.warn"
grep WARNING $logfile | grep -v -e FLAGS\ param -e ATLAS > $WRK/$module.warn
nw=$(cat $WRK/$module.warn | wc -l )
if [ $nw -ne 0 ]; then 
	ec "#### ATTN: $nw warnings found !!"; 
else 
	ec "# ... NO warnings found - congrats"; rm $WRK/$module.warn
fi 

# extract table 3 form xml file
$pydir/scamp_xml2dat.py $module.xml 

# rename the pngs to have the filter name and the pass - just to rename the png files
if [ $FILTER == 'NB118' ]; then FILTER='N'; fi
if [ $FILTER == 'Ks' ];    then FILTER='K'; fi

rename _1.png _${FILTER}.png [f,i,r,p]*_1.png

#-----------------------------------------------------------------------------
# and finish up
#-----------------------------------------------------------------------------
ec " >>>>  pscamp finished - walltime: $(wt)  <<<<"
ec "#-----------------------------------------------------------------------------"
ec ""

for f in $(cat $list); do rm $f ; done   # cleanup
exit 0

#-----------------------------------------------------------------------------

#!/bin/bash 
#PBS -S /bin/sh
#PBS -N pscamp@PTAG@_@FILTER@
#PBS -o @IDENT@.out
#PBS -j oe
#PBS -l nodes=1:ppn=31,walltime=@WTIME@:00:00
#-----------------------------------------------------------------------------
# pscamp: run scamp on a list of ldacs
# requires: astromatic suite, ... intelpython, astropy.io.fits, uvis scripts and libs
#-----------------------------------------------------------------------------
set -u 
export PATH="/softs/astromatic/bin:$PATH"  #echo $PATH
export PYTHONPATH="/home/moneti/uvis/python:/home/moneti/uvis/python_lib" 

#-----------------------------------------------------------------------------
# this is for Henry's scamp with 
#-----------------------------------------------------------------------------
module() { eval $(/usr/bin/modulecmd bash $*); }
module purge ; module load inteloneapi/2021.1 intelpython/3-2019.4 cfitsio
export LD_LIBRARY_PATH=/lib64:${LD_LIBRARY_PATH}

#-----------------------------------------------------------------------------
# other functions
#-----------------------------------------------------------------------------

ec() { echo "$(date "+[%d.%h.%y %T"]) $1 "; }    # echo with date
ec() { echo "[pscamp.sh]" $1; }    # echo with scamp
dt() { echo "$(date "+%s.%N") $bdate" | awk '{printf "%0.2f\n", $1-$2}'; }
wt() { echo "$(date "+%s.%N") $bdate" | awk '{printf "%0.2f hrs\n", ($1-$2)/3600}'; }  # wall time

#-----------------------------------------------------------------------------
# Some variables
#-----------------------------------------------------------------------------

module=pscamp@PTAG@               # w/o .sh extension
uvis=/home/moneti/softs/uvis-pipe # top UltraVista code dir
bindir=$uvis/bin
pydir=$uvis/python                # python scripts
confdir=$uvis/config              # config dir
scripts=$uvis/scripts             # other scripts dir (awk ...)

# check  if run via shell or via qsub:
ec "#-----------------------------------------------------------------------------"
if [[ "$0" =~ "$module" ]]; then
    ec "# $module: running as shell script on $(hostname)"
#	if [[ "${@: -1}" =~ 'dry' ]] || [ "${@: -1}" == 'test' ]; then dry=T; else dry=F; fi
	list=@LIST@
	dry=@DRY@
	WRK=@WRK@
	ptag=@PTAG@
	FILTER=$FILTER
	verb=" -VERBOSE_TYPE LOG"
	pipemode=0
else
    ec "# $module: running via qsub (from pipeline) on $(hostname)"
	WRK=@WRK@
	dry=@DRY@
	ptag=@PTAG@
	list=@LIST@
	FILTER=@FILTER@
	verb=" -VERBOSE_TYPE LOG" # QUIET"
	pipemode=1
fi

ec "#-----------------------------------------------------------------------------"

#-----------------------------------------------------------------------------------------------
case  $FILTER in   # P,Q,R,T filters were test spaces in dr4
   N | NB118 | P) magzero=29.14 ; FILTER=NB118 ;;
   Y | Q        ) magzero=29.39 ;;
   J | R        ) magzero=29.10 ;;
   H | S        ) magzero=28.62 ;;
   K | Ks | T   ) magzero=28.16 ; FILTER=Ks    ;;
   * ) ec "# ERROR: invalid filter $FILTER"; exit 3 ;;
esac   
#-----------------------------------------------------------------------------------------------

#version="2.6.3"     # this version used in DR4
#version="2.7.8"    
#version="2.9.2"
#version="2.9.3-altaz_fix"    # to test, from EB
version="2.10.0"   # avec Gaia-EDR3, mais tjrs sans support des PMs

myscamp="/softs/astromatic/scamp/${version}-gnu/bin/scamp" 

#myscamp="/home/hjmcc/Downloads/scamp/src/scamp" ; version="2.10.0_hjmcc"  # v2.10.0 with new intel compiler (from Henry)
#myscamp="/home/moneti/bin/scamp_2.9.3_morpho"   ; version="2.9.3_morpho"  ; export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/softs/plplot/5.15.0/lib
#-----------------------------------------------------------------------------------------------

cd $WRK/images

nldacs=$(cat $list | wc -l)
naheads=$(ls v20*.ahead 2> /dev/null | wc -l)

sconf=scamp_dr5.conf   # new one for DR5
logfile=$WRK/pscamp${ptag}.log ; rm -f $logfile

args=" -c $sconf  -MAGZERO_OUT $magzero  -ASTRINSTRU_KEY OBJECT "
catal="-ASTREFCAT_NAME GAIA-EDR3_1000+0211_r61.cat"  # use a local reference catalogue
ahead="-AHEADER_GLOBAL vista_gaia.ahead -MOSAIC_TYPE SAME_CRVAL"
extra="-XML_NAME pscamp${ptag}.xml"
pname="-CHECKPLOT_NAME fgroups${ptag},referr2d${ptag},referr1d${ptag},interr2d${ptag},interr1d${ptag}"

# build command line
comm="$myscamp @$list  $args  $ahead  $catal  $extra $pname $verb"


ec "# Using $list with $nldacs files; and $naheads ahead files "
ec "# Filter is $FILTER; magzero = $magzero" 
ec "# Using $myscamp  ==> $($myscamp -v)"
ec "# PBS resources: $(head $WRK/$module.sh | grep nodes= | cut -d \  -f3)"
ec "# Scamp config file is $sconf"
ec "# logfile is $logfile"
ec "# Command line is:"
ec "    $comm"
ec ""
if [[ $dry == 'T' ]]; then
	echo "[---DRY---] Working directory is $WRK"
	echo "[---DRY---] Input files are like $(tail -1 $list)"
    echo "[---DRY---] >>  Dry-run of $0 finished .... << "
	ec "#-----------------------------------------------------------------------------"
#	for f in $(cat $list); do rm $f .; done
	exit 0
else
	for f in $(cat $list); do ln -s ldacs/$f .; done
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
$pydir/scamp_xml2dat.py pscamp${ptag}.xml 

# rename the pngs to have the filter name and the pass - just to rename the png files
if [ $FILTER == 'NB118' ]; then FILTER='N'; fi
if [ $FILTER == 'Ks' ];    then FILTER='K'; fi

rename _1.png _${FILTER}.png [f,i,r]*_1.png

#-----------------------------------------------------------------------------
# and finish up
#-----------------------------------------------------------------------------
ec " >>>>  pscamp finished - walltime: $(wt)  <<<<"
ec "#-----------------------------------------------------------------------------"
ec ""

for f in $(cat $list); do rm $f ; done   # cleanup
exit 0

#-----------------------------------------------------------------------------

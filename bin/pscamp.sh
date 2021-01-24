#!/bin/bash 
#PBS -S /bin/sh
#PBS -N pscamp@PTAG@_@FILTER@
#PBS -o @IDENT@.out
#PBS -j oe
#PBS -l nodes=1:ppn=23,walltime=@WTIME@:00:00
#-----------------------------------------------------------------------------
# pscamp: run scamp on a list of ldacs
# requires: astromatic suite, ... intelpython, astropy.io.fits, uvis scripts and libs
#-----------------------------------------------------------------------------
set -u 
export PATH="/softs/astromatic/bin:$PATH"  #echo $PATH
export PYTHONPATH="/home/moneti/uvis/python:/home/moneti/uvis/python_lib" 
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

version="2.6.3"     # this version used in DR4
#version="2.7.8"    #version="2.9.2"
#version="2.10.0"   # avec Gaia-EDR3, mais tjrs sans support des PMs
myscamp="/softs/astromatic/scamp/${version}-gnu/bin/scamp" 
##echo "using $myscamp" ; $myscamp -v    #DEBUG  

ver="$(echo $version | tr -d \. )"
tag=$ver
sconf=scamp_dr5.conf   # new one for DR5
#-----------------------------------------------------------------------------------------------

cd $WRK/images

nldacs=$(cat $list | wc -l)
naheads=$(ls v20*.ahead 2> /dev/null | wc -l)

args=" -c $sconf  -MAGZERO_OUT $magzero  -ASTRINSTRU_KEY OBJECT "
catal="-ASTREFCAT_NAME GAIA-EDR3_1000+0211_r61.cat"  # use a local reference catalogue
ahead="-AHEADER_GLOBAL vista_gaia.ahead -MOSAIC_TYPE SAME_CRVAL"
extra="-XML_NAME pscamp${ptag}.xml"
pname="-CHECKPLOT_NAME fgroups${ptag},referr2d${ptag},referr1d${ptag},interr2d${ptag},interr1d${ptag}"

logfile=$WRK/pscamp${ptag}.log ; rm -f $logfile

# build command line
comm="$myscamp @$list  $args  $ahead  $catal  $extra $pname $verb"

# make links to needed files
if [ ! -e scamp_dr5.conf ]; then ln -sf $confdir/scamp_dr5.conf . ; fi
if [ ! -e vista_gaia.ahead ]; then ln -sf $confdir/vista_gaia.ahead . ; fi
if [ ! -e GAIA-EDR3_1000+0211_r61.cat ]; then ln -sf $confdir/GAIA-EDR3_1000+0211_r61.cat . ; fi

ec "# Using $list with $nldacs files; and $naheads ahead files "
ec "# Filter is $FILTER; magzero = $magzero" 
ec "# Using $myscamp  ==> $($myscamp -v)"
ec "# PBS resources: $(head $WRK/$module.sh | grep nodes= | cut -d \  -f3)"
ec "# Scamp config file is $(ls -L $sconf)"
ec "# logfile is $logfile"
ec "# Command line is:"
ec "    $comm"
ec " "
if [[ $dry == 'T' ]]; then
	echo "[---DRY---] Working directory is $WRK"
	echo "[---DRY---] Input files are like $(tail -1 $list)"
    echo "[---DRY---] >>  Dry-run of $0 finished .... << "
	ec "#-----------------------------------------------------------------------------"
	exit 0
fi

#- ec "#-----------------------------------------------------------------------------"
#- ec "## Running scamp on $list with $nldacs entries "
#- ec "## Command line is:"
#- echo $comm
#- ec "#-----------------------------------------------------------------------------"

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

#-## extract contrast results
#-#
#-#if [ $(cat fluxscale.dat | wc -l) -ge 1 ]; then
#-#	res=$(grep -v -e INF -e 0.00000000 fluxscale.dat | tr -s ' ' | cut -d' ' -f2 | awk -f $scripts/std.awk )
#-#	ec "# mean flux scale:  $res"
#-#else
#-#	ec "#### ATTN: FLUXSCLE not found in .head files ... "
#-#fi
#-#
#-## stats on contrast
#-#$pydir/pscamp_xml2dat.py   # extract contrast, shift, and other info from scamp xml file
#-#if [ -e $logfile ]; then   # same info from logfile
#-#	grep "ld\ A" pscamp.log | tr \" \   | sort -n -k10 > pscamp_tb3.log
#-#	ec "# Mean X-Y contrast: $(awk '{print $10}' pscamp_tb3.log | awk -f $scripts/std.awk )"
#-#    # files with low contrast:
#-#	awk '{if ($10 < 2) print $0}' pscamp_tb3.log > pscamp_low.log
#-#	nlow=$(cat pscamp_low.log 2> /dev/null | wc -l) ; nl=$(cat pscamp_tb3.log | wc -l)
#-#	if [ $nl -ge 1 ]; then
#-#		ec "# Found $nlow of $nl files with low contrast ($(echo "100 * $nlow / $nl" | bc)%). "
#-#	else
#-#		ec "# Found $nlow of $nl files with low contrast "
#-#		rm pscamp_low.log
#-#	fi
#-#else
#-#	ec "# No logfile - no stats on contrast"
#-#fi

# rename the pngs to have the filter name and the pass - just to rename the png files
if [ $FILTER == 'NB118' ]; then FILTER='N'; fi
if [ $FILTER == 'Ks' ];    then FILTER='K'; fi

rename _1.png _${FILTER}.png [f,i,r]*_1.png
#rename .png _${FILTER}.png distort*.png

#-----------------------------------------------------------------------------
# and finish up
#-----------------------------------------------------------------------------
ec " >>>>  pscamp finished - walltime: $(wt)  <<<<"
ec "#-----------------------------------------------------------------------------"
ec ""
exit 0

#-----------------------------------------------------------------------------

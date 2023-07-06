#!/bin/sh
#PBS -S /bin/sh
#PBS -N sw_@FILTER@_@PAW@
#PBS -o @IDENT@_@PAW@.out            
#PBS -j oe
#PBS -l nodes=1:ppn=@PPN@:hasnogpu,walltime=48:00:00
#-----------------------------------------------------------------------------
# pswarp:  pswarp script
# requires: astropy.io.fits, uvis scripts and libs
# NB: walltime somewhat more that 24 hr needed for ~2000 frames
#-----------------------------------------------------------------------------

set -u  
# paths
export PATH="~/bin:$PATH:/softs/dfits/bin:/softs/astromatic/bin"
export PYTHONPATH="/home/moneti/uvis/python:/home/moneti/uvis/python_lib"

# add python and UltraVista scripts and libs
#module () {  eval $(/usr/bin/modulecmd bash $*); }
#module purge; module load intelpython/2

#-----------------------------------------------------------------------------
# Some variables and functions
#-----------------------------------------------------------------------------

ec() { echo "$(date "+[%d.%h.%y %T"]) $1 "; }    # echo with date
ecn() { echo -n "$(date "+[%d.%h.%y %T"]) $1 "; }    # echo with date
mycd() { \cd $1; ec " --> $PWD"; }               # cd with message
sdate=$(date "+%s")

uvis=/home/moneti/softs/uvis-pipe            # top UltraVista code dir
bindir=$uvis/bin
#pydir=$uvis/python                # python scripts
confdir=$uvis/config              # config dir
errcode=0

#-----------------------------------------------------------------------------
# Setup
#-----------------------------------------------------------------------------
module=pswarp                     # w/o .sh extension

# check  if run via shell or via qsub: 
echo ""
if [[ "$0" =~ "$module" ]]; then
    echo "## This is $module: running as shell script "
	WRK=$WRK
	FILTER=$FILTER
	if [[ "${@: -1}" == 'dry' ]]; then dry=T; else dry=F; fi
	subsky="N"
	list=$1     # presumably like list_paw3
	pass=2
	verb=" -VERBOSE_TYPE LOG"
else
    echo "## This is $module: running via qsub (from pipeline)"
	dry=@DRY@
	WRK=@WRK@
	list=@LIST@
	subsky=@SUBSKY@
	FILTER=@FILTER@
	pass=@PASS@
	verb=" -VERBOSE_TYPE LOG"
fi 

pawname=$(echo $list | cut -d\_ -f2-6 | cut -d\. -f1) 
outname=substack_${pawname}                          
#echo "DEBUG: pawname $pawname" ; echo "DEBUG: outname $outname" 

#-----------------------------------------------------------------------------
# The REAL work ... done in temporary workdir
#-----------------------------------------------------------------------------

datadir=$WRK/images            # reference dir containing data
rhost=$(echo $WRK | cut -c 2-4)  # host of WRK

if [[ "$rhost" =~ "c0" ]]; then
	ec "### On login node $rhost ... not good ... quitting"
	exit 0
fi

# build work dir name: 
dirname=$(echo $list | cut -d\. -f1)
whost=$(hostname)   #; echo "DEBUG: ref/work hosts: $rhost  $whost"

#if [[ $whost == 'n09' ]] || [[ $whost == 'n08' ]] || [[ $whost == 'n17' ]]; then
if [[ $whost == 'n08' ]] || [[ $whost == 'n17' ]]; then
    workdir=/${whost}data/${dirname}_$FILTER     # node with small scratch
else                        
    workdir=/scratch/${dirname}_$FILTER          # other node
fi

if [ ! -d $datadir ];  then echo "ERROR: $WRK/images not found ... quitting"; exit 5; fi
if [ ! -s $datadir/$list ]; then echo "ERROR: $list not found in $WRK/images ... quitting"; exit 5; fi

nl=$(cat $datadir/$list | wc -l)
echo "  =================================================================="
echo "    >>>>  Begin pswarp $list on $nl files <<<<"
echo "  ------------------------------------------------------------------"

cltag=@CLTAG@   # which clean files to use

ec "## Working on $(hostname); temprary work dir name is $workdir; data on $WRK"

# run these in a separate subdir to avoid problems with parallel runs
if [ -d $workdir ]; then 
	ec "## Found old $workdir - delete all its contents" 
	rm -rf $workdir/*
else
	ec "## Build new $workdir" 
	mkdir $workdir
	if [ $? -ne 0 ]; then 
		ec "ERROR: can't mkdir $workdir ... quitting"
		exit 3
	fi
fi

mycd $workdir ; sleep 1
ec "## For $pawname working in $(hostname):$(pwd)"  
ec "## outname $outname,  pass $pass"
ec "## ==> build links to data files"

ln -sf $datadir/$list .
for f in $(cat $list); do r=${f%.fits}
	if [ $pass -eq 1 ]; then 
		ln -sf $datadir/origs/${r}.fits $f
	else
		ln -sf $datadir/cleaned/${r}_${cltag}.fits $f
	fi
	ln -sf $datadir/weights/${r}_weight.fits .
	ln -sf @HEADSDIR@/${r}.head .
done

#### products and logfile written directly into work area to easily follow progress
outfile=$datadir/$outname  # to build products directly there
ln -sf $confdir/@HEADFILE@ $outfile.head 
logfile=$datadir/pswarp_$pawname.log   # rm $pass from name - 22.jan.22

# Build command line for DR5
# Keep the temp file for eventual debugging
args=" -c $confdir/swarp238.conf  -WEIGHT_SUFFIX _weight.fits  -WEIGHT_TYPE MAP_WEIGHT \
   -IMAGEOUT_NAME  ${outfile}.fits  -WEIGHTOUT_NAME ${outfile}_weight.fits   \
   -COMBINE_TYPE CLIPPED  -CLIP_SIGMA 2.8  -DELETE_TMPFILES N   \
   -RESAMPLE Y  -RESAMPLING_TYPE LANCZOS2  \
   -SUBTRACT_BACK $subsky  -WRITE_XML Y  -XML_NAME ${outname}.xml   "
#   -BACK_SIZE 256  -BACK_FILTERSIZE 5      # equivalent to extended objects option in mk_object_mask
   # -CLIP_WRITELOG Y  -CLIP_LOGNAME ${outname}_clip.log

# ATTN: lots of jobs aborted with  -COMBINE_BUFSIZE 16384!!  ok with 8192 (def)

ec "-----------------------------------------------------------------------------" > $logfile
ec " - logfile of pswarp.sh  " >> $logfile
ec " - found $list with $(cat $list | wc -l) entries" >> $logfile
ec "-----------------------------------------------------------------------------" >> $logfile

comm="swarp @$list $args   $verb "
echo "% $comm " >> $logfile

imroot=$(head -1 $list)
ec "## Input files are like:"
ecn "   "; ls -lh $imroot | tr -s ' ' | cut -d' ' -f9-11
ecn "   "; ls -lh ${imroot%.fits}.head | tr -s ' ' | cut -d' ' -f9-11
ecn "   "; ls -lh ${imroot%.fits}_weight.fits | tr -s ' ' | cut -d' ' -f9-11

if [ $dry == 'F' ]; then
	$comm >> $logfile 2>&1
	if [ $? -ne 0 ]; then ec "Execution ERROR ... "; errcode=2; fi

	nerr=$(grep Error $logfile | wc -l)
	if [ $nerr -ge 1 ]; then
		ecn "ERROR: found Errors in logfile:"
		grep Error $logfile
		ec "ERROR: check results in $workdir"
		errcode=4
	else
		ec "# swarp run successful, see $(ls -lh ${outfile}.fits | cut -d\  -f5-9) "
		ec "# $(tail -1 $logfile) "  # line showing "Add done" and exec time 
        ec "# Clean up ... delete $workdir"
		rm -rf $workdir
		rm -rf $workdir
	fi
    #-----------------------------------------------------------------------------
    # and finish up
    #-----------------------------------------------------------------------------
	edate=$(date "+%s"); dt=$(($edate - $sdate))
	echo " >>>> pswarp finished - total runtime: $dt sec  <<<<"
else
	echo "## Command line is:"
	echo " % $comm " 
	echo "## exit dry mode"
	errcode=10
fi
echo "------------------------------------------------------------------"
echo ""

exit $errcode
       

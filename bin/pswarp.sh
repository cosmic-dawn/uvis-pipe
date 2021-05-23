#!/bin/sh
#PBS -S /bin/sh
#PBS -N sw_@FILTER@_@PAW@
#PBS -o @IDENT@_@PAW@.out            
#PBS -j oe
#PBS -l nodes=1:ppn=22,walltime=44:00:00
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
echo "DEBUG: pawname $pawname" ; echo "DEBUG: outname $outname" 

#-----------------------------------------------------------------------------
# do the real work ....
#-----------------------------------------------------------------------------

datadir=$WRK/images            # reference dir containing data

# build work dir: 
rhost=$(echo $WRK | cut -c 2-4)  # host of WRK
if [[ "$rhost" =~ "c0" ]]; then
	echo "### On login node $rhost ... not good ... quitting"
	exit 0
fi

if [[ "$rhost" =~ $(hostname) ]]; then
	workdir=$WRK/images/${pawname}_$FILTER
#    remoteRun=0
else
	workdir=/scratch/${pawname}_$FILTER      # work dir
	
#    remoteRun=1
fi

if [ ! -d $datadir ];  then echo "ERROR: $WRK/images not found ... quitting"; exit 5; fi
if [ ! -s $datadir/$list ]; then echo "ERROR: $list not found in $WRK/images ... quitting"; exit 5; fi

nl=$(cat $datadir/$list | wc -l)
echo "=================================================================="
echo " >>>>  Begin pswarp $list on $nl files <<<<"
echo "------------------------------------------------------------------"

#mycd $datadir
echo "## Working on $(hostname); temprary work dir is $workdir; data on $WRK"

# run these in a separate subdir to avoid problems with parallel runs
if [ -d $workdir ]; then 
	echo "## Found old $workdir - delete  its contents" 
	rm -rf $workdir/*
else
	echo "## Build new $workdir" 
	mkdir $workdir
fi

mycd $workdir 
ec "## For $pawname working in $(hostname):$workdir"  
ec "## outname $outname,  pass $pass"
ec "## ==> build links to data files"

ln -sf $datadir/$list .
for f in $(cat $list); do r=${f%.fits}
	if [ $pass -eq 1 ]; then 
		ln -sf $datadir/origs/${r}.fits $f
	else
		ln -sf $datadir/cleaned/${r}_clean.fits $f
	fi
	ln -sf $datadir/weights/${r}_weight.fits .
	ln -sf $datadir/heads/${r}.head .
done

#### products and logfile written directly into work area to easily follow progress
outfile=$datadir/$outname  # to build products directly there
ln -sf $confdir/@HEADFILE@ $outfile.head 
logfile=$datadir/pswarp${pass}_$pawname.log

# Build command line for DR5
args=" -c $confdir/swarp238.conf -WEIGHT_SUFFIX _weight.fits  -WEIGHT_TYPE MAP_WEIGHT \
   -IMAGEOUT_NAME  ${outfile}.fits  -WEIGHTOUT_NAME ${outfile}_weight.fits   \
   -COMBINE_TYPE CLIPPED  -CLIP_SIGMA 2.8   \
   -RESAMPLE Y  -RESAMPLING_TYPE LANCZOS2  -FSCALASTRO_TYPE VARIABLE  -COMBINE_BUFSIZE 8192 \
   -SUBTRACT_BACK $subsky  -DELETE_TMPFILES Y  -NTHREADS 36 -WRITE_XML Y  -XML_NAME ${outname}.xml   "
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
	nerr=$(grep Error $logfile | wc -l)
	if [ $nerr -ge 1 ]; then
		ecn "ERROR: found Errors in logfile:"
		grep Error $logfile
		ecn "ERROR: check results in $workdir"
		errcode=4
	else
		ec "# swarp run successful "
		ec "# $(tail -1 $logfile) "
		ec "# mv products back to $WRK "
		#mv substack*.fits $datadir
		errcode=0
        ec "# Clean up ... delete $workdir"; rm -rf $workdir
	fi
	mv ${outname}*.*  $datadir
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
       

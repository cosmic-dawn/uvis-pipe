#!/bin/sh
#PBS -S /bin/sh
#PBS -N qFits_@FILTER@_@ID@
#PBS -o qFits_@ID@.out
#PBS -j oe
#PBS -l nodes=1:ppn=11,walltime=24:00:00
#-----------------------------------------------------------------------------
# pseudo qFits: 
# requires: astromatic s/w, dfits, python
# Dec 2019: add ldac saturation flagging
#-----------------------------------------------------------------------------

set -u  
umask 022
export PATH="~/bin:$PATH:/softs/dfits/bin:/softs/astromatic/bin"

module () {  eval $(/usr/bin/modulecmd bash $*); }
module purge ; module load intelpython/3-2020.2
export LD_LIBRARY_PATH=/lib64:${LD_LIBRARY_PATH}

sdate=$(date "+%s")

#-----------------------------------------------------------------------------
# Some variables
#-----------------------------------------------------------------------------
module=qFits_p2
uvis=/home/moneti/softs/uvis-pipe
bindir=$uvis/bin
pydir=$uvis/python                 # python scripts
qFitsdir=$uvis/config/qFits        # config dir

export PYTHONPATH=$pydir

#-----------------------------------------------------------------------------
# check  if run via shell or via qsub:
#-----------------------------------------------------------------------------

node=$(hostname)   # NB: compute nodes don't have .iap.fr in name

if [[ "$0" =~ "$module" ]]; then
    echo "$module: running as shell script on $node"
	list=$1
	WRK=$WRK
	FILTER=$FILTER
	if [[ "${@: -1}" =~ 'dry' ]]; then dry=T; else dry=F; fi
else
    echo "$module: running via qsub (from pipeline) on $node with 8 threads"
	dry=@DRY@
	WRK=@WRK@
	list=@LIST@
	FILTER=@FILTER@
fi

#-----------------------------------------------------------------------------
# Other functions ... need the above variables
#-----------------------------------------------------------------------------

ec() {    # echo with date
    if [ $dry == 'T' ]; then echo "[DRY MODE] $1";
    else echo "$(date "+[%d.%h %T]") $1 " 
    fi
} 
ecn() {     # idem for -n
    if [ $dry == 'T' ]; then echo -n "[DRY MODE] $1"
    else echo -n "$(date "+[%d.%h %T]") $1 " 
    fi 
}

#-----------------------------------------------------------------------------
cd $WRK/images ; echo "-> $PWD"
if [ $? -ne 0 ];   then ec "ERROR: $WRK/images not found ... quitting"; exit 10; fi

if [ ! -s $list ]; then ec "ERROR: $list not found in $WRK/images ... quitting"; exit 10; fi

nl=$(cat $list | wc -l)
ec "=================================================================="
ec " >>>>  Begin qFits $list with $nl entries  <<<<"
ec "------------------------------------------------------------------"
ecn " In     "; echo $WRK
ecn " Using  "; ww -v
ecn " Using  "; sex -v
ecn " Using  "; psfex -v
ecn " pydir is   "; echo $pydir
ecn " bindir is  "; echo $bindir
ecn " confdir is "; echo $qFitsdir
ec "------------------------------------------------------------------"

info=$(mktemp)
logfile=$(echo $list | cut -d\. -f1).log

if [ ! -d weights ]; then mkdir weights; fi
if [ ! -d  ldacs ];  then mkdir  ldacs ; fi
if [ ! -d   regs ];  then mkdir   regs ; fi
if [ ! -d   logs ];  then mkdir   logs ; fi

#-----------------------------------------------------------------------------
# To do first in test area: 
# - build links to cleaned images and build FileInfo.dat
#   . in link name drop _clean from original name
#   . links are built into "origs" dir to have
# 8.feb.22: no longer needed: cleaned "dir" is link to $dr5/N/images/cleaned 
#-----------------------------------------------------------------------------
# Now do real work
#-----------------------------------------------------------------------------

dr5=/n08data/UltraVista/DR5
imdir=$WRK/images    

#-----------------------------------------------------------------------------
# copy param files:
if [ ! -e vircam.ret ]; then 
	cd $qFitsdir;
	cp -a default.nnw gauss_3.0_7x7.conv $WRK/images
	cp -a sex_cosmic.config sex_cosmic.param vircam.ret $WRK/images
	cp -a sex_scamp.config sex_scamp.param  ww_weight.config $WRK/images
	ec "###  copied config and param files ###"; ec ""
fi

#-----------------------------------------------------------------------------
cd $imdir

#verb=" -VERBOSE_TYPE QUIET"
verb=" -VERBOSE_TYPE NORMAL"

for f in $(cat $list); do

   root=${f%.fits}
   if [ ! -e $f ]; then ln -s cleaned/${root}_clean.fits $f ; fi

   ec "--------------  Begin work on v20100116_00385.fits  --------------"

   grep $f ../FileInfo.dat | tr -s ' ' > $info  # get associated files
   flat=../calib/$(cut -d\  -f4 $info)          # flatfield
   norm=${flat%.fits}_norm.fits                 # normalised flat
   bpm=/n08data/UltraVista/DR5/bpms/$(cut -d\  -f5 $info)           # bpm mask

   pb=0
   if [ ! -s $flat ]; then ec " ERROR: $flat not found;"; pb=1; fi
   if [ ! -s $norm ]; then ec " ERROR: $norm not found;"; pb=1; fi
   if [ ! -s $bpm  ]; then ec " ERROR: $bpm  not found;"; pb=1; fi
   if [ $pb -ge 1 ]; then ec " ... quitting ..."; exit 10; fi

   #-----------------------------------------------------------------------------
   # 1. SExtractor to get cosmics
   #    NB: for pass 2 there should not be any cosmics
   #-----------------------------------------------------------------------------

   flag=${root}_flag.fits           # ; touch $flag    
   cosmic=${root}_cosmic.fits       # ; touch $cosmic
   weight=${root}_weight.fits       # output ... later

   args=" -c sex_cosmic.config -PARAMETERS_NAME sex_cosmic.param -CATALOG_TYPE NONE  \
       -FILTER_NAME vircam.ret  -SATUR_KEY TOTO -SATUR_LEVEL 30000 -WRITE_XML N  "
   if [ ! -e $cosmic ]  ; then 
       coscomm="sex $f -CHECKIMAGE_NAME $cosmic  $args $verb"
       ec ""; ec ">>>> 1. SEx for cosmics for "$f
	   logfile=${root}_se1.err
	   errfile=${root}_se1.log
	   
	   if [ $dry == 'T' ]; then
		   echo $coscomm
		   echo " ## DRY MODE - do nothing ## "
       else
      	   $coscomm 1> $logfile 2> $errfile
		   if [ $? -ne 0 ]; then ec "ERROR ... quitting"; exit 1; fi
      	   ec " ==> $cosmic built ..."    
       fi
   else
	   ec "# Attn: found $cosmic ... skip "
   fi
   
   #-----------------------------------------------------------------------------
   # 2. ww to build weight file
   #    NB. for pass 2 the mean is near 0; also don't use image
   #        itself to reject pixels; 
   #-----------------------------------------------------------------------------

   if [ ! -e $weight ]; then 
	  nin=2         # number of input files to build weight
      # arguments
      args=" -c ww_weight.config  -OUTWEIGHT_NAME $weight \
         -WEIGHT_NAMES $norm,$cosmic,$bpm  -WRITE_XML N \
         -WEIGHT_MIN 0.7,-1,0.5 -WEIGHT_MAX 1.3,0.1,1.5 -WEIGHT_OUTFLAGS 0,1,2 \
         -POLY_OUTFLAGS 3  -POLY_OUTWEIGHTS 0.0  -OUTFLAG_NAME $flag  -POLY_INTERSECT N "
      
      wwcomm=" ww $args $verb" 
	  ec ""; ec ">>>> 2. WW for weight for "$f
	  logfile=${root}_weight.log
	  errfile=${root}_weight.err

      if [ $dry == 'T' ]; then
		  echo $wwcomm
		  echo " ## DRY MODE - do nothing ## "
      else
      	  $wwcomm  1> $logfile 2> $errfile
		  if [ $? -ne 0 ]; then ec "ERROR ... quitting"; exit 2; fi
		  $pydir/cp_astro_kwds.py -i $f -s _weight  >> $logfile
		  mv $logfile logs
		  if [ ! -s $errfile ]; then rm $f; else mv $errfile logs; fi
      	  ec " ==> $weight built ... moved to weights dir"   
		  rm $cosmic ${root}_se1.???
	  fi
   else
	   ec "# Attn: found $weight ... skip "
   fi
   
   #-----------------------------------------------------------------------------
   # 3. SExtractor - for scamp
   #-----------------------------------------------------------------------------

   ldac=$root.ldac
   if [ ! -e $ldac ]; then 
      args=" -CATALOG_NAME $ldac  -WEIGHT_IMAGE $weight  -FLAG_IMAGE $flag  \
        -DETECT_THRESH 10.  -ANALYSIS_THRESH 5. -SATUR_KEY TOTO  -SATUR_LEVEL 35000 "
      
      sexcomm="sex $f -c sex_scamp.config  $args  $verb" 
      ec ""; ec ">>>> 3. SEx for scamp for "$f
	  logfile=${root}_se3.err
	  errfile=${root}_se3.log
	  
      if [ $dry == 'T' ]; then
		  ec "$sexcomm"
		  echo " ## DRY MODE - do nothing ## "
      else
      	  $sexcomm  1> $logfile 2> $errfile
		  if [ $? -ne 0 ]; then ec "ERROR ... quitting"; exit 5; fi
      	  ec " ==> $ldac built ... moved to ldacs"
		  mv $weight weights
		  $pydir/ldac2region.py $ldac
		  mv $ldac ldacs
		  mv $root.reg regs
		  mv $logfile logs
		  if [ ! -s $errfile ]; then rm $f; else mv $errfile logs; fi
		  rm $root.fits $flag # first one is a link, no longer needed
      fi
   else
	   ec "# Attn: found $ldac ... skip "
   fi

#   exit 0 ##### QUIT here for now

   ec "------------------------------------------------------------------"
   ec ""

   if [ $dry == 'T' ]; then
	   echo " ## DRY MODE - quitting after first loop ## "
	   exit 0
   fi

#   exit 0 ##### QUIT here for now
done

#$pydir/ldac2region.py v20*.ldac

# clean up: remove err logs of size 0
#for f in v20*.err; do if [ ! -s $f ]; then rm $f; fi; done
#rm vircam.ret *.config *.nnw *.param gauss*.conv 

#-----------------------------------------------------------------------------

edate=$(date "+%s"); dt=$(($edate - $sdate))
rm $info
ec " >>>> qFits finished - total runtime: $dt sec  <<<<"
ec "------------------------------------------------------------------"
echo ""
exit 0

#-----------------------------------------------------------------------------


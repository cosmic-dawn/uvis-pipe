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

#ec() { echo "$(date "+[%d.%h.%y %T"]) $1 "; }    # echo with date
#ecn() { echo -n "$(date "+[%d.%h.%y %T"]) $1 "; }    # echo with date

#-----------------------------------------------------------------------------
# Some variables
#-----------------------------------------------------------------------------
module=qFits
uvis=/home/moneti/softs/uvis-pipe
bindir=$uvis/bin
pydir=$uvis/python                # python scripts
qFitsdir=$uvis/config/qFits        # config dir

export PYTHONPATH=$pydir

#-----------------------------------------------------------------------------
# check  if run via shell or via qsub:
#-----------------------------------------------------------------------------

node=$(hostname)   # NB: compute nodes don't have .iap.fr in name

if [[ "$0" =~ "$module" ]]; then
    ec "$module: running as shell script on $node"
	list=$1
	WRK=$WRK
	FILTER=$FILTER
	if [[ "${@: -1}" =~ 'dry' ]]; then dry=T; else dry=F; fi
else
    ec "$module: running via qsub (from pipeline) on $node with 8 threads"
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
cd $WRK/images

if [ $? -ne 0 ]; then ec "ERROR: $WRK/images not found ... quitting"; exit 10; fi
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

cd $WRK/images ; echo "-> $PWD"
info=$(mktemp)
logfile=$(echo $list | cut -d\. -f1).log

#-----------------------------------------------------------------------------
# To do first in test area:
# . head files are named .ahead as specified in SEx config files, and to 
#   distinguish them from head files to be built later by scamp.
#-----------------------------------------------------------------------------
dr5=/n08data/UltraVista/DR5
imdir=$WRK/images    
im1=$(head -1 $imdir/$list)  # first image of list 
#echo $imdir $im1 ; exit 0    #### DEBUG

if [ ! -e $imdir/$im1 ]; then
	echo "## Setup ... build links to $(cat $list | wc -l)images and their heads"
	echo "## Source directory is \$dr5/$FILTER/images/"
	if [ $dry == 'T' ]; then
		echo " ## DRY MODE - do nothing ## "
		exit 0
	fi
    for f in $(cat $imdir/$list); do 
		ln -sf $dr5/$FILTER/images/cleaned/${f%.fits}_clean.fits $imdir/${f%_clean.fits}
#		ln -sf $dr5/$FILTER/images/heads/${f%.fits}.head $imdir/${f%.fits}.ahead 
#		ln -sf $dr5/$FILTER/images/heads/${f%.fits}.head $imdir/${f%.fits}_weight.ahead 
	done
	echo "## Setup ... build FileInfo.dat"
    cd $imdir
	for f in v20*[0-9].fits; do grep ${f%.fits} $dr5/$FILTER/FileInfo.dat ; done > ../FileInfo.dat
	echo "   ==> FileInfo.dat contains $(cat $WRK/FileInfo.dat | wc -l) entries ..."
	echo "## Setup complete ... quitting"; exit 0
else
	echo "## Found $(cat $imdir/$list | wc -l) images ... continue"
fi

#-----------------------------------------------------------------------------
# Now do real work
#-----------------------------------------------------------------------------

cd $imdir

# copy param files:
if [ ! -e psfex.config ]; then 
	cd $qFitsdir;
	cp -a sex_cosmic.config sex_cosmic.param vircam.ret $WRK/images
	cp -a sex_psfex.config sex_psfex.param default.nnw gauss_3.0_7x7.conv $WRK/images
	cp -a psfex.config sex_scamp.config sex_scamp.param  ww_weight.config $WRK/images
	echo "### copied config and param files"; echo
	cd -
fi

verb=" -VERBOSE_TYPE QUIET"
verb=" -VERBOSE_TYPE NORMAL"

for f in $(cat $list); do

   bdate=$(date "+%s")

   root=${f%.fits}
   if [ ! -e $f ]; then ln -s origs/$f . ; fi

   echo "##  -----------------  Begin work on $f  -----------------"
   grep $f ../FileInfo.dat | tr -s ' ' > $info  # get associated files
#   cat $info   ##DEBUG
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

   if [ ! -e $cosmic ]  ; then 
       args=" -c sex_cosmic.config -PARAMETERS_NAME sex_cosmic.param   \
             -FILTER_NAME vircam.ret  -CHECKIMAGE_NAME $cosmic  -CATALOG_TYPE NONE \
             -SATUR_KEY TOTO -SATUR_LEVEL 30000 -WRITE_XML N  "

      coscomm="sex ${root}.fits $args $verb"
      ec ""; ec ">>>> 1. SEx for cosmics for "$f
	  logfile=${root}_sex1.err
	  errfile=${root}_sex1.log

      echo $coscomm
	  if [ $dry == 'T' ]; then
		 echo " ## DRY MODE - do nothing ## "
      else
      	  $coscomm 1> $logfile 2> $errfile
		  if [ $? -ne 0 ]; then ec "ERROR ... quitting"; exit 1; fi
      	  ec " ==> $cosmic built ..."    ; sleep 1
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

	  echo $wwcomm
      if [ $dry == 'T' ]; then
		 echo " ## DRY MODE - do nothing ## "
      else
      	 $wwcomm  1> $logfile 2> $errfile
		 if [ $? -ne 0 ]; then ec "ERROR ... quitting"; exit 2; fi
		 $pydir/cp_astro_kwds.py -i $f -s _weight # >> $logfile
      	 ec " ==> $weight built ... moved to weights dir"   ; sleep 1  #; rm $cosmic 
	  fi
   else
	  ec "# Attn: found $weight ... skip "
   fi

#   #-----------------------------------------------------------------------------
#   # 3. SExtractor for psfex
#   #-----------------------------------------------------------------------------
#
#   psfx=${root}_psfex.xml           #   ; touch $psfx
#   pdac=${root}_psfex.ldac          #   ; touch $pdac
#   if [ ! -e $pdac ]; then 
#      args=" -c sex_psfex.config  -PARAMETERS_NAME sex_psfex.param \
#             -BACK_SIZE 128 -BACK_FILTERSIZE 3  -CATALOG_NAME $pdac  -CHECKIMAGE_TYPE NONE \
#             -WEIGHT_IMAGE $weight   -FLAG_IMAGE $flag  -WRITE_XML N \
#             -STARNNW_NAME default.nnw  -FILTER_NAME gauss_3.0_7x7.conv  \
#             -DETECT_THRESH 50. -ANALYSIS_THRESH 5. -SATUR_KEY TOTO -SATUR_LEVEL 25000 "
#
#      psexcomm="sex $f $args  $verb"
#      ec ""; ec ">>>> 3. SEx for PSFEx for "$f
#      echo $psexcomm
#	  logfile=${root}_sex2.err
#	  errfile=${root}_sex2.log
#
#      if [ $dry == 'T' ]; then
#		 echo " ## DRY MODE - do nothing ## "
#      else
#      	  $psexcomm   1> $logfile 2> $errfile
#		  if [ $? -ne 0 ]; then ec "ERROR ... quitting"; exit 3; fi
#      	  ec " ==> $pdac built ..."    ; sleep 1
#      fi
#   else
#	  ec "# Attn: found $pdac .... skip "
#   fi
#
#   #-----------------------------------------------------------------------------
#   # 4. PSFEx - for stats only; don't need more
#   #-----------------------------------------------------------------------------
#
#   if [ ! -e $psfx ]; then
#      args=" -c psfex.config  -WRITE_XML Y -XML_NAME $psfx  \
#             -CHECKPLOT_TYPE NONE  -CHECKIMAGE_TYPE NONE  -NTHREADS 2"
#      psfcomm="psfex $pdac  $args  $verb"
#      ec ""; ec ">>>> 4. PSFEx for "$f
#      echo $psfcomm
#	  logfile=${root}_psfex.err
#	  errfile=${root}_psfex.log
#      if [ $dry == 'T' ]; then
#		 echo " ## DRY MODE - do nothing ## "
#      else
#      	  $psfcomm   1> $logfile 2> $errfile
#		  if [ $? -ne 0 ]; then ec "ERROR ... quitting"; exit 4; fi
#		  rm ${root}_psfex.psf #$pdac
#      	  ec " ==> $psfx  built ... "    ; sleep 1
#      fi
#   else
#	  ec "# Attn: found $psfx ..... skip "
#   fi
#
   #-----------------------------------------------------------------------------
   # 5. SExtractor - for scamp
   #-----------------------------------------------------------------------------

   ldac=$root.ldac
   if [ ! -e $ldac ]; then 
      args=" -CATALOG_NAME $ldac  -WEIGHT_IMAGE $weight  -FLAG_IMAGE $flag  \
        -DETECT_THRESH 10.  -ANALYSIS_THRESH 5. -SATUR_KEY TOTO  -SATUR_LEVEL 35000 "
      
      sexcomm="sex $f -c sex_scamp.config  $args  $verb" 
      ec ""; ec ">>>> 5. SEx for scamp for "$f
      ec "$sexcomm"
	  logfile=${root}_sex3.err
	  errfile=${root}_sex3.log

      if [ $dry == 'T' ]; then
		  echo " ## DRY MODE - do nothing ## "
      else
      	  $sexcomm  1> $logfile 2> $errfile
		  if [ $? -ne 0 ]; then ec "ERROR ... quitting"; exit 5; fi
      	  ec " ==> $ldac built ... "  #; rm $flag
		  $pydir/ldac2region.py $ldac  ; sleep 1
      fi
   else
	   ec "# Attn: found $ldac ... skip "
   fi

#   exit 0 ##### QUIT here for now

#   #-----------------------------------------------------------------------------
#   # 6. Flag saturated sources in ldac for scamp
#   #-----------------------------------------------------------------------------
#   sdac=${ldac%.ldac}_noSAT.ldac
#   if [ ! -e $sdac ]; then 
#	  satcomm="$pydir/flag_saturation.py -c $ldac "  #--noplot "
#      ec ""; ec ">>>> 6. Flag saturated sources in "$ldac
#	  ec "$satcomm"
#      if [ $dry == 'T' ]; then
#		  echo " ## DRY MODE - do nothing ## "
#      else
#		  $satcomm 
#		  if [ $? -ne 0 ]; then ec "ERROR ... quitting"; exit 6; fi
#		  ec " ==> ${sdac} built ... "  ; sleep 1
#      fi
#   else
#	  ec "# Attn: found ${sdac} ... skip "
#   fi
#

   edate=$(date "+%s"); dt=$(($edate - $bdate))
   ec "------------------------------------------------------------------"
   ec " >>>> Done - runtime: $dt sec  <<<<"
   ec "------------------------------------------------------------------"
   ec ""

   if [ $dry == 'T' ]; then
	   echo " ## DRY MODE - quitting after first loop ## "
	   exit 0
   fi

echo 
#   exit 0 ##### QUIT here for now
done

# clean up: remove err logs of size 0
for f in v20*.err; do if [ ! -s $f ]; then rm $f; fi; done
mv v20?????*weight.fits weights
mv v20??????_?????.ldac ldacs
mv v20??????_?????.reg  regs
mv v20*.log             logs
rm v20*cosmic.fits v20*flag.fits

#-----------------------------------------------------------------------------

edate=$(date "+%s"); dt=$(($edate - $sdate))
rm $info
ec " >>>> qFits finished - total runtime: $dt sec  <<<<"
ec "------------------------------------------------------------------"
echo ""
exit 0

#-----------------------------------------------------------------------------


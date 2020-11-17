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

ec() { echo "$(date "+[%d.%h.%y %T"]) $1 "; }    # echo with date
ecn() { echo -n "$(date "+[%d.%h.%y %T"]) $1 "; }    # echo with date

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

node=$(hostname)   # NB: compute nodes don't have .iap.fr in name

# check  if run via shell or via qsub:
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

verb=" -VERBOSE_TYPE QUIET"

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

cd $WRK/images
info=$(mktemp)
logfile=$(echo $list | cut -d\. -f1).log

for f in $(cat $list); do

   bdate=$(date "+%s")

   root=${f%.fits}
   if [ ! -e $f ]; then ln -s origs/$f . ; fi

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
   # 1. sex to get cosmics
   #-----------------------------------------------------------------------------

#   chmod 644 $f; $pydir/rm_pv_kwds.py $f; chmod 444 $f

   flag=${root}_flag.fits           # ; touch $flag    
   cosmic=${root}_cosmic.fits       # ; touch $cosmic
   weight=${root}_weight.fits       # output ... later

   if [ ! -e $weight ]  ; then 
       args=" -c sex_cosmic.config -PARAMETERS_NAME sex_cosmic.param \
             -FILTER_NAME vircam.ret  -CHECKIMAGE_NAME $cosmic  -CATALOG_TYPE NONE \
             -SATUR_KEY TOTO -SATUR_LEVEL 30000 -WRITE_XML N  "

      coscomm="sex ${root}.fits $args $verb"
      ec ""; ec ">>>> 1. SEx for cosmics for "$f
      echo $coscomm
	  if [ $dry == 'T' ]; then
		 echo " ## DRY MODE - do nothing ## "
      else
      	  $coscomm 
		  if [ $? -ne 0 ]; then ec "ERROR ... quitting"; exit 1; fi
      	  ec " ==> $cosmic built ..."    ; sleep 1
      fi
   else
	  ec "# Attn: found $cosmic ... skip "
   fi

   #-----------------------------------------------------------------------------
   # 2. ww to build weight file
   #-----------------------------------------------------------------------------

   if [ ! -e $weight ]; then 
	  nin=4         # number of input files to build weight
      # arguments
      args=" -c ww_weight.config  -OUTWEIGHT_NAME $weight \
         -WEIGHT_NAMES $norm,$f,$cosmic,$bpm  -WRITE_XML N \
         -WEIGHT_MIN 0.7,10,-1,0.5 -WEIGHT_MAX 1.3,50000,0.1,1.5 -WEIGHT_OUTFLAGS 0,1,2,4 \
         -POLY_OUTFLAGS 3  -POLY_OUTWEIGHTS 0.0  -OUTFLAG_NAME $flag  -POLY_INTERSECT N "
      
      wwcomm=" ww $args $verb" 
	  ec ""; ec ">>>> 2. WW for weight for "$f
	  echo $wwcomm
      if [ $dry == 'T' ]; then
		 echo " ## DRY MODE - do nothing ## "
      else
      	 $wwcomm 
		 if [ $? -ne 0 ]; then ec "ERROR ... quitting"; exit 2; fi
#		 $pydir/cp_astro_kwds.py -i $f -s _flag   # >> $logfile
		 $pydir/cp_astro_kwds.py -i $f -s _weight # >> $logfile
      	 ec " ==> $weight built ... "   ; sleep 1  #; rm $cosmic 
	  fi
   else
	  ec "# Attn: found $weight ... skip "
   fi

   #-----------------------------------------------------------------------------
   # 3. SExtractor for psfex
   #-----------------------------------------------------------------------------

   psfx=${root}_psfex.xml           #   ; touch $psfx
   pdac=${root}_psfex.ldac          #   ; touch $pdac
   if [ ! -e $pdac ]; then 
      args=" -c sex_psfex.config  -PARAMETERS_NAME sex_psfex.param   \
             -BACK_SIZE 128 -BACK_FILTERSIZE 3  -CATALOG_NAME $pdac  -CHECKIMAGE_TYPE NONE \
             -WEIGHT_IMAGE $weight   -FLAG_IMAGE $flag  -WRITE_XML N \
             -STARNNW_NAME default.nnw  -FILTER_NAME gauss_3.0_7x7.conv  \
             -DETECT_THRESH 50. -ANALYSIS_THRESH 5. -SATUR_KEY TOTO -SATUR_LEVEL 25000 "

      psexcomm="sex $f $args  $verb"
      ec ""; ec ">>>> 3. SEx for PSFEx for "$f
      echo $psexcomm
      if [ $dry == 'T' ]; then
		 echo " ## DRY MODE - do nothing ## "
      else
      	  $psexcomm 
		  if [ $? -ne 0 ]; then ec "ERROR ... quitting"; exit 3; fi
      	  ec " ==> $pdac built ..."    ; sleep 1
      fi
   else
	  ec "# Attn: found $pdac .... skip "
   fi

   #-----------------------------------------------------------------------------
   # 4. PSFEx - for stats only; don't need more
   #-----------------------------------------------------------------------------

   if [ ! -e $psfx ]; then
      args=" -c psfex.config  -WRITE_XML Y -XML_NAME $psfx  \
             -CHECKPLOT_TYPE NONE  -CHECKIMAGE_TYPE NONE  -NTHREADS 2"
      psfcomm="psfex $pdac  $args  $verb"
      ec ""; ec ">>>> 4. PSFEx for "$f
      echo $psfcomm
      if [ $dry == 'T' ]; then
		 echo " ## DRY MODE - do nothing ## "
      else
      	  $psfcomm 
		  if [ $? -ne 0 ]; then ec "ERROR ... quitting"; exit 4; fi
		  rm ${root}_psfex.psf #$pdac
      	  ec " ==> $psfx  built ... "    ; sleep 1
      fi
   else
	  ec "# Attn: found $psfx ..... skip "
   fi

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
      if [ $dry == 'T' ]; then
		  echo " ## DRY MODE - do nothing ## "
      else
      	  $sexcomm 
		  if [ $? -ne 0 ]; then ec "ERROR ... quitting"; exit 5; fi
      	  ec " ==> $ldac built ... "  #; rm $flag
#		  $pydir/ldac2region.py $ldac  ; sleep 1
      fi
   else
	  ec "# Attn: found $ldac ... skip "
   fi

   #-----------------------------------------------------------------------------
   # 6. Flag saturated sources in ldac for scamp
   #-----------------------------------------------------------------------------
   sdac=${ldac%.ldac}_noSAT.ldac
   if [ ! -e $sdac ]; then 
	  satcomm="$pydir/flag_saturation.py -c $ldac "  #--noplot "
      ec ""; ec ">>>> 6. Flag saturated sources in "$ldac
	  ec "$satcomm"
      if [ $dry == 'T' ]; then
		  echo " ## DRY MODE - do nothing ## "
      else
		  $satcomm 
		  if [ $? -ne 0 ]; then ec "ERROR ... quitting"; exit 6; fi
		  ec " ==> ${sdac} built ... "  ; sleep 1
      fi
   else
	  ec "# Attn: found ${sdac} ... skip "
   fi

   edate=$(date "+%s"); dt=$(($edate - $bdate))

   ec "------------------------------------------------------------------"
   ec " >>>> Done - runtime: $dt sec  <<<<"
   ec "------------------------------------------------------------------"
   ec ""
done


#-----------------------------------------------------------------------------

edate=$(date "+%s"); dt=$(($edate - $sdate))
rm $info
ec " >>>> qFits finished - total runtime: $dt sec  <<<<"
ec "------------------------------------------------------------------"
echo ""
exit 0

#-----------------------------------------------------------------------------
# to cleanup:
rm qFits*.* images/v20*_0????_*.* images/v20*_00???.ldac images/v20*_00???.head

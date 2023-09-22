#!/bin/bash 
#-----------------------------------------------------------------------------
# File: uAlt.sh 
#   New version of uvis.sh, but to use the alternate sky (Alt) scripts
#-----------------------------------------------------------------------------
# Purpose:   Pipeline to process UltraVista DR6 data - by filter
# Requires: 
# - work directory with data, given by $WRK env. var.
# - python3, python scripts from terapix pipe adapted to python 3,
#            in ~/softs/uvis-pipe/python etc.
# - wrapper scripts in ~/softs/uvis-pipe/bin
# Author:    A. Moneti - Nov.19
#-----------------------------------------------------------------------------
#  UltraVista pipeline data processing steps:
#-----------------------------------------------------------------------------
# prelim: count, decompress, build rejected list
# setup: to build test areas ... NOT IMPLEMENTED
# P1: convert to WIRCam, fix flats, qFits, flag satur in ldacs
# - R3: convert $nimages images to WIRCam format (keyword conversion)"
# - R4: on flats: remove PV from the headers and normalise" 
# - R5: qFits.sh: pseudo qFits on $list with $(cat $list | wc -l) entries"
# - R6: Extract psf stats from $npsfx xml files and discard ones with bad PSF"
# P2: scamp, swarp, build stack and its mask, build obj masks
# - R7: prepare $nlists runs of pscamp.sh ... "
# - R8: destripe.sh: destripe $nimages casu images "
# - R9: pswarp.sh: swarp pass1 ... "
# - R10: pmerge.sh: Merge p1 substacks into $stout..."
#-----------------------------------------------------------------------------
## NB for DR6: P2 done externally with pssm.sh, but not needed in pipeline as
## global mask was built (externally) from DR5 full stacks
#-----------------------------------------------------------------------------
# P3: build object masks, add CASU sky,compute best sky 
# - R11: mkMasks.sh: build object masks (tmpdir) ==> _mask files  in Masks/
# - R12: addSky.sh:  add CASU skys      (local)  ==> _withSky files in withSky/
# - R13: mkSky.sh:   build good sky     (tmpdir) ==> _sky files   in mkSky/
# - R14: updateWeights.sh: overwrite    (local)  ==> _weight files updated
# - R15: subSky.sh: sub sky and clean   (tmpdir) ==> _clean files in cleaned/ 
# P4: subsky, destripe and bild final stack
# - R16: pswarp.sh: swarp pass2 lo res  (tmpdir) ==> substacks    in swarp_lr/
# - R17: pmerge.sh: merge lr substacks  (local)  ==> UVIS lo-res stacks
# Repeat R16/17 for high resol stacks
# - R16: pswarp.sh: swarp pass2 hi res  (tmpdir) ==> substacks    in swarp_hr/
# - R17: pmerge.sh: merge hr substacks  (local)  ==> UVIS hi-res stacks
#-----------------------------------------------------------------------------
# Versions:
# v3.00: initial version, from DR4 processing                      (22.nov.19)
# v3.01: updated up to conv. to WIRCAM, and more                   (29.nov.19)
# v3.02: updates for qFits pscamp, and more                        (21.oct.20)
# v3.03: split pscamp run by pairs of paws                         (09.dec.20)
# v3.04: general revision of swarp for pass 1                      (23.jan.21)
# v3.05: general debugging to end                                  (19.may.21)
# v3.06: misc. optimisations, mostly in logging                    (26.may.21)
# v3.07: minor fixes                                               (24.jun.21)
# v3.10: vairous updates (minor) for DR6                           (29.oct.22)
# v3.11: more updates (in P3) for DR6                              (06.apr.23)
# v3.12: moved R15 to P3 and other updates                         (14.apr.23)
# v3.13: copied uvis to uAlt with Alt scripts                      (10.may.23)
# v3.14: misc. fixes and corrections                               (11.aug.23)
##-----------------------------------------------------------------------------
set -u  # exit if a variable is not defined - recommended by Stephane

if [ $# -eq 0 ]; then # || [ $1 == 'help' ] || [ $1 == '-h' ]; then 
    echo "#-----------------------------------------------------------------------------"
    echo "This script is  "$(which $0)" to process UltraVista data"
    echo "#-----------------------------------------------------------------------------"
    echo "| Syntax: "
    echo "| - pipe step [auto]  begin at step pN of the processing; "
    echo "|                     if auto is given, then continue automatically "
    echo "| - pipe -h or help    print this help "
    echo "#-----------------------------------------------------------------------------"
    exit 0
fi 

if [[ "${@: -1}" =~ 'dry' ]] || [ "${@: -1}" == 'test' ]; then dry=T; else dry=F; fi
if [[ "${@: -1}" =~ 'aut' ]]; then pauto=T; else pauto=F; fi
if [[ "${@: -1}" =~ 'int' ]]; then int=T;  else int=F;  fi
if [[ "${@: -1}" =~ 'env' ]]; then dry=T; fi

if [ $dry == 'T' ]; then pauto=T ; fi
#-----------------------------------------------------------------------------

vers=$(grep '^# v3.' $0 | tail -1 | cut -c 4-7,67-79)
if [ $# -eq 0 ]; then
    echo "# SYNTAX:"
    echo "    uvis.sh option (dry or auto)"
    echo "  Needs WRK environment variable defined to be work dir."
    echo "#------------------------------------------------------------------ "
    echo "# data processing options:"
    grep "^### -" $0 | cut -c6-99
    exit 0
else
    if [[ $1 =~ "ver" ]]; then 
        echo ">> $0 version $vers"; exit 0
    fi 
fi

module() { eval $(/usr/bin/modulecmd bash $*); }
module purge ; module load intelpython/3-2019.4 cfitsio
export LD_LIBRARY_PATH=/lib64:${LD_LIBRARY_PATH}

#-----------------------------------------------------------------------------
# Some variables
#-----------------------------------------------------------------------------

uvis=/home/moneti/softs/uvis-pipe

bindir=$uvis/bin
pydir=$uvis/python
confdir=$uvis/config

if [ -z ${WRK+x} ]; then 
    echo "!! ERROR: must export WRK variable before starting" ; exit 2; 
else
    FILTER=$(echo $WRK | cut -d/ -f5)
fi

node=$(echo $WRK | cut -c 2-4)         # 
DR6=$(echo $WRK  | cut -d/ -f1-4)      # Base directory

REL="DR6-"$FILTER                      # used in names of some products

badfiles=$WRK/DiscardedFiles.list      # built and appended to during processing
fileinfo=$WRK/FileInfo.dat             # lists assoc files (bpm, sky, flat) and other info for each image

pipelog=${WRK}/uAlt.log ; if [ ! -e $pipelog ]; then touch $pipelog; fi
Trash=zRejected         ; if [ ! -d $Trash ]; then mkdir $Trash; fi


#-----------------------------------------------------------------------------
# Other options
#-----------------------------------------------------------------------------

do_hires=F    # to do or not hi-res stack
    
#-----------------------------------------------------------------------------
# Functions
#-----------------------------------------------------------------------------

wtime() {   # wall time: convert sec to H:M:S
    echo $(date "+%s.%N") $btime | awk '{print strftime("%H:%M:%S", $1-$2, 1)}'; 
}
ec() {    # echo with date
    if [ $dry == 'T' ]; then echo "[TEST MODE] $1";
    else echo "$(date "+[%d.%h %T]") $1 " | tee -a $pipelog 
    fi
} 
ecn() {     # idem for -n
    if [ $dry == 'T' ]; then echo -n "[TEST MODE] $1"
    else echo -n "$(date "+[%d.%h %T]") $1 " | tee -a $pipelog
    fi 
}

mycd() { 
    if [ -d $1 ]; then \cd $1; ec " --> $PWD"; 
    else echo "!! ERROR: $1 does not exit ... quitting"; exit 5; fi
}

imdir=${WRK}/images
curfiles() {   # list avaliable files for each frame
    if [ -d $WRK/images ]; then 
        nimages=$(ls -1 $WRK/images/v20*_0????.fits 2> /dev/null | wc -l) #;  echo $nimages
        if [ $nimages -eq 0 ]; then
            echo " ---------- Dirs still empty ---------- "
        else
            froot=$(cd $WRK ; ls images/v20*_0????.fits | head -$(($nimages / 2)) | tail -1 | cut -d\. -f1 | cut -d\/ -f2 )
            echo "#-------------------------------------------------------------------------------------------- "
            echo "# Found $nimages files like $froot in $PWD: " 
            echo "#-------------------------------------------------------------------------------------------- "
            \ls -Flhd $WRK/images/${froot}*.*   | cut -c27-399 | awk '{printf "#  "$0"\n"}' | sed 's|'$WRK/images'/||g'
            echo "#-------------------------------------------------------------------------------------------- "
            echo "# link references to files above " 
            echo "#-------------------------------------------------------------------------------------------- "
            \ls -FlhdL $WRK/images/${froot}*.*  | cut -c27-399 | awk '{printf "#  "$0"\n"}' | sed 's|'$WRK/images'/||g'
            echo "#-------------------------------------------------------------------------------------------- "
            echo "# Other files found in $PWD/subdirs: " 
            echo "#-------------------------------------------------------------------------------------------- "
            \ls -Flhd $WRK/images/*/${froot}*.* | cut -c27-399 | awk '{printf "#  "$0"\n"}' | sed 's|'$WRK/images'/||g'
            echo "#-------------------------------------------------------------------------------------------- "
        fi
    else 
        echo "----------  images dir not found; processing space not setup ---------- "
    fi  
}

askuser() {   # ask user if ok to continue
    echo -n " ==> Is this ok? (yes/no):  "  >> $pipelog
    while true; do read -p " ==> Is this ok? (yes/no): " answer
        echo $answer >> $pipelog
        case $answer in
            [yYpl]* ) ec "Continue ..."; break;;
            *       ) ec "Quitting ..."; exit 3;;
        esac
    done  
}

procenv() {   # Print some parameters for user to check
    echo "#-----------------------------------------------------------------------------"
    echo "# Processing environment: "
    echo "  - Release area is   "$(ls -d $DR6)
    echo "  - Working area is   "$(ls -d $WRK)" for Filter is "$FILTER
    echo "  - shell scripts in  "$(ls -d $bindir)
    echo "  - python scripts in "$(ls -d $pydir)
    echo "  - config files in   "$(ls -d $confdir)
    echo "  - pipe logfile is   "$(ls $pipelog)
    echo "  - Release tag is    "$REL
    echo "  - Found images directory with "$nimages" image files"
    echo "#-----------------------------------------------------------------------------"  
}

erract() { # what to do in case of error
    echo ""
    ec "!!! PROBLEM "; tail $logfile
    exit 5
}

pipehelp() {
    egrep -n '^elif' $0  | tr -s ' ' | grep -v ' : ' > t1
    egrep -n '## - R' $0 | tr -s ' ' | grep -v egrep > t2
    egrep -n '^#@@' $0   | tr -d '@' > t3  
    cat t1 t2 t3 | sort -nk1,1 | tr -s '#' | cut -d \# -f2 | tr -d \"
    rm t1 t2 t3
}
#-----------------------------------------------------------------------------
# Misc. checks
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# check / set params
#-----------------------------------------------------------------------------

if [ $# -eq 8 ]; then
    echo "#-----------------------------------------------------------------------------"
    echo " Running  $(which $0) to process UltraVista data, step "$1
    echo "#-----------------------------------------------------------------------------"
fi

if [ ! -e $WRK/images/list_images ]; then 
    cd $WRK/images
    echo "list_images not found - rebuild it ...."
    \ls -1 v20*_0????.fits > list_images
    ls -l $WRK/images/list_images
    cd $WRK
fi


nimages=$(cat $WRK/images/list_images | wc -l)
imroot=$(head -$(($nimages / 2))  $WRK/images/list_images | tail -1 | cut -d\. -f1 | cut -d\/ -f2 )

#
DR6=/n08data/UltraVista/DR6
bpmdir=/n08data/UltraVista/DR6/bpms

#-----------------------------------------------------------------------------------------------

ec "   #=================================================#"
ec "   #                                                 #"
ec "   #    This is uvis.sh ver $vers         #"
ec "   #                                                 #"
ec "   #=================================================#"

cd $WRK

#-----------------------------------------------------------------------------------------------
# First a dummy step to start sequence .... do nothing:
#-----------------------------------------------------------------------------------------------

if [ $# -ge 1 ] && [ $1 = 'xxx' ]; then
    echo " DUMMY step .... nothing to do"
	mycd $WRK/images
#    echo "CHECK: what's done ..."
    echo "#-----------------------------------------------------------------------------"
	if [ -e list_origs ]; then 
		echo "# Number of input (CASU) files ................ $(cat list_origs    | wc -l)"
	fi
	if [ -e list_accepted ]; then
		echo "# Number of files accepted for processing ..... $(cat list_accepted | wc -l)"
	fi
	if [ -e list_skies ]; then
		echo "# Number of files for which sky was built ..... $(cat list_skies    | wc -l)"
	fi
	if [ -e list_cleaned ]; then
		echo "# Number of files processed and cleaned ....... $(cat list_cleaned  | wc -l)"
	fi
	
    echo "#-----------------------------------------------------------------------------"
	ldir=$(ls -trd swarp_???_* | tail -1)
	echo "# Most recent substacks ... in $ldir:"
#	\cd $ldir ; 
	ls $ldir/subs*_s???.fits 2> /dev/null > x 
	nn=$(cat x  | wc -l) 
	if [ $nn -ge 1 ]; then 
		cat x
	else
		echo "# No substacks found "
	fi
#	\cd -
	
    echo "#-----------------------------------------------------------------------------"
	echo "# Final stacks:"
	ls UVIS*_??.fits 2> /dev/null > x 
	nn=$(cat x  | wc -l) 
	if [ $nn -ge 1 ]; then 
		cat x
	else
		echo "# No final stacks found "
	fi
    echo "#-----------------------------------------------------------------------------"

#-----------------------------------------------------------------------------------------------
#@@ ------------------------------------------------------------------------------------
#@@  UltraVista pipeline data processing steps:
#@@ ------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------
elif [[ $1 =~ 'prel' ]]; then      # P0: prelims: count, decompress, NOT FULLY AUTOMATED
#-----------------------------------------------------------------------------------------------
   
    ncurr=0 ; pcurr="p$ncurr" ; pnext="p$(($ncurr+1))"
    mycd $WRK
    
    ec "##----------------------------------------------------------------------------"
    ec "##          ======  Preliminaries  ======"
    ec "##----------------------------------------------------------------------------"
    
    nfit=$(ls -1 images/*.fit 2> /dev/null | wc -l)
    if [ $nfit -gt 10 ]; then
        ec " - Number of casu files in images:  $nfit "
        ec " - Number of casu files in stacks:  $(ls -1 stacks/*.fit  | wc -l) "
        ec " - Number of casu files in calib:   $(ls -1 calib/*.fit   | wc -l) "
    fi
#
# NB. No data rejection at this stage for DR5 - keep all frame for use in building sky; 
# BUT do not use these frame to build stack
#
    if [ ! -s reject_for_stack.lst ]; then 
        ec "#-----------------------------------------------------------------------------"
        ec "## - R0: Build lists of images rejected in DR1,4 - to exclude them from P1 stack"
        ec "#-----------------------------------------------------------------------------"
        
        grep $FILTER $DR5/DR1_rejected.lst | cut -d\, -f1 > rejected_dr1.lst
        grep $FILTER $DR5/DR4_badPSF.lst   | cut -d\  -f1 > rejected_dr4.lst
        nbad=$(cat rejected_dr?.lst | wc -l)
        ec "# Found $nbad files to reject (later) from DR1 and DR4"
        cat rejected_dr?.lst | sort -u | awk '{print $1".fits"}' > reject_for_stack.lst
        nn=$(cat reject_for_stack.lst | wc -l)
        ec "# - built rejected_dr[1,4].lst and reject_for_stack.lst with $nn files"
    else
        echo "## lists of images to discard already available - continue"
    fi

    if [ ! -e to_imcopy ]; then
        ec "#-----------------------------------------------------------------------------"
        ec "## - R1: decompress CASU files"
        ec "#-----------------------------------------------------------------------------"
    
        \ls -1 [i,c,s]*/*.fit | awk '{print "imcopy ",$1, $1"s"}' > to_imcopy
        if [ $dry == 'T' ]; then 
            ec " >> wrote to_imcopy to run inparallel mode"
            ec "----  EXITING PIPELINE DRY MODE         ---- "
            exit 10
        fi

        cat to_imcopy | parallel  -j 8
        mkdir $WRK/RawData/$FILTER
        mv [i,c,s]*/*.fit $WRK/RawData/FILT
        ec "# All .fit files decompressed to .fits, then moved to RawData/$FILT dir"
        chmod 444 to_imcopy    # serves as record that decompression has been done.
    else
        echo "## Decompression already done"
    fi

    #-----------------------------------------------------------------------------
    
    if [ $pauto == 'T' ]; then                                 # END P0
        ec "#-----------------------------------------------------------------------------"
        ec "# $pcurr finished; ==> auto continue to $pnext"
        ec "#-----------------------------------------------------------------------------"
        $0 $pnext $pauto ; exit 0
    else
        ec "#-----------------------------------------------------------------------------"
        ec "#                                End of $pcurr "
        ec "#-----------------------------------------------------------------------------"
    fi
 
#-----------------------------------------------------------------------------------------------
elif [[ $1 =~ 'setu' ]]; then      # - R2: (optional) build test areas - NOT IMPLEMENTED
#-----------------------------------------------------------------------------------------------
   
    bpmdir=/n08data/UltraVista/DR6/bpms
    rootdir=${WRK%/$FILTER}"/RawData/$FILTER" 

    echo "## CASU data (.fit files) in $rootdir"
    exit 0

    cd $WRK
    if [ ! -d images ]; then 
        ec "##----------------------------------------------------------------------------"
        ec "#"
        ec "##          ======  GENERAL SETUP  ======"
        ec "#"
        ec "##----------------------------------------------------------------------------"
        ec "## P0: Prepare converted data files"
        ec "#-----------------------------------------------------------------------------"

        mkdir images calib stacks 
        ln -sf $rootdir/images/v20*_0????.fits   images
#       ln -s $rootdir/stacks/*.fits    stacks  
        ln -sf $rootdir/calib/*.fits     calib   
        ln -sf $bpmdir/bpm*.fits        calib   # some were already in calib
        cp $rootdir/FileInfo.dat  FileInfo.full

        cd images
        \ls -1 v20*_0????.fits > list_images; nimages=$(cat list_images | wc -l)
        for f in v20*.fits; do grep $f ../FileInfo.full >> ../FileInfo.dat; done
        #echo HERE
        cd -
        ec "#-----------------------------------------------------------------------------"
        ec "## - built links to $nimages ${FILTER} images and their ancillary flats and skies:  "
        ec "#-----------------------------------------------------------------------------"
        ec " - Number of images files:      $(ls -1L images/v20*_0????.fits | wc -l) "
        ec " - Number paws included:        $(cut -d' ' -f2,2 FileInfo.dat | sort -u | wc -l) "
        ec " - Number of flats in calib:    $(ls -1L calib/${FILTER}*[0-9].fits     | wc -l) "
        ec " - Normalised flats in calib:   $(ls -1L calib/${FILTER}*norm.fits      | wc -l) "
        ec " - Number of skies in calib:    $(ls -1L calib/sky*.fits   | wc -l) "
        ec " - Number of bpms in calib:     $(ls -1L calib/bpm*.fits   | wc -l) "
        
        touch calib/norm_flat.log images/convert_to_WIRCAM.log images/RUN_mkFileInfo
        ec "#-----------------------------------------------------------------------------"
        ec "## - Continue with regular processing  "
        ec "#-----------------------------------------------------------------------------"
        exit 0
    else
        ec "#### ATTN: images/ and calib/ dirs already exist ... delete them and restart"
        exit 0
    fi

if ([ $HOSTNAME != "c03" ] || [ $HOSTNAME != "c02" ]) && [ $dry != 'T' ]; then  
    ec "#=========================================================#"
    ec "#### ATTN: cannot start jobs from $HOSTNAME. Switched to dry mode  #"
    ec "#=========================================================#"
    dry='T'
fi

#-----------------------------------------------------------------------------------------------
elif [ $1 = 'p1' ]; then      # P1: convert to WIRCam, fix flats, qFits, flag satur in ldacs
#-----------------------------------------------------------------------------------------------

#% P1: convert to WIRCam, fix flats, qFits, flag satur'n in ldacs
#% - convert various kwds to WIRCam (terapix) format:
#%   for each v20*_0????.fits file write a _WIRCam.fits file
#%   move the original .fits files to DR5/ConvData/$FILT/
#%   rename the WIRCam file to remove _WIRCam from the name
#% - convert kwds in flats, and produce normalised flags
#% - qFits:
#%   . builds weight files, 
#%   . builds ldacs for psfex and runs psfex to get psf size and reject files with bad psf
#%   . builds an ldac for use with scamp
#%   . determine saturation level from the scamp ldacs and flag saturated sources
#% - select frames based on PSF size
#%------------------------------------------------------------------

    ec "#-----------------------------------------------------------------------------"
    ec "## P1: convert to WIRCam, fix flats, pseudo-qFits, and flag saturation in ldacs"
    ec "#-----------------------------------------------------------------------------"
    ncurr=1 ; pcurr="p$ncurr" ; pprev=p$(($ncurr-1)) ; pnext="p$(($ncurr+1))"
 
    if [ ! -d $WRK/images ]; then 
        ec "#### PROBLEM: directory structure not setup ... quitting"
        exit 5
    fi
    mycd $WRK/images/origs

    # rebuild list_images
    #ls -1 v20*_0????.fits > list_images
    nimages=$(cat list_images | wc -l)

    if [ $nimages -eq 0 ]; then 
        ec "!! ERROR: no images found ... "
        askuser
    fi
    cd $WRK/images
        
    ec "CHECK: found list_images with $nimages entries "
    ec "#-----------------------------------------------------------------------------"

    # check for existing _WIRCam files
    nwirc=$(ls v20*_WIRCam.fits 2> /dev/null | wc -l)
    if [ $nwirc -gt 0 ]; then
        ec "#### ATTN: found $nwirc _WIRCam files ... delete them and start over"
        exit 0
    fi
        
    # to check conv to WIRCam, look for IMRED_FF and IMRED_MK kwds exist in ext.16
    nkwds=$(dfits origs/$(tail -1 list_images) | grep IMRED_ | wc -l) #; echo $nkwds; exit 0
 
    # still in $WRK/images; shell scripts ==> $WRK

    if [ -e convert_to_wircam.log ] && [ $nkwds -eq 2 ]; then
        ec "CHECK: found convert_images.log, and files seem to contain IMRED_?? keywds"
        ec "CHECK: ==> conversion to WIRCam already done ... skip it"
        ec "#-----------------------------------------------------------------------------"
    else
        rcurr=R3
        ec "#-----------------------------------------------------------------------------"
        ec "## - R3: convert $nimages images to WIRCam format (keyword conversion)"
        ec "#-----------------------------------------------------------------------------"
        
        rm -f chunk_?.lst ../convert_all
        chunk_size=2500
        if [ $nimages -gt $chunk_size ]; then 
            split -n l/$(($nimages/$chunk_size)) -a1 --additional-suffix='.lst' list_images chunk_ 
        else
            cp list_images chunk_a.lst
        fi

        for l in chunk_?.lst; do
            nl=$(cat $l | wc -l)
            id=$(echo $l | cut -d\_ -f2 | cut -d\. -f1) 
            qfile=$WRK/convert_$id.sh
            sed -e 's|@WRK@|'$WRK'|' -e 's|@FILTER@|'$FILTER'|'  -e 's|@ID@|'$id'|'  \
                -e 's|@LIST@|'$l'|'  -e 's|@NODE@|'$node'|'  -e 's|@DRY@|F|' \
                $bindir/pconvert.sh > $qfile            
                ec "# Built $qfile with $nl entries"
            echo "qsub $qfile; sleep 1" >> $WRK/convert_all
        done   

        mycd $WRK    ### back to $WRK to run jobs
        njobs=$(cat convert_all 2> /dev/null | wc -l)
        
        ec "# ==> written to file 'convert_all' with $njobs entries "
        ec "#-----------------------------------------------------------------------------"

        if [ $dry == 'T' ]; then 
            echo "   >> DRY MODE ... do nothing  << "
            exit 3
        else 
            rm -f pconv_?.out                    # clean up before submitting:
            ln -sf $confdir/Convert_keys.list images/.   # Need this confi file
            
            ec "# Submit qsub files ... ";  source convert_all >> $pipelog
            ec '# Begin wait loop -- wait for $njobs convert jobs to finish  --'
            btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
            while :; do                          # qsub wait loop 
                ndone=$(ls pconv_?.out 2> /dev/null | wc -l)
                [ $njobs -eq $ndone ] && break          # jobs finished
                sleep 60
            done  
            ec "# convert jobs finished, walltime: $(wtime) - check results ..."
            npb=$(grep PROBLEM pconv_?.out | wc -l)
            if [ $npb -ne 0 ]; then
                ec "#### PROBLEM: problem(s) found in .out files ... please check"
                grep PROBLEM pconv_?.out
                askuser
            else
                ec "## All scripts terminated successfully ..."
            fi
            
            mycd $WRK/images   # back here to check products
            # check for existing _WIRCam files
            nwirc=$(ls v20*_WIRCam.fits 2> /dev/null | wc -l)
            if [ $nwirc -ne $nimages ]; then
                echo "#### ATTN: found only $nwirc _WIRCam files for $nimages expected. "
                askuser
            fi
            
            exit 0
            cat conv_wircam_?.log | grep -v ^# > convert_to_wircam.log
            chmod 444 convert_to_wircam.log
            rm convert_full_?.log conv_wircam_?.log 
            #mv v20*_0????.fits ../../ConvData/$FILTER          # delete original CASU files
            rm v20*_0????.fits                                  # delete links to CASU files
            rename _WIRCam.fits .fits v20*_WIRCam.fits          # rename them to 'simple' names
            
            ec "#-----------------------------------------------------------------------------"
        fi
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi

    #----------------------------------------------------------------------------------------------#
 
    mycd $WRK/calib
 
#    \ls -1 *_flat_*.fits | grep -v norm > list_flats  
    # for DR6:  process new files only
    rm -rf list_tmp list_flats
    \ls -1 *_flat_*[0-9].fits > list_flats
    \ls -1 *_flat_*[0-9]_norm.fits > list_normd

    nl=$(cat list_flats | wc -l)
    nf=$(cat list_normd | wc -l)    #;   echo $nl $nf #; exit 0

    if [ -e norm_flats.log ] && [ $nf -eq $nl ]; then
        ec "CHECK: found norm_flat.log and normalised flats ... "
        ec "CHECK: ==> flat handling already done ... skip it"
        ec "#-----------------------------------------------------------------------------"
    else
        rcurr=R4
        ec "#-----------------------------------------------------------------------------"
        ec "## - R4: on flats: remove PV from the headers and normalise" 
        ec "#-----------------------------------------------------------------------------"
 
        module=pfixFlats.sh
        qfile=$WRK/$module
        sed -e 's|@WRK@|'$WRK'|' -e 's|@FILTER@|'$FILTER'|' \
            -e 's|@LIST@|list_flats|'  -e 's|@NODE@|'$node'|'  -e 's|@DRY@|F|' \
            $bindir/$module > $qfile      ; chmod 755 $qfile      
        ec "# Built $qfile for $nl flats"

        mycd $WRK    ### back to $WRK to run jobs
        ec "#-----------------------------------------------------------------------------"

        if [ $dry == 'T' ]; then 
            ec "   >> EXITING TEST MODE << "
            exit 3
        fi

        ec "# Submit qsub file ... ";  qsub $qfile
        ec '# Begin wait loop -- wait for $njobs convert jobs to finish  --'
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        while :; do                          # qsub wait loop 
            [ -e pflats.out ] && break       # jobs finished
            sleep 60
        done  
        ec "# fix flats finished, walltime: $(wtime) - check results ..."

        ec "# Flat handing completed: 'PV' removed from headers and normalised"
        ec "#-----------------------------------------------------------------------------"
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi

    #----------------------------------------------------------------------------------------------#
 
    mycd $WRK/images
 
    if [ -e ${fileinfo} ]; then
		nf=$(cat $fileinfo | wc -l)
        ec "CHECK: Found $(echo $fileinfo | rev | cut -d\/ -f 1 | rev) with $nf entries ... continue"
        ec "#-----------------------------------------------------------------------------"
    else
        if [ $dry == 'T' ]; then ec "====> Ready to build FileInfo.dat"; exit 5; fi
 
        ec "# Build fileinfo table ..."
        dfits -x 1 v20*_0????.fits | fitsort -d OBJECT FILTER IMRED_FF IMRED_MK STACK SKYSUB | \
            sed -e 's/Done with //' -e 's/\[1\]/s/' -e 's/_st/_st.fits/' -e 's/\t/  /g' -e 's/   /  /g' > ${fileinfo}
        
        # Check that all support files are present: build lists 
        # rm consecutive spaces in order to be able to use space as separator
        cat ${fileinfo} | tr -s \   > fileinfo.tmp   
        cut -d' ' -f4,4 fileinfo.tmp | sort -u  > list_flats
        cut -d' ' -f5,5 fileinfo.tmp | sort -u  > list_bpms
        cut -d' ' -f6,6 fileinfo.tmp | sort -u  > list_stacks
        cut -d' ' -f7,7 fileinfo.tmp | sort -u  > list_skies
 
        err=0
        for f in $(cat list_flats);  do if [ ! -s ../calib/$f ];   then echo "#### ATTN: $f missing in calib";  echo $f >> list_missing; err=1; fi; done
        for f in $(cat list_skies);  do if [ ! -s ../calib/$f ];   then echo "#### ATTN: $f missing in calib";  echo $f >> list_missing; err=1; fi; done
        for f in $(cat list_bpms);   do if [ ! -s $DR6/bpms/$f ];  then echo "#### ATTN: $f missing in calib";  echo $f >> list_missing; err=1; fi; done
        if [ $err -eq 1 ]; then ec "# missing files ... see images/list_missing"; askuser
        else ec "# All needed flats, bpm, skies, stacks available ... continue";  fi
        rm list_bpms list_flats list_skies list_stacks
 
        ec "#-----------------------------------------------------------------------------"
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
    
    if [ $HOSTNAME != "c03" ] && [ $HOSTNAME != "c02" ]; then  
        ec "#=========================================================#"
        ec "#### ATTN: cannot start jobs from $HOSTNAME. Switched to dry mode  #"
        ec "#=========================================================#"
        dry='T'
    fi

    #----------------------------------------------------------------------------------------------#
    #       pseudo qualityFITS
    #----------------------------------------------------------------------------------------------#

    mycd $WRK/images

    nl=$(ls ldacs/v20*_0????.ldac 2> /dev/null | wc -l)
    if [[ -e $WRK/qFits.DONE ]] && [ $nl -ge 1 ] ; then 
        ec "CHECK: Found qFits.DONE and $nl ldacs ..."
        ec "CHECK: ==> qFits has been run ... skip to next step"
        ec "#-----------------------------------------------------------------------------"
    else
        rcurr=R5  

        if [ -s list_special ]; then 
            list=list_special
            ec "############ ATTN: Using special list ############"
        else 
            list=list_images
        fi
        nims=$(cat $list | wc -l)

        ec "## - R5:  qFits.sh:  pseudo qFits on $list with $(cat $list | wc -l) entries"
        ec "#-----------------------------------------------------------------------------"
        rm -f $WRK/qFits*.* qFits_*.??? chunk_*.lst 
        
        # check needed directories:
        for dir in ldacs qFits xml weights logs regs heads objects; do
            if [ ! -d $WRK/images/$dir ]; then mkdir $WRK/images/$dir; fi
        done

        # use small chunk size for testing
        if [ $nims -ge 520 ]; then 
            n_chunks=25
        else
            n_chunks=5
        fi
        split -n l/$n_chunks --additional-suffix='.lst' $list qFits_ 
        nts=$(ls qFits_??.lst | wc -l)

        for l in qFits_??.lst; do
            id=$(echo $l | cut -d\_ -f2 | cut -d\. -f1)       # ; echo $id
            qfile=$WRK/qFits_${id}.sh ; touch $qfile ; chmod 755 $qfile
            sed -e 's|@LIST@|'$l'|'  -e 's|@ID@|'$id'|'  -e 's|@FILTER@|'$FILTER'|' \
                -e 's|@NODE@|'$node'|'  -e 's|@WRK@|'$WRK'|'  -e 's|@DRY@|'$dry'|'  \
                -e 's|@IDENT@|'$PWD/qFits_$id'|'  $bindir/qFits.sh > $qfile

            ec "# Built $(echo $qfile | cut -d\/ -f6) with $(cat $l | wc -l) entries"
            echo "qsub $qfile; sleep 1" >> $WRK/qFits.submit
        done   

        ec "# ==> written to file 'qFits.submit' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then 
            ec "   >> EXITING TEST MODE << "
            ec "# to clean up ...  rm qFits*.sh qFits.submit images/qFits*.lst";  exit 0
        fi

        # get official param and config files:
        cd $confdir/qFits; rsync -auv -q *.config *param *ret *conv *nnw $WRK/images
        mycd $WRK    # back here to submit jobs

        # actual job submission
        ec "# Submit qsub files ... then wait for them to finish"
        source qFits.submit
        ec " >>>>   Wait for $nts qFits jobs ...  <<<<<"

        btime=$(date "+%s.%N");  sleep 30      # before starting wait loop
        while :; do                            # begin qsub wait loop for qFits
            nn=$(ls qFits_*.out 2> /dev/null | wc -l)   # jobs done
            [ $nn -eq $nts ] && break          # jobs finished
            sleep 30
        done  
        ec "# qFits jobs finished, walltime: $(wtime) - check results ..."

        # remove empty lines from torque logfiles
        for f in qFits_??.out; do strings $f > xx; mv xx $f; done

        # check torque EXIT STATUS
        grep 'EXIT\ STATUS' qFits_??.out | grep -v \ 0 > estats
        nbad=$(cat estats | wc -l)
        if [ $nbad -gt 0 ]; then
            ec "#### PROBLEM: $nbad qFits_xx.sh exit status not 0 ... "
            cat estats
            ec " ... continue with other checks ..."
        else
            ec "CHECK: qFits_xx.sh exit status ok ... continue"
            rm estats
        fi

#        # Look for unbuilt regs files ... don't know origin of problem ...
#        cd images
#        for f in $(cat $list); do 
#            if [ ! -e regs/${f%.fits}.reg ]; then echo $f; fi
#        done > missed.reg
#        nn=$(cat missed.reg | wc -l)
#        if [ $nn -gt 0 ]; then
#            ec "## Try do build $nn missing reg files ... "
#            for f in $(cat missed.reg); do $pydir/ldac2region.py ldacs/${f%.fits}_orig.ldac; done
#        else
#            rm missed.reg
#        fi
#        cd ..

        # ---------------------- Finished qFits run; check products ----------------------
        
        grep -ni -e Segmentation -e ERROR qFits_??.out  > qFits.errs
        grep -ni WARNING qFits_??.out | grep -v -e ATLAS -e FutureWarn > qFits.warns
        nerrs=$(cat qFits.errs 2> /dev/null  | wc -l)
        nwarn=$(cat qFits.warns 2> /dev/null | wc -l)

        if [ $nerrs -ge 1 ]; then 
            ec "#### ATTN: Found $nerrs errors in qFits_xx.out files; see qFits.errs"
        else
            rm qFits.errs
        fi

        if [ $nwarn -ge 1 ]; then 
            ec "#### ATTN: Found $nwarn warnings in qFits_xx.out files; see qFits.warns"
        else
            rm qFits.warns
        fi

        ec "# Check that all expected files are built:"
        mycd $WRK/images   
        rm -f missed.* qFits.missed   # 2>&1 /dev/null
        for f in $(cat $list | cut -d. -f1); do 
            if [ ! -e   origs/${f}_sky-histo.png ]; then echo ${f}_sky-histo.png >> missed.sky-hist; fi
            if [ ! -e   origs/${f}_sky-stats.dat ]; then echo ${f}_sky-stats.dat >> missed.sky-stat; fi
            if [ ! -e weights/${f}_weight.fits   ]; then echo ${f}_weight.fits   >> missed.weight;  fi
            if [ ! -e     xml/${f}_psfex.xml     ]; then echo ${f}_psfex.xml     >> missed.psfxml;  fi
            if [ ! -e     xml/${f}_psfex.dat     ]; then echo ${f}_psfex.dat     >> missed.psfdat;  fi
            if [ ! -e   ldacs/${f}.ldac          ]; then echo ${f}.ldac          >> missed.ldac;    fi
            if [ ! -e   ldacs/${f}_nstars.dat    ]; then echo ${f}_nstars.dat    >> missed.nstars;  fi
            if [ ! -e    regs/${f}.reg           ]; then echo ${f}.reg           >> missed.reg;     fi
            if [ ! -e objects/${f}_saturation-data.dat ]; then echo ${f}_saturation-data.dat >> missed.sat-data;    fi
            if [ ! -e objects/${f}_saturation-hist.png ]; then echo ${f}_saturation-hist.png >> missed.sat-hist;    fi
            if [ ! -e   qFits/${f}_qFits.dat     ]; then echo ${f}_qFits.dat     >> missed.summ;    fi
        done
        nm=$(ls missed.* 2> /dev/null | wc -l)
        if [ $nm -gt 0 ]; then
            for f in missed.*; do 
                if [ ! -s $f ]; then 
                    rm $f
                else
                    cat $f >> qFits.missed 2> /dev/null
                fi
            done
        else
            touch qFits.missed
        fi
        nmis=$(cat qFits.missed | wc -l)
        if [ $nmis -gt 0 ]; then
            ec "#### ATTN: $nmis files missing - see qFits.missed"
            if [ $nmis -le 10 ]; then
                cat qFits.missed
            else
                ec "#### the first and last few missing:"
                head -5 qFits.missed ; echo ; tail -5 qFits.missed
            fi
        else
            ec "# All expected files found"
            rm qFits.missed
        fi

        # Build summary tables
        ec "# Build summary tables for : Nstars, FWHM, ELLI, DESC and PROBs "
        # from input file
        grep ^medi   qFits/v20*_qFits.dat | cut -d\/ -f2 | cut -c1-15,33-299 > MSKY.dat
        grep ^\ std  qFits/v20*_qFits.dat | cut -d\/ -f2 | cut -c1-15,33-299 > SRMS.dat
        grep ^desc   qFits/v20*_qFits.dat | cut -d\/ -f2 | cut -c1-15,33-299 > DESC.dat
        grep ^casu   qFits/v20*_qFits.dat | cut -d\/ -f2 | cut -c1-15,33-299 > CASU.dat
        # from PSFEx xml file
        grep ^fwhm   qFits/v20*_qFits.dat | cut -d\/ -f2 | cut -c1-15,33-299 > FWHM.dat
        grep ^elli   qFits/v20*_qFits.dat | cut -d\/ -f2 | cut -c1-15,33-299 > ELLI.dat
        grep ^Nacc   qFits/v20*_qFits.dat | cut -d\/ -f2 | cut -c1-15,33-299 > Nacc.dat
        # from SExtractor for scamp
        grep ^Nstars qFits/v20*_qFits.dat | cut -d\/ -f2 | cut -c1-15,33-299 > Nstr.dat
        grep ^satlev qFits/v20*_qFits.dat | cut -d\/ -f2 | cut -c1-15,33-299 > SATU.dat
        grep PROBLEM DESC.dat > PROBs.dat

        npbs=$(cat PROBs.dat | wc -l)
        if [ $npbs -gt 0 ]; then
            ec "#### ATTN: found $npbs files with high noise in top chips, see PROBs.dat ####"
        else
            rm -f PROBs.dat
        fi

        # Now clean up .....
        ec "# qFits runs successfull ...  GOOD JOB!! Clean-up and continue"        
        
        # (re)build selected lists
        cd weights; ls v20*_0????_weight.fits > ../list_weights ; cd ..
        cd xml;     ls v20*_0????_psfex.xml   > ../list_xml     ; cd ..
        cd ldacs;   ls v20*_0????_ns.ldac     > ../list_ldacs   ; cd ..
#       cd ldacs;   ls v20*_0????_nstars.dat  > ../list_nstars  ; cd ..
        cd qFits;   ls v20*_0????_qFits.dat   > ../list_dats    ; cd ..

        Ndone=$(cat $WRK/images/list_ldacs | wc -l)  #$(ls $WRK/images/qFits/v20*_qFits.dat | wc -l)
        Nimas=$(cat $WRK/images/list_origs | wc -l)  #ls $WRK/images/origs/v20*_0????.fits | wc -l) # arglist too long
        if [ $Nimas -eq $Ndone ]; then
            echo  "# qFits runs successfull ...  GOOD JOB!! " > $WRK/qFits.DONE
        else
            ec "# ATTN: qFits done for $Ndone files; $(($Nimas - $Ndone)) outstanding"
        fi
    fi

    exit 0    ####   QUIT HERE FOR NOW   ####
    #----------------------------------------------------------------------------------------------#
    #       Get psf stats from psfex
    # In DR6 skip this step: selection is done later at chip level.
    #----------------------------------------------------------------------------------------------#
 
    mycd $WRK/images

    if [ -s Nstars.tab ] && [ -s $WRK/PSFsel.out ]; then
        ec "CHECK: PSF selection already done ..."
        ec "CHECK: ==> skip to next step"
        ec "#-----------------------------------------------------------------------------"
    else
        npsfx=$(ls v20*psfex.xml | wc -l) 
        ec "## - R6:  PSFsel.sh: extract psf stats from $npsfx xml files"
        ec "#-----------------------------------------------------------------------------"
        rm -f $WRK/PSFsel.??? $WRK/PSFsel.sh
        # NB: single file - not working on list (could change)

        ls -1 v20*_0????_psfex.xml > PSFsel.lst
        qfile=$WRK/PSFsel.sh; touch $qfile; chmod 755 $qfile
        sed -e 's|@LIST@|PSFsel.lst|'  -e 's|@WRK@|'$WRK'|'  -e 's|@DRY@|'$dry'|' \
            -e 's|@FILTER@|'$FILTER'|'  $bindir/PSFsel.sh > $qfile

        nl=$(cat PSFsel.lst | wc -l)
        ec "# Built $qfile with $nl entries"
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi

        mycd $WRK    #  Back here for submission
        ec "# submitting $qfile ...  "; qsub $qfile
        ec " >>>>   Wait for PSFsel job to finish ...   <<<<<" 
        btime=$(date "+%s.%N"); sleep 30   # before starting wait loop
        while :; do           #  begin qsub wait loop for PSFsel
            [ -s PSFsel.out ] && break          # jobs finished

            ndone=$(cat PSFsel.dat 2> /dev/null | wc -l)
            ntodo=$(($nl - $ndone)) ;  nsec=$(($ntodo/10))
            if [ $nsec -le 60 ];   then nsec=60; fi            # min: 1 min
            if [ $nsec -ge 1800 ]; then nsec=1800; fi          # max: 0.5 hr
            nmin=$(echo "scale=1; $nsec/60" | bc); nhrs=$(echo "scale=1; $nmin/60" | bc)
            if [ $nsec -gt 5400 ]; then wmsg="$nhrs hr"; else wmsg="$nmin min"; fi
            ec "# $ndone PSFsels done, $ntodo remaining - next check in $wmsg "
            sleep $nsec
        done  
        ec "# PSFsel job finished; wallime $(wtime). "
        grep -v votable PSFsel.out > x ; mv x PSFsel.out

        ngood=$(tail -1 PSFsel.out | grep STATUS:\ 0 | wc -l) 
        if [ $ngood -eq 0 ]; then
            ec "#### PROBLEM: PSFsel.sh exit status not 0 ... check PSFsel.out"
            askuser
        fi
        ec "CHECK: PSFsel.sh exit status ok ... continue"

        nerr=$(grep Error PSFsel.out | wc -l)
        if [ $nerr -gt 0 ]; then
            ec "#### PROBLEM: found $nerr errors in PSFsel.out "
            askuser
        fi

        ndone=$(grep ^v20 $WRK/images/PSFsel.dat | wc -l)
        if [ $ndone -ne $nl ]; then
            ec "#### PROBLEM: found only $ndone lines of $nl in PSFsel.dat "
            askuser
        fi

        mycd images   
        nbad=$(grep v20 badPSF.dat 2> /dev/null | wc -l)
        ec "# PSF selection done, exit status ok; no errors found; "
        ec "# $nbad files with bad PSF found and removed ... "

        best=$(grep v20 PSFsel.dat | sort -k2,3 | head -1)
#####        ec "# Select highest quality image: $best"
        ### TBD: write ??? kwd to indicate ref photom image for scamp

        ls -1 v20*_0????.ldac > list_ldacs  ; nldacs=$(cat list_ldacs | wc -l)
        ls -1 v20*_0????.fits > list_images ; nimages=$(cat list_images | wc -l)
        ec "# list_images and list_ldacs rebuild with $nimages file" 

        ec  "# PSF selection successfull ...  GOOD JOB!! " ; chmod 444 $WRK/PSFsel.out

        ec "#-----------------------------------------------------------------------------"
        if [ $int == 'T' ]; then ec "# >>> Interactive mode:" ; askuser; fi

        # ---------------------- create subdirs for products
        
        
        mycd $WRK; rm qFits.submit
        if [ ! -d qFits ]; then mkdir qFits ; fi
        mv images/v20*_psfex.xml qFits   # DELETE?? not sure worth keeping further
        mv qFits_??.sh qFits_??.???   qFits   
    fi  
#    echo "### QUIT HERE FOR NOW ###" ; exit 0    

    if [ $pauto == 'T' ]; then                                 # END P1
        ec "#-----------------------------------------------------------------------------"
        ec "# $pcurr finished; ==> auto continue to $pnext"
        ec "#-----------------------------------------------------------------------------"
        $0 $pnext $pauto
    else 
        ec "#-----------------------------------------------------------------------------"
        ec "#                                End of $pcurr "
        ec "#-----------------------------------------------------------------------------"
    fi
#    echo "### QUIT HERE FOR NOW ###" ; exit 0       
#-----------------------------------------------------------------------------------------------
elif [ $1 = 'p2' ]; then      # P2: scamp, swarp, build stack and its mask, build obj masks
#-----------------------------------------------------------------------------------------------

#% P2: scamp, swarp, merge to build p1 stack, its mask, etc.
#% - run scamp with gaia catal to build head files; rm ldac files
#% - run swarp to build substacks - by paw for now, at firstpass resol'n
#% - merge the substacks into the pass1 stack
#% - and build its mask and associated products
#%------------------------------------------------------------------

    mycd $WRK/images
    nn=$(ls UVIS*p1.fits UVIS*p1_weight.fits UVIS*p1_ob_flag.fits 2> /dev/null | wc -l )
    if [ $nn -eq 3 ]; then
        ec "# Found $nn UVIS*.fits files - looks like P2 has been done already ... quitting"
        exit 0
    fi

    ec "#-----------------------------------------------------------------------------"
    ec "## P2: scamp, swarp, and first-pass stack: check available data ..."
    ec "#-----------------------------------------------------------------------------"
    
    ncurr=2 ; pcurr="p$ncurr" ; pprev=$(($ncurr-1)) ; pnext="p$(($ncurr+1))"
    
    if [ ! -s list_images ]; then 
        ec "# WARNING: 'list_images not found ... build it "
        cd origs; ls -1 v20*_0????.fits > ../list_images; cd -
    fi

    if [ ! -s list_ldacs  ]; then 
        ec "# WARNING: list_ldacs not found ... build it"
        cd ldacs; ls -1 v20*.ldac > ../list_ldacs; cd -
    fi

    if [ ! -s list_weights  ]; then 
        ec "# WARNING: list_weights not found ... build it"
        cd weights; ls -1 v20*_weight.fits > ../list_weights; cd -
    fi

    if [ ! -e list_heads  ]; then 
        ec "# WARNING: list_heads not found ... build it"
        cd heads; ls -1 v20*.head > ../list_heads; cd -
    fi

    nldacs=$(cat list_ldacs   | wc -l)
    nimages=$(cat list_images | wc -l)
    nwghts=$(cat list_weights | wc -l)
    nheads=$(cat list_heads   | wc -l)
    
    if [ $nimages -eq $nldacs ]; then 
        ec "CHECK: found $nimages images, $nwghts weights, $nldacs ldacs, $nheads head files ... " 
        ec "CHECK: ... seems ok to continue with first pass."
    else
        ec "#### PROBLEM: Number of images, ldacs, weights not the same ..."
        echo "  $nimages,  $nldacs  $nwghts"
        askuser
    fi  
    
    ec "##----------------------------------------------------------------------------"
    ec "#"
    ec "##          ======  BEGIN FIRST PASS  ======"
    ec "#"
    ec "##----------------------------------------------------------------------------"

    # Some product names
    stout=UVISTA_${FILTER}_p1              # name of pass1 stack w/o .fits extension (low res)
    stout_flag=${stout%.fits}_obFlag.fits  # and the object flag

    #----------------------------------------------------------------------------------------#
    #       scamp
    #----------------------------------------------------------------------------------------#
    # check whether scamp has already been run ... 

    nn=$(ls -1 $WRK/images/scamp/pscamp_p??.out 2> /dev/null | wc -l) #  ; echo $nn

    if [ $nn -eq 3 ] && [ $nheads -eq $nimages ]; then
        ec "CHECK: scamp logfile already exists and found $nheads head files ..." 
        ec "CHECK:  ==> scamp already done skip to R8/ swarp "
        ec "#-----------------------------------------------------------------------------"
    else
        rcurr=R7
        nl=$(cat list_paw? | wc -l)  # total num of files to process
        if [ $nl -lt 2000 ]; then 
            wtime=48    # useful in testing
        else 
            wtime=250
        fi
        nsec=30  # wait loop check interval

        # split by paws
        if [ -e list_paw0 ]; then   # N-band: paws 0-3 only
            cat list_paw1 list_paw0 | sed 's/fits/ldac/' > list_paw14
            sed 's/fits/ldac/' list_paw2 > list_paw25
            sed 's/fits/ldac/' list_paw3 > list_paw36
        else
            cat list_paw1 list_paw4 | sed 's/fits/ldac/' > list_paw14
            cat list_paw2 list_paw5 | sed 's/fits/ldac/' > list_paw25
            cat list_paw3 list_paw6 | sed 's/fits/ldac/' > list_paw36
        fi
        
        nlists=$(ls list_paw?? | wc -l)
        ec "## - R7: pscamp.sh: scamp, pass-1, split by paws "
        ec "#-----------------------------------------------------------------------------"

        rm -rf $WRK/pscamp.submit  
        for plist in list_paw??; do
            ptag=_p$( echo $plist | cut -c9-10)  # tag to build output file names

            rm -f $WRK/pscamp$ptag.out $WRK/pscamp$ptag.log $WRK/pscamp$ptag.sh   
            nn=$(cat $plist | wc -l)

            qfile=$WRK/"pscamp$ptag.sh"; touch $qfile; chmod 755 $qfile
            sed -e 's|@NODE@|'$node'|'     -e 's|@IDENT@|'$PWD/pscamp$ptag'|'  -e 's|@DRY@|'$dry'|'  \
                -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'$plist'|'    -e 's|@WRK@|'$WRK'|'  \
                -e 's|@WTIME@|'$wtime'|'   -e 's|@PTAG@|'$ptag'|'  $bindir/pscamp.sh > $qfile
        
            if [ $nn -lt 100 ]; then    # short (test) run - decrease resources
                sed -i -e 's|ppn=22|ppn=8|' -e 's|time=48|time=06|' $qfile
            fi

            ec "# Built $qfile for $plist with $nn entries"
            echo  "qsub $qfile ; sleep 1" >> $WRK/pscamp.submit
        done

        ec "# ==> Built \$WRK/pscamp.submit with $(cat $WRK/pscamp.submit | wc -l) entries"
        ec "#-----------------------------------------------------------------------------"

        if [ $dry == 'T' ]; then 
            ec "#   >> BEGIN dry-run of $(echo $qfile | cut -d\/ -f6):  << "
            $qfile dry
            ec "#   >> FINISHED dry-run of $0 finished .... << "
            exit 0
        fi

        #-----------------------------------------------------------------------------
        ec "# Now for real work ...."
        #-----------------------------------------------------------------------------

        ec "# - Clean up: rm flxscale.dat v20*.head"
        rm -rf fluxscale.dat v20*.head

        ec "# - Bild links to needed files:"
        if [ ! -e scamp_dr5.conf ]; then ln -sf $confdir/scamp_dr5.conf . ; fi
        if [ ! -e vista_gaia.ahead ]; then ln -sf $confdir/vista_gaia.ahead . ; fi
        if [ ! -e GAIA-EDR3_1000+0211_r61.cat ]; then ln -sf $confdir/GAIA-EDR3_1000+0211_r61.cat . ; fi

        #-----------------------------------------------------------------------------
        mycd $WRK   # to submit jobs and wait for them to finish
        #-----------------------------------------------------------------------------

        ec "# - Submitting pscamp_p?? jobs ..."
        source pscamp.submit
        ec " >>>>   wait for pscamp to finish ...   <<<<<"
        
        btime=$(date "+%s"); sleep 20   # before starting wait loop
        while :; do              #  begin qsub wait loop for pscamp
            ndone=$(ls $WRK/images/pscamp_p??.out 2> /dev/null | wc -l)
            [ $ndone -eq 3 ] && break               # jobs finished
            sleep $nsec
        done  
        mv images/pscamp_p??.out .
        chmod 644 pscamp_p??.out

        ec "# $njobs pscamp_p?? jobs finished, walltime $(wtime) - now check exit status"
        ngood=$(grep STATUS:\ 0 pscamp_p??.out | wc -l)
        if [ $ngood -ne 3 ]; then
            ec "#### PROBLEM: pscamp.sh exit status not 0 ... check pscamp.out"
            askuser
        fi

        #-----------------------------------------------------------------------------
        mycd $WRK/images    # to run other checks
        #-----------------------------------------------------------------------------
        # check number of .head files produced
        nheads=$(ls -1 v20*.head | wc -l)
        if [ $nheads -lt $nl ]; then
            ec "#### PROBLEM: built only $nheads head files for $nl ldacs ... "
            askuser
        fi

        # check warnings 
        nwarn=$(cat $WRK/pscamp_p??.warn 2> /dev/null | wc -l)
        if [ $nwarn -ge 1 ]; then 
            ec "# WARNING: $nwarn warnings found in logfile for $nl files"
        fi   

        # check fluxscale table built by pscamp script
        ec "#-----------------------------------------------------------------------------"
        ec "#       Scamp flux-scale results "
        ec "#-----------------------------------------------------------------------------"
        mycd $WRK/images
        fsfile=fluxscale.dat    
        grep FLXSCALE v20*.head | cut -d\/ -f1 | sed 's/.head:FLXSCALE=//' | sort -k1 -u | \
            awk '{printf "%-16s %10.6f %8.4f \n", $1, $2, 2.5*log($2)/log(10) }' > $fsfile

        nfs=$(cat $fsfile | wc -l)   
        if [ $nfs -eq 0 ]; then
            ec "#### ATTN: $fsfile empty!!  FLXSCALE kwd not written by scamp??"
        else
            nun=$(sort -u -k2 $fsfile | wc -l)    # number of unique values
            if [ $nun -le $((2*$nfs/3)) ]; then
                ec "#### ATTN: $fsfile has $nun unique values of about $(($nimages * 16)) expected"
            fi
            
            nbad=$(\grep 0.0000000 $fsfile |  wc -l)
            if [ $nbad != 0 ]; then echo "#### ATTN: found $nbad chips with FLUXSCALE = 0.00"; fi
            nbad=$(\grep INF $fsfile |  wc -l)
            if [ $nbad != 0 ]; then echo "#### ATTN: found $nbad chips with FLUXSCALE = INF"; fi
            res=$(grep -v -e INF -e 0.00000000 $fsfile | tr -s ' ' | cut -d' ' -f2 | awk -f $uvis/scripts/std.awk )
            ec "# mean flux scale: $res"
        fi

        ec "#-----------------------------------------------------------------------------"
        ec "CHECK: pscamp.sh successful, $nheads head files built ... clean-up and continue"
        if [ ! -d heads ] ; then mkdir heads ; else rm -f heads/* ; fi
        mv v20*.head heads   #####; ln -s scamp/v*.head .
        if [ ! -d scamp ] ; then mkdir scamp ; else rm -f scamp/* ; fi
        mv fgroups*.png ???err*.png pscamp* fluxscale.dat list_paw?? scamp
        mv ../pscamp_p??.* scamp 
        rm -f v20*.ldac    # normally not there

        ec "#-----------------------------------------------------------------------------"
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
    

    #----------------------------------------------------------------------------------------#
    #       swarp - p1
    #----------------------------------------------------------------------------------------#
    #  
    # 
    # 
    #----------------------------------------------------------------------------------------#

    # check if swarp alreay done:
    nsubima=$(ls -1 substack_paw?_??.fits 2> /dev/null | wc -l) # ; echo $nsubima ; echo $npaws
    if [ $nsubima -ge 2 ]; then 
        ec "CHECK: Found $nsubima substacks - swarp done "
        ec "CHECK: ==> skip to next step "
        ec "#-----------------------------------------------------------------------------"
    else 
        rcurr=R9
        ec "## - R9:  pswarp.sh: swarp pass1 ... "
        ec "#-----------------------------------------------------------------------------"
        
        # check if paw lists exist
        if [ $(ls list_paw? 2> /dev/null | wc -l) -eq 0 ]; then
            ec "# No paw lists found ... build them "
            $0 plists  
        fi
        npaws=$(ls list_paw? 2> /dev/null | wc -l)

        # build links to files (external)
        rm -f $WRK/pswarp1.submit $WRK/estats$WRK/pswarp1_paw?_??.sh  pswarp1_paw?_??.???     # just in case
#        headfile=firstpass.head
        headfile=std1G.head
        subsky=Y                             # for pass1 DO subtract sky

        ec "#-------------------------------------------------------#"
        ec "#### ATTN: head-file: $headfile"
        ec "#### ATTN: subsky:    $subsky"
        ec "#-------------------------------------------------------#"

        nim=450  # approx num of images in each sublist
        for list in list_paw[0-9]; do  
            nl=$(cat $list | wc -l)   

            ppaw=$(echo $list | cut -d\_ -f2)       # NEW tmporary name for full paw
            split -n l/$(($nl/$nim+1)) $list --additional-suffix='.lst' pswarp1_${ppaw}_
            for slist in pswarp1_${ppaw}_??.lst; do
                nl=$(cat $slist | wc -l)    
                paw=$(echo $slist | cut -d\_ -f2-3 | cut -d\. -f1)   
                outname=substack_${paw}
                #ec "DEBUG:  For paw $paw, $nl images ==> $outname with subsky $subsky"
            
                # ---------------------- Local run by sublist ----------------------
                
                qfile=$WRK/"pswarp1_$paw.sh"; touch $qfile; chmod 755 $qfile
                sed -e 's|@NODE@|'$node'|'  -e 's|@IDENT@|'$PWD/pswarp1'|'  -e 's|@DRY@|0|'  \
                    -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'$slist'|'  -e 's|@WRK@|'$WRK'|' \
                    -e 's|@PAW@|'$paw'|'  -e 's|@HEADFILE@|'$headfile'|'                     \
                    -e 's/@SUBSKY@/'$subsky'/'  $bindir/pswarp.sh > $qfile
            
                ec "# Built $qfile with $nl images for paw $paw ==> $outname"
                echo "qsub $qfile ; sleep 1" >> $WRK/pswarp1.submit
            done
        done 
        nq=$(cat $WRK/pswarp1.submit | wc -l)
        ec "# ==> written to file \$WRK/pswarp.submit with $nq entries "     
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi

        ec "# Submit qsub files ... (see pswarp1.submit.log)"
        source $WRK/pswarp1.submit > pswarp1.submit.log
        ec " >>>>   Wait for $nq pswarp jobs ... first check in 1 min  <<<<<"

        btime=$(date "+%s.%N");  sleep 60 
        while :; do           #  begin qsub wait loop for pswarp
            njobs=$(ls $WRK/images/pswarp1_paw?_??.out 2> /dev/null | wc -l)
            [ $njobs -eq $nq ] && break          # jobs finished
            sleep 30
        done  
        ec "# pswarp finished; walltime $(wtime)"
        chmod 644 pswarp1_paw?_??.out 
        
        grep EXIT\ STATUS pswarp1_paw?_??.out  >> estats
        nbad=$(grep -v STATUS:\ 0  estats | wc -l)  # files w/ status != 0
        if [ $nbad -gt 0 ]; then
            ec "#### PROBLEM: pswarp1_pawx_xx.sh exit status not 0 "
            grep -v STATUS:\ 0 estats 
            askuser
        fi
        ec "# CHECK: pswarp1_pawx_xx.sh exit status ok"; rm estats

        # check num sustacks found
        nn=$(ls substack*paw?_??.fits | wc -l)
        if [ $nn -lt $nq ]; then
            ec "#### PROBLEM:  found only $nn substacks for $nq expected ..."
            askuser
        fi

        # check sizes of substacks
        ns=$(\ls -l substack_paw?_??.fits | \
            tr -s ' ' | cut -d ' ' -f5,5 | sort -u | wc -l)
        if [ $ns -gt 1 ]; then 
            ec "#### PROBLEM: substacks not all of same size .... "
            ls -l substack_paw?_??.fits
            askuser
        fi

        # check for WARNINGS in logfiles
        warn=0
        for f in pswarp1_paw?_??.log; do
            grep WARNING $f | wc -l > ${f%.log}.warn
            if [ $(wc ${f%.log}.warn | wc -l) -gt $nq ]; then warn=1; fi
        done
        if [ $warn -eq 1 ]; then 
            ec "#### ATTN: found warnings in pswarp logfiles"
            askuser
        fi

        if [ ! -d swarp_p1 ]; then mkdir swarp_p1; fi
        mv pswarp1_*_??.??? ../pswarp1*.sh pswarp1*.warn pswarp1.submit.log  swarp_p1
        mv substack*clip.log substack*.xml  swarp_p1
        rm substack*_??.head

        ec "#-----------------------------------------------------------------------------"
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi


    #----------------------------------------------------------------------------------------#
    #          merge p1 substacks
    #----------------------------------------------------------------------------------------#
    
    if [ -e $stout ]; then 
        ec "#CHECK: stack $stout already built; "
        ec "#       ==> continue with building its mask and flag"
    else 
        rcurr=R10
        ec "## - R10:  pmerge.sh: Merge p1 substacks into $stout..."
        ec "#-----------------------------------------------------------------------------"
        rm -f pmerge_p1.??? pmerge.sh
        ls -1 substack_paw?_??.fits > pmerge.lst
        nsubstacks=$(cat pmerge.lst | wc -l)
        if [ $nsubstacks -eq 0 ]; then
            "ERROR: no substacks found - quitting"; exit 2
        fi

        qfile=$WRK/"pmerge_p1.sh"; touch $qfile; chmod 755 $qfile
        sed -e 's|@IDENT@|'$WRK/pmerge'|'  -e 's|@DRY@|'$dry'|'  -e 's|@PASS@|'1'|'  \
            -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'pmerge.lst'|'  -e 's|@WRK@|'$WRK'|'  \
            -e 's|@STOUT@|'$stout'|'    $bindir/pmerge.sh > $qfile
        
        ec "# Built $qfile with $nsubstacks entries"
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec "# submitting $qfile ... "; qsub $qfile
        ec " >>>>   wait for pmerge to finish ...   <<<<<"
        
        btime=$(date "+%s.%N");  sleep 20   # before starting wait loop
        while :; do                  #  begin qsub wait loop for pmerge
            ndone=$(ls $WRK/pmerge.out 2> /dev/null | wc -l) 
            #;  ec " DEBUG: njobs: $njobs"
            [ -e $WRK/pmerge.out ] && break                   # jobs finished
            sleep 30
        done  
        ec "# pmerge finished - now check exit status"
        chmod 644 $WRK/pmerge.out
        mv pmerge.out pmerge_p1.out
        
        ngood=$(tail -1 $WRK/pmerge_p1.out | grep STATUS:\ 0 | wc -l)
        if [ $ngood -ne 1 ]; then
            ec "#### PROBLEM: pmerge_p1.sh exit status not 0 ... check pmerge_p1.out"
            askuser
        fi
          
        ec "# CHECK: pmerge_p1.sh exit status ok ... continue"
        ec "# $stout and associated products built:"
        ls -lrth UVISTA*p1*.*
        ec "# ..... GOOD JOB! "
        mv substack_paw?_??.fits substack_paw?_??_weight.fits swarp_p1
    fi

    if [ $pauto == 'T' ]; then                                 # END P2
        ec "#-----------------------------------------------------------------------------"      
        ec "# $pcurr finished ... good job!!! ==> auto continue to $pnext"
        ec "#-----------------------------------------------------------------------------"      
        $0 $pnext $pauto
    else 
        ec "#-----------------------------------------------------------------------------"
        ec "#                                End of $pcurr "
        ec "#-----------------------------------------------------------------------------"
    fi

#-----------------------------------------------------------------------------------------------
elif [ $1 = 'p3' ]; then      # P3: build object masks, add CASU sky, compute best sky 
#-----------------------------------------------------------------------------------------------

    ncurr=3 ; pcurr="p$ncurr" ; pprev=$(($ncurr-1)) ; pnext="p$(($ncurr+1))"
    mycd $WRK/images

    # for DR6 input list is list_accepted .. nominally a link

    if [ -e list_special ]; then
        list=list_special;  ec "#### ATTN: Using special list ####"
		useSpecial=True
    else
		cat list_accepted > list_images
        list=list_images
		useSpecial=False
    fi
    nimages=$(cat $list | wc -l)
    nacc=$(cat list_accepted | wc -l)

    # global object flag for DR6 is computed from DR5 products outside of pipeline
    # NB: this is defined in mkMasks.sh; used here as check only

#    obFlag=DR5_${FILTER}_obFlag.fits
    obFlag=RefObMask.fits
    if [ ! -e $obFlag ]; then
        ec "# Global objects flag $obFlag not found ... quitting" 
        exit 5
    else
        ec "# Global objects flag is:"
		ecn "#   "
		ls -l $obFlag | tr -s \  | cut -d\  -f9-11
    fi

    ec "# ==> Looks like it's ok to continue ... " 
    ec "#-----------------------------------------------------------------------------"

    # ----------------------  Finished checking  ----------------------

    # build zeroes files: last valid bpm file
    lbpm=$(\ls -t $bpmdir/bpm*.fits | head -1) # last bpm to use as zeroes.file
    rm -f zeroes.fits # should there be one already
    ln -s $lbpm zeroes.fits

    #----------------------------------------------------------------------------------------------#
    #       Build masks for sky subtraction
    #----------------------------------------------------------------------------------------------#
    # 
    # 
    # 
    # 
    #----------------------------------------------------------------------------------------------#

    nmsk=$(ls -1 Masks/v20*_mask.fits 2> /dev/null | wc -l )  #; echo $nmsk $nimages

    if [ -e mkMasks.log ] && [ $nmsk -ge $nimages  ] ; then #&& [ ! -e list_special ]; then
        ec "CHECK: found mkMasks logfile and $nmsk _mask files ... "
        ec "CHECK: ===> skip to next step"
        ec "#-----------------------------------------------------------------------------"
    else     
        rcurr=R11

###        if [ -e list_special ]; then
###            list=list_special;  ec "#### ATTN: Using special list ####"
###			useSpecial=True
###        else
###            list=list_images
###			useSpecial=False
###        fi
###        nimages=$(cat $list | wc -l)

        ec "## - R11: mkMasks.sh: build sky-subtraction masks for $nimages images "
        ec "#-----------------------------------------------------------------------------"

        rm -f mkMasks.submit estats mkMasks_??.??? mkMasks_??.sh  

        # ATTN: exec time ~2.5min/file; ==> should not go over nexp=500 to maintain
        #       some margin, given the mkMask walltime of 24 hrs
        # ==> rate is ~24 jobs/hr, depending on machine
		if [ $useSpecial == "True" ]; then nexp=100; else nexp=450; fi

        nts=$(( $nimages/$nexp +1 ))   # num of jobs

        split -n l/$nts $list --additional-suffix='.lst' mkMasks_
        ec "# split into $nts chunks of about $nexp images"

        for l in mkMasks_??.lst; do
            id=$(echo $l | cut -d\_ -f2 | cut -d\. -f1) 
            qfile=mkMasks_${id}.sh  ; touch $qfile; chmod 755 $qfile
            sed -e 's|@NODE@|'$node'|'  -e 's|@LIST@|'$l'|'  -e 's|@DRY@|0|' \
                -e 's|@IDENT@|'$PWD/mkMasks_$id'|'   -e 's|@FILTER@|'$FILTER'|' \
                -e 's|@REFMASK@|'$obFlag'|'  -e 's|@ID@|'$id'|'  \
 				-e 's|@WRK@|'$WRK'|'  $bindir/mkMasks.sh > $qfile
            
            ec "# Built $qfile with $(cat $l | wc -l) entries"
            echo "qsub ${qfile}; sleep 1 " >> mkMasks.submit
        done  
        ec "# ==> written to file 'mkMasks.submit' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi

        ec "# Submit qsub files ... ";  source mkMasks.submit
        ec " >>>>   wait for $nts mkMasks jobs ... first check in 1 min  <<<<<"
        
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        while :; do             # qsub wait loop 
            ndone=$(ls $WRK/images/mkMasks_??.out 2> /dev/null | wc -l)
            [ $ndone -eq $nts ] && break          # jobs finished

            fdone=$(ls -1 /*/mkMasks_??_${FILTER}/v20*_0????_mask.fits v20*_0????_mask.fits 2> /dev/null | wc -l)
            running=$(qstat -au moneti | grep Masks_${FILTER}_ | grep \ R\  | wc -l)
			ftodo=$(($nimages - $fdone))
			if [ $running -gt 0 ]; then
				nsec=$(( 30*${ftodo}/${running} ))
			else
				ec "### ATTN: no jobs running - all queued? ... PROBLEM???"
				nsec=3600
			fi

            if [ $nsec -le 60 ];   then nsec=60; fi            # min: 1 min
            if [ $nsec -gt 3600 ]; then nsec=3600; fi          # max: 1 hr
            nmin=$(echo "scale=1; $nsec/60" | bc); nhrs=$(echo "scale=1; $nmin/60" | bc)
            if [ $nsec -gt 5400 ]; then wmsg="$nhrs hr"; else wmsg="$nmin min"; fi
            echo "$(date "+[%d.%h %T"]) $running jobs running; $fdone masks done, $ftodo remaining - next check in $wmsg "
            sleep $nsec
        done  
        ec "# mkMasks finished; walltime $(wtime) - check results ..."
        
        grep EXIT\ STATUS mkMasks_??.out >> estats
        nbad=$(grep -v \ 0 estats 2> /dev/null | wc -l)
        if [ $nbad -gt 0 ]; then
            ec "#### PROBLEM: some mkMasks's exit status not 0 ... "
            grep -v \ 0  estats
            grep -i error mkMasks_??.out
            askuser
        else
            ec "CHECK: mkMask.sh exit status ok ... continue"; rm -f estats 
        fi
        # check number of files produced:
        nnew=$(ls -1 v20*_mask.fits | wc -l)
        if [ $nnew -lt $nimages ]; then
            ec "CHECK: found only $nnew mask files; $(($nimages-$nnew)) missing...."
            askuser
        else
            ec "CHECK: found all $nnew expected mask files "
        fi
        
        # check error files: in mkMasks_??.dat
        rm -f mkMasks.dat                # clean up before building it
        cat mkMasks_??.dat > mkMasks.dat
        nn=$(grep '\ 0\.00\ ' mkMasks.dat | wc -l)
        if [ $nn -gt 0 ]; then
            ec "#### PROBLEM: $nn files with one or more chips fully masked: check mkMasks_??.dat"
            askuser
        fi
        
        # build general logfile
        grep Building\ mask mkMasks_??.log > mkMasks.log

        # mv new _mask files and scripts to Masks dir for safekeeping
        # ... needed for updateWeights later
        if [ ! -d Masks ]; then mkdir Masks; fi
        mv v20*_mask.fits mkMasks_*.*  Masks 
        chmod 444 Masks/v20*_mask.fits
		cd Masks; \ls v20*_mask.fits > ../list_masks; cd ..
		nm=$(cat list_masks | wc -l)

        ec "# mkMasks checks complete; no problem found; now have $nm masks in Masks/ dir"
        ec "#-----------------------------------------------------------------------------"

		if [ -e list_special ]; then
			ec "# ATTN: removing list_special and continuing with full list"
			dd=$(date "+%y%m%d")
		    cp list_special list_special_mkMask_$dd
			ec "########   QUIT HERE after mkMasks with list_special   ########" 
		fi
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi

    #----------------------------------------------------------------------------------------------#
    #       Add casu sky
    #----------------------------------------------------------------------------------------------#
    # Done locally; links to origs files are created, sky is added, results is _withSky
    # Links to the sky and bpm images are made locally and deleted when done
    # 
    # When done the _withSky files are moved to withSky dir, together with scripts, logs, etc.
    # and links to origs are deleted.
    #----------------------------------------------------------------------------------------------#

    list=list_images
    nwsky=$(ls -1 withSky/v20*withSky.fits 2> /dev/null | wc -l )  
    if [ -e addSky.log ]; then      # && [ $nwsky -ge $nimages  ]; then
      ec "CHECK: found addSky.log ... "
      ec "CHECK: ===> skip to next step"
      ec "#-----------------------------------------------------------------------------"
    else     
        rcurr=R12

        if [ -e list_special ]; then
            list=list_special;  ec "#### ATTN: Using special list ####"
        else
            list=list_images
        fi
        nimages=$(cat $list | wc -l)

        ec "## - R12: addSky.sh: add CASU sky to $nimages images in $list "
        ec "#-----------------------------------------------------------------------------"
        rm -f addSky.submit addSky_??.??? addSky_??.sh   

        nn=$(ls v20*_?????.fits 2> /dev/null | wc -l)  
        if [ $nn -ne $nimages ]; then
            ec "==> Found only $nn image files of $nimages expected ..."
            ec "==> Build links to image files ... "
			if  [ -e list_special ]; then
				for f in $(cat $list); do ln -sf origs/$f . ; done
			else
				ln -sf origs/v20*_?????.fits .
			fi
            nn=$(ls v20*_?????.fits 2> /dev/null | wc -l)  
            ec "    ... Done: built $nn links "
        fi

        # Build links to sky and bpm files
        rm -f sky_*.fits bpm_*.fits
        ec "# Build links to sky files."; ln -s $WRK/calib/sky_*.fits . 
        ec "# Build links to bpm files."; ln -s $DR6/bpms/bpm_*.fits .
       
        # split the list into chunks of max 500 images, normally doable in 32 hrs:
        if [ $nimages -lt 499 ]; then nts=7; else nts=$(($nimages/500 + 1)); fi
		ec "# Split input list into $nts chunks of max 500 files"
        split -n l/$nts $list --additional-suffix='.lst'
        ec "#-----------------------------------------------------------------------------"

        for l in x??.lst; do
			id=${l:1:2} 
		    # here we extract the respective lines from FileInfo.dat in order to
     		# have the sky file needed
			rr=$(for f in $(cat $l); do echo -n " -e $f" ; done) 
			grep $rr ../FileInfo.dat > addSky_${id}.lst

            qfile=addSky_${id}.sh  ; touch $qfile; chmod 755 $qfile
            sed -e 's|@NODE@|'$node'|'  -e 's|@LIST@|'addSky_${id}.lst'|'  -e 's|@DRY@|'$dry'|' \
                -e 's|@IDENT@|'$PWD/addSky_$id'|'   -e 's|@FILTER@|'$FILTER'|' \
                -e 's|@ID@|'$id'|'  -e 's|@WRK@|'$WRK'|'  $bindir/addSky.sh > $qfile
            
            ec "# Built $qfile with $(cat $l | wc -l) entries"
            echo "qsub $qfile; sleep 1" >> addSky.submit
			rm $l
        done   
        ec "# ==> written to file 'addSky.submit' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec "# Submit qsub files ... ";  source addSky.submit
        ec " >>>>   wait for $nts addSky jobs ... first check in 1 min  <<<<<"
        
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        while :; do             # qsub wait loop for addSky
            ndone=$(ls $WRK/images/addSky_??.out 2> /dev/null | wc -l)
            [ $ndone -eq $nts ] && break          # jobs finished

            running=$(qstat -au moneti | grep addSky_${FILTER} | grep \ R\  | wc -l)
            fdone=$(ls -1 v20*_0????_withSky.fits | wc -l)
			ftodo=$(($nimages - $fdone))	# exec time is  ~2 sec/file, depending on machine
			if [ $running -gt 0 ]; then
				nsec=$(( 2*${ftodo}/${running} ))
			else
				ec "### ATTN: no jobs running - all queued? ... PROBLEM???"
				nsec=3600
			fi
            if [ $nsec -le 60 ];   then nsec=60; fi            # min: 1 min
            if [ $nsec -gt 3600 ]; then nsec=3600; fi          # max: 1 hr
            nmin=$(echo "scale=1; $nsec/60" | bc); nhrs=$(echo "scale=1; $nmin/60" | bc)
            if [ $nsec -gt 5400 ]; then wmsg="$nhrs hr"; else wmsg="$nmin min"; fi
            echo "$(date "+[%d.%h %T"]) $running jobs running; $fdone skies added, $ftodo remaining - next check in $wmsg  " 
            nn=$(\ls -lh v20*withSky.fits | grep -v 257M | wc -l)  
            if [ $nn -gt 0 ]; then 
                ec "# WARNING: Found $nn _withSky files probably incomplete ... continuing nevertheless"
            fi
            sleep $nsec
        done  
        ec "# addSky finished; walltime $(wtime) - check results ..."
        
        rm -f estats         # before filling it
        for l in addSky_??.out; do tail -1 $l >> estats; done
        nbad=$(grep -v STATUS:\ 0  estats 2> /dev/null | wc -l)
        if [ $nbad -gt 0 ]; then
            ec "#### PROBLEM: some addSky's exit status not 0 ... "
            grep -v \ 0  estats;    askuser
        else
            ec "CHECK: addSky.sh exit status ok ... continue"; rm -f estats
        fi

        ls -lh v20*_withSky.fits > tmplist

        # quick check of file completeness
        nn=$(grep -v 257M tmplist | wc -l )
        if [ $nn -ne 0 ]; then
            ec "CHECK: found $nn incomplete files (size != 257MB): "
            grep -v 257M tmplist 
            askuser
        fi
        
        # check number of files produced:
        nnew=$(cat tmplist | wc -l)
        if [ $nnew -lt $nimages ]; then
            ec "CHECK: found only $nnew withSky files of $nimages expected...."
            askuser
        else
            rm tmplist
        fi
        
        # build general log file:
        grep on\ v20 addSky_??.out | cut -d\: -f2 > addSky.log
        chmod 644 v20*withSky.fits addSky*.???

        # clean up:
        if [ ! -d withSky ]; then mkdir withSky; fi 
        mv v20*_withSky.fits withSky  ;  chmod 444 withSky/v20*_withSky.fits
        mv addSky_??.* withSky            # scripts and inputs
        rm sky_20*.fits bpm*[0-9].fits    # links to ../calib/sky and bpm files
		rm zeroes.fits bpm*link.fits
        rm v20*_0????.fits                # links to orig images in origs dir
		

        ec "# addSky checks complete; no problem found ... continue" 
        ec "#-----------------------------------------------------------------------------"
		if [ -e list_special ]; then
			ec "# ATTN: removing list_special and continuing with full list"
			dd=$(date "+%y%m%d")
		    cp list_special list_special_addSky_$dd
#			ec "########   QUIT HERE after addSky with list_special   ########" ; exit
		fi
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi

#	echo "#### STOP HERE TO FINISH addSky ####" ; exit 0 
    #----------------------------------------------------------------------------------------------#
    #       Compute good sky - ATTN: here we use mkAlt.sh
    #----------------------------------------------------------------------------------------------#
    # Work done in subdirs; links to the input files are created there ==> No need for links here 
    # Inputs:  withSky images, masks, weights. NOT head: bogus head file built internally and used
    # Outputs: _alt files ... will be subtracted later
	# exec time: 3.5-4.5 min/file (TBC)
    #----------------------------------------------------------------------------------------------#

    ns=$(ls mkAlt/v20*_alt.fits 2> /dev/null | wc -l) #; echo $nn $ns
    if [ -e mkAlt.log ] && [ $ns -ge 5 ]; then
        ec "CHECK: Found mkAlt.log files and $ns _alt.fits files for $nimages images. "
        ec "CHECK: ====> proper skies already built - skip to next step"
        ec "#-----------------------------------------------------------------------------"
    else     
        rcurr=R13

        if [ -e list_special ]; then
            list=list_special;  ec "#### ATTN: Using special list ####"
        else
            list=list_wSky
			if [ -e $list ]; then 
				cd withSky
				ls v20*_withSky.fits 2> /dev/null| sed 's/_withSky//' > ../$list
				cd ..
			fi
        fi
        nimages=$(cat $list | wc -l)

        ec "## - R13: mkAlt.sh: determine and subtract good sky from $nimages images"    
        ec "#-----------------------------------------------------------------------------"
        nout=$(ls -1 mkAlt_??.out 2> /dev/null | wc -l)
        nlog=$(ls -1 mkAlt_??.log 2> /dev/null | wc -l)
        if [ $nout -ge 1 ] || [ $nlog -ge 1 ]; then
            ec "#### ATTN: found $nout mkAlt_??.out and $nlog mkAlt_??.log files ... delete them and continue??"
            askuser
        fi
        rm -f mkAlt.submit mkAlt_??.lst mkAlt_??.skylst mkAlt_??.sh   mkAlt_??.out mkAlt_??.log
        # Don't need links to withSky images here (images/): mkAlt work is done in 
        # subdirs and needed links are created there. 

        # nexp=250  # Used for H, aug.23
		nexp=400  
        if [ $nimages -lt 620 ]; then nts=7; else nts=$(($nimages/$nexp + 1)); fi
        ec "# Now split into $nts chunks of max $nexp images, normally doable in 35 hrs"
        split -n l/$nts $list --additional-suffix='.lst' mkAlt_

        # build sublists with images from which to choose skies:
        if [ ! -e list_special ]; then      # ok when list is "continuous"
            ec "# Build skylists using full list of images..."
            ls -1 mkAlt_??.lst > srclist   # list of lists
            for f in $(cat srclist); do
                e=$(grep -B1 $f srclist | head -1) #; echo $e   # file before, if there, or $f
                g=$(grep -A1 $f srclist | tail -1) #; echo $g   # file after, if there, or $f
                olist=${f%.lst}.skylst
                ec "# Build $olist for $f"
                
                if [ "$e" == "$f" ]; then      # $f is first list
                    cat $f       > $olist      # take the list ...
                    head -20 $g >> $olist      # append first 20 from next list

                elif [ "$f" == "$g" ]; then    # $f is last list
                    tail -20 $e  > $olist      # take last 20 of previous list ...
                    cat $f      >> $olist      # append current list

                else                           # all other files
                    tail -20 $e  > $olist      # take last 20 of previous list ...
                    cat $f      >> $olist      # append curent list ...
                    head -20 $g >> $olist      # append first 20 from next list
                fi
				# remove "fixed offset" images (have same jitter_i)
				j1=$(grep $f jitter.dat)
				nj1=$(echo j1 | wc -l)
				if ( nj1 -eq 1); then    # file is in jitter.dat
					pp1=$(echo $j1 | cut -d\  -f2) #   ; echo $pp1
				fi
            done
        else                               # look for nearby frames in full list.
            ec "# Build skylsts using partial list ..."
            for l in mkAlt_??.lst; do
                olist=${l%.lst}.skylst
                rm -rf tmplist $olist
                for f in $(cat $l); do grep $f -A20 -B20 list_special >> tmplist; done
                sort -u tmplist > $olist
            done
        fi
        rm -f srclist tmplist

        ec "#-----------------------------------------------------------------------------"
        # build ad-hoc shell scripts
        for l in mkAlt_??.lst; do
            id=$(echo $l | cut -d\_ -f2 | cut -d\. -f1) 
            qfile=mkAlt_${id}.sh  ; touch $qfile; chmod 755 $qfile
            sed -e 's|@NODE@|'$node'|'  -e 's|@LIST@|'$l'|'  -e 's|@DRY@|0|' \
                -e 's|@IDENT@|'$PWD/mkAlt_$id'|'   -e 's|@FILTER@|'$FILTER'|' \
                -e 's|@ID@|'$id'|'  -e 's|@WRK@|'$WRK'|'  \
                $bindir/mkAlt.sh > $qfile         ############ chgd to Alt
            
            ec "# Built $qfile with $(cat $l | wc -l) entries"
            echo "qsub ${qfile} ; sleep 1" >> mkAlt.submit
        done  
        ec "#-----------------------------------------------------------------------------"
        ec "# ==> written to file 'mkAlt.submit' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec "# Submit qsub files ... ";  source mkAlt.submit
        ec " >>>>   wait for $nts mkAlt jobs ...  <<<<<"
        
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        echo 
        echo "# initial mkAlt work directories are: "
        ls -ld /scratch??/mkAlt*_${FILTER} ./mkAlt*_${FILTER} 2> /dev/null | tee -a $pipelog
        echo
        echo " ----------  Begin monitoring  ----------"
        while :; do             # qsub wait loop for mkAlt
            ndone=$(ls $WRK/images/mkAlt_??.out 2> /dev/null | wc -l)
            [ $ndone -eq $nts ] && break          # jobs finished
            
            running=$(qstat -au moneti | grep mkAltSky_${FILTER}_ | grep \ R\  | wc -l)
            fdone=$(ls -1 /[ns]*/mkAlt_??_${FILTER}/v20*_alt.fits v20*_alt.fits 2> /dev/null | wc -l)
            ftodo=$(($nimages - $fdone)) 
			if [ ${running} -ge 1 ]; then
				nsec=$(( 200*${ftodo}/${running} ))
			else                 ##  case all jobs qeued
				ec "### ATTN: no jobs running - all queued? ... PROBLEM???"
				nsec=3600
			fi
            if [ $nsec -le 60 ];   then nsec=60; fi            # min: 1 min
            if [ $nsec -gt 3600 ]; then nsec=3600; fi          # max: 1 hr
            nmin=$(echo "scale=1; $nsec/60" | bc); nhrs=$(echo "scale=1; $nmin/60" | bc)
            if [ $nsec -gt 5400 ]; then wmsg="$nhrs hr"; else wmsg="$nmin min"; fi
            echo "$(date "+[%d.%h %T"]) $running jobs running; $fdone skies done, $ftodo remaining - next check in $wmsg "
            nn=$(\ls -lh v20*_alt.fits 2> /dev/null | grep -v 257M | wc -l)
            if [ $nn -gt 0 ]; then 
                echo "# WARNING: Found $nn _sky files probably incomplete ... continuing nevertheless"
            fi
            sleep $nsec
        done  
        ec "# mkAlt finished; walltime $(wtime) - check results ..."
        # ---------------------- check products ----------------------

        rm -f estats         # before filling it
        grep STATUS:\  mkAlt_??.out | grep -v \ 0 > estats
        nbad=$(grep -v STATUS:\ 0  estats 2> /dev/null | wc -l)
        if [ $nbad -gt 0 ]; then
            ec "#### PROBLEM: some mkAlt's exit status not 0 ... "
            cat estats;    askuser
        else
            ec "CHECK: mkAlt.sh exit status ok ... continue"; rm -f estats 
        fi

        # build general logfile
        grep CHECK: mkAlt_??.log > mkAlt.log

        ### handle frames which have insuffient nearby frames to determine sky ###
        grep skip mkAlt.log | cut -d\: -f3 | tr -d ' '  > list_noSky
        nn=$(cat list_noSky | wc -l)
        if [ $nn -ge 1 ]; then
            ec "# Found $nn images with insufficient neighbours to build sky, see list_noSky"
            echo "# $nn files with too few neighbours to build sky" >> $badfiles
            cat list_noSky >> $badfiles
            ec "# ... removed them (links) and added names to $badfiles"
            ls -1 v20*_0????_alt.fits | sed 's/_sky//' > list_images
            nimages=$(cat list_images | wc -l)
            ec "# ==> $nimages valid images left."
        else
            ec "# Found NO images with insufficient neighbours to build sky ... WOW!"
            rm list_noSky
        fi

        ### check number of valid files produced:
        nnew=$(ls -1 v20*_alt.fits | wc -l)
        if [ $nnew -lt $nimages ]; then
            ec "CHECK: found only $nnew _sky files; $(($nimages-$nnew)) missing...."
            ls v20*_alt.fits | sed 's/_alt//' > list_done
            comm -23 list_images list_done > list_missing
            askuser
        fi

        # quick check of file completeness
        \ls -lh v20*_alt.fits > tmplist
        nn=$(grep -v 257M tmplist | wc -l )
        if [ $nn -ne 0 ]; then
            ec "CHECK: found $nn incomplete files (size != 257MB): "
            grep -v 257M tmplist
            askuser
        fi
        chmod 644 v20*_alt.fits v20*_cnt.fits mkAlt_??.???
		ls v20*_alt.fits > list_skies

        ### cleanup; split to avoid arglist too long error
        if [ ! -d mkAlt ]; then mkdir mkAlt; fi
        mv v20*_alt.fits mkAlt_??.* mkAlt
		mv v20*_cnt.fits mkAlt; cp list_skies mkAlt

        ec "# mkAlt checks complete; no problem found ... continue"
        ec "#-----------------------------------------------------------------------------"
		if [ -e list_special ]; then
			ec "# ATTN: removing list_special and continuing with full list"
			dd=$(date "+%y%m%d")
		    cp list_special list_special_mkAlt_${dd}
			ec "########   QUIT HERE after mkAltSky with list_special   ########"
		fi
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
    

    #----------------------------------------------------------------------------------------------#
    #       Update weights: set _weight to 0 where no sky was determined
    #----------------------------------------------------------------------------------------------#
    # Inputs: weight.fits file and _alt.fits files; 
    # Output: new weight.fits, with unseen pixels (0 in _alt.fits) are also set to zero
    # 
    #----------------------------------------------------------------------------------------------#

    if [ -s updateWeights.log ]; then
        ec "CHECK: found updateWeights.log file ..."
        ec "CHECK: ====> weight files already updated - skip to next step"
        ec "#-----------------------------------------------------------------------------"
    else     
        rcurr=R14  ; echo "[DEBUG] Begin R14 ..."
        if [ -e list_special ]; then
            list=list_special;  ec "#### ATTN: Using special list ####"
        else
            list=list_images
			nn=$(cat $list 2> /dev/null | wc -l)
#			echo "[DEBUG] list is $list with $nn entries"
			if [ $nn -eq 0 ]; then 
				cd mkAlt
				ls v20*_alt.fits 2> /dev/null| sed 's/_alt//' > ../$list 
				cd -
				nn=$(cat $list 2> /dev/null | wc -l)
				ec "#### ATTN: rebuilt $list with $nn entries"
			fi
        fi
        nimages=$(cat $list | wc -l)
#        echo "[DEBUG] $list $nimages ; pwd" ; exit

        ec "## - R14: updateWeights.sh: to exclude area where no sky is calculated for $nimages images"  
        ec "#-----------------------------------------------------------------------------"
        nout=$(ls -1 updateWeights_??.out 2> /dev/null | wc -l)
        nlog=$(ls -1 updateWeights_??.log 2> /dev/null | wc -l) 
        if [ $nout -ge 1 ] || [ $nlog -ge 1 ]; then
            ec "#### ATTN: found $nout updateWeights_??.out and $nlog update_weights_??.log files ... "
            ec "      ... delete them and continue??"
            askuser
        fi
        # remove old files if any
        rm -f updateWeights.submit updateWeights_??.out  updateWeights_??.lst  updateWeights_??.sh






		nexp=999
        if [ $nimages -lt 420 ]; then nts=7; else nts=$(($nimages/$nexp + 1)); fi
        split -n l/$nts $list --additional-suffix='.lst' updateWeights_

        for l in updateWeights_??.lst; do
            id=$(echo $l | cut -d\_ -f2 | cut -d\. -f1) 
            qfile=updateWeights_${id}.sh  ; touch $qfile; chmod 755 $qfile
            sed -e 's|@NODE@|'$node'|'  -e 's|@LIST@|'$l'|'  -e 's|@DRY@|0|' \
                -e 's|@IDENT@|'$PWD/updateWeights_$id'|'   -e 's|@FILTER@|'$FILTER'|' \
                -e 's|@ID@|'$id'|'  -e 's|@WRK@|'$WRK'|'  \
                $bindir/updateAlt.sh > $qfile
            
            ec "# Built $qfile with $(cat $l | wc -l) entries"
            echo "qsub $qfile; sleep 1" >> updateWeights.submit
        done      
        ec "# ==> written to file 'updateWeights.submit' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec "# Submit qsub files ... ";  source updateWeights.submit
        ec " >>>>   wait for $nts updateWeights jobs ... first check in 1 min  <<<<<"
        
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        while :; do             # qsub wait loop for updateWeights
            ndone=$(ls $WRK/images/updateWeights_??.out 2> /dev/null | wc -l)
            [ $ndone -eq $nts ] && break          # jobs finished
             # No obvious way to monitor progress ... thus just wait ... pretty fast anyway           
            sleep 60
        done  
        ec "# updateWeights jobs  finished; walltime $(wtime) - check results ..."
        
        # ---------------------- check products ----------------------

        nbad=$(grep -v STATUS:\ 0 estats 2> /dev/null | wc -l)
        if [ $nbad -gt 0 ]; then
            ec "#### PROBLEM: some updateWeights's exit status not 0 ... "
            grep -v \ 0  estats;    askuser
        else
            ec "CHECK: updateWeights.sh exit status ok ... continue" ; rm -f estats
        fi
        
        # join the logfiles - contains number of new pixels masked for each frame
        cat updateWeights_??.log > updateWeights.log

        chmod 644 updateWeights_??.???
        # cleanup
        if [ ! -d updateWeights ]; then mkdir updateWeights; fi
        mv updateWeights_??.* updateWeights

        # rm all links to images, weights, etc ... separately to avoid arg list too long
        rm v20*_weight.fits 
        rm v20*_alt.fits
        rm v20*_mask.fits zeroes.fits # v20*.head

        ec "# updateWeights checks complete; no problem found ... continue"
        ec "#-----------------------------------------------------------------------------"
		if [ -e list_special ]; then
			ec "# ATTN: removing list_special and continuing with full list"
			dd=$(date "+%y%m%d")
		    cp list_special list_special_mkAlt_${dd}
			ec "########   QUIT HERE after updateAlt with list_special   ########"
		fi
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi

##  FOR DR6: actual sky subtraction and destriping remains in P3

    #----------------------------------------------------------------------------------------#
    #       Actual sky subtraction, destriping, and large-scale bgd removal
    #----------------------------------------------------------------------------------------#
    # Requires: orig file, _alt _mask images
    # Produces _cln (cleaned) files)
    # 
    #----------------------------------------------------------------------------------------#

	cltag=cln    # or clean
    nn=$(ls cleaned/v20*_${cltag}.fits 2> /dev/null | wc -l)

    if [ -e subAlt.log ] && [ $nn -ge $nimages ]; then 
        ec "CHECK: Found $nn clean images in cleaned dir and subAlt.log files "
        ec "CHECK: ====> skip to next step "
        ec "#-----------------------------------------------------------------------------"
    else     
        rcurr=R15
        ec "## - R15:  subAlt.sh: pure sky-sub, destriping, and lsb-cleaning of $nimages images "
        ec "#-----------------------------------------------------------------------------"

        nout=$(ls -1 subAlt_??.out 2> /dev/null | wc -l)
        nlog=$(ls -1 subAlt_??.log 2> /dev/null | wc -l) # ; echo $nout $nlog
        if [ $nout -ge 1 ] || [ $nlog -ge 1 ]; then
            ec "#### ATTN: found $nout subAlt_??.out and $nlog subAlt_??.log files "
            if [ ! -s list_special ]; then 
                ec "#### ATTN: is this a test? ... delete them and continue??"
                askuser
                rm subAlt_??.out subAlt_??.log
            else
                ec "#### ATTN: also found list_special ... continue "
            fi
        fi
        rm -f subAlt.submit estats subAlt_??.lst subAlt_??.sh

        if [ -s list_special ]; then 
            list=list_special; ec "#### ATTN: Using special list ####"
        else
            list=list_images
			nn=$(cat $list 2> /dev/null | wc -l)

			if [ $nn -eq 0 ]; then 
				cd mkAlt
				ls v20*_alt.fits 2> /dev/null| sed 's/_alt//' > ../$list
				cd -
				nn=$(cat $list 2> /dev/null | wc -l)
				ec "#### ATTN: rebuilt $list with $nn entries"
			fi
        fi
        nimages=$(cat $list | wc -l)

        # check (again?) the masks
        grep '\ 0.00' mkMasks.dat > badMasks.dat
        nn=$(cat badMasks.dat | wc -l)
        if [ $nn -gt 0 ]; then
            ec "#### PROBLEM: some masks contains chips that are entirely masked ... see badMasks.dat "
            askuser
        else
            ec "CHECK: _mask files ok: no fully masked chips ... continue "
            rm -f badMasks.dat
        fi

        ec "# ==> Build processing scripts for $nimages files from $list:"
        nexp=1000        # exec time: 45 sec/file  ==> 
        if [ $nimages -lt 450 ]; then nts=7; else nts=$(($nimages/$nexp + 1)); fi
#        if [ $nimages -lt 300 ]; then nts=15; fi  # for testing
#        if [ $nts -le 5 ]; then nts=5; fi      # to avoid few very long lists

        split -n l/$nts $list --additional-suffix='.lst' subAlt_
        
        for l in subAlt_??.lst; do
            nl=$(cat $l | wc -l)
            if [ $nl -ge 1 ]; then
                id=$(echo $l | cut -d\_ -f2 | cut -d\. -f1) 
                qfile=subAlt_${id}.sh  ; touch $qfile; chmod 755 $qfile
                sed -e 's|@NODE@|'$node'|'  -e 's|@LIST@|'$l'|'  -e 's|@DRY@|0|' \
                    -e 's|@IDENT@|'$PWD/subAlt_$id'|'   -e 's|@FILTER@|'$FILTER'|' \
                    -e 's|@ID@|'$id'|'  -e 's|@WRK@|'$WRK'|'  \
                    -e 's/@CLTAG@/'$cltag'/'  $bindir/subAlt.sh > $qfile    ######## chgd to Alt
            
                ec "# Built $qfile with $nl entries"
                echo "qsub $qfile; sleep 1" >> subAlt.submit
            else
                ec "#### ATTN: list $l empty ..."
            fi
        done   
        nts=$(cat subAlt.submit | wc -l)
        ec "# ==> written to file 'subAlt.submit' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec  "# Submit qsub files ... ";  source subAlt.submit
        ec " >>>>   wait for $nts subAlt jobs ... first check in 1 min  <<<<<"
        
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        while :; do             # qsub wait loop for subAlt
            ndone=$(ls $WRK/images/subAlt_??.out 2> /dev/null | wc -l)
            [ $ndone -eq $nts ] && break          # jobs finished
            
            fdone=$(ls /*/subAlt*${FILTER}/v20*_cln.fits $WRK/images/cleaned/v20*_cln.fits 2> /dev/null | wc -l)
            running=$(qstat -au moneti | grep subAlt_${FILTER}_ | grep \ R\  | wc -l) 
			ftodo=$(($nimages - $fdone))
			if [ $running -gt 0 ]; then
				nsec=$(( 30*${ftodo}/${running} ))
			else
				ec "### ATTN: no jobs running - all queued? ... PROBLEM???"
				nsec=3600
			fi
            if [ $nsec -le 60 ];   then nsec=60; fi            # min: 1 min
            if [ $nsec -gt 3600 ]; then nsec=3600; fi          # max: 1 hr
            nmin=$(echo "scale=1; $nsec/60" | bc); nhrs=$(echo "scale=1; $nmin/60" | bc)
            if [ $nsec -gt 5400 ]; then wmsg="$nhrs hr"; else wmsg="$nmin min"; fi
            echo "$(date "+[%d.%h %T"]) $running jobs running; $fdone images cleaned of $nimages - next check in $wmsg "
            sleep $nsec
        done  
        ec "# subAlt jobs  finished; walltime $(wtime) - check results ..."
        
        # ---------------------- check products ----------------------

        grep EXIT\ STATUS subAlt_??.out >> estats
        nbad=$(grep -v STATUS:\ 0  estats 2> /dev/null | wc -l)
        if [ $nbad -gt 0 ]; then
            ec "#### PROBLEM: some subAlt's exit status not 0 ... "
            grep -v \ 0  estats;    askuser
        else
            ec "CHECK: subAlt.sh exit status ok ..."
            rm -f estats 
        fi

        # check for errors in .out files
        grep -i error subAlt_??.out > subAlt.err
        nn=$(cat subAlt.err | wc -l)
        if [ $nn -ne 0 ]; then
            ec "CHECK: found $nn errors in torque .out files .... see subAlt.err "
            askuser
        fi

        # build general logfile: join subAlt_??.log files
        grep -e Begin\ work -e output subAlt_??.log > subAlt.log

        chmod 644 v20*_*_${cltag}.fits subAlt_??.???

        if [ ! -d cleaned ]; then mkdir cleaned; fi
        mv v20*_${cltag}.fits subAlt_??.* cleaned            # and other subAlt files
        # leave global subAlt.log to check if done

		cd cleaned/
        ls v20*_${cltag}.fits > ../list_cleaned
		cd ..
        ncleaned=$(cat list_cleaned | wc -l)
		ec "CHECK: found $ncleaned _${cltag} files "

		if [ -e list_special ]; then
			ec "# ATTN: removing list_special, continuing with full list"
			dd=$(date "+%y%m%d")
		    cp list_special list_special_subAlt.$dd
		fi

        ec "CHECK: subAlt.sh done, $ncleaned clean images moved to dir cleaned/"
        ec "#-----------------------------------------------------------------------------"
        ec "# Number of input (CASU) files ................ $(cat list_origs    | wc -l)"
        ec "# Number of files accepted for processing ..... $(cat list_accepted | wc -l)"
        ec "# Number of files for which sky was built ..... $(cat list_skies    | wc -l)"
        ec "# Number of files processed and cleaned ....... $(cat list_cleaned  | wc -l)"
#        ec "#-----------------------------------------------------------------------------"
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
    
    cd $WRK  
	ec ""
    ec "#-----------------------------------------------------------------------------"
    ec "#                                End of $pcurr "
    ec "#           Now use pssm.sh to scamp/swarp/merge cleaned files "
    ec "#                                                                             #"
    ec "#                   Non senza fatiga si giunge al fine                        #"
    ec "#-----------------------------------------------------------------------------#"
    exit 0

#@@ ------------------------------------------------------------------------------------
#@@  And options for status checking
#@@ ------------------------------------------------------------------------------------

elif [[ $1 =~ 'env' ]]; then   # env: check environment
   procenv

elif [[ $1 == 'help' ]] || [[ $1 == '-h'  ]] ; then  # help, -h: list pipeline options
    pipehelp

elif [ $1 = 'plists' ]; then  # plists: rebuild paw lists and check

    mycd $WRK/images
    ec " >> Rebuild list_images and list_paw? ...  Not implemented here"

  ## ec "##-----------------------------------------------------------------------------"

#-----------------------------------------------------------------------------------------------
else
#-----------------------------------------------------------------------------------------------
   echo "!! ERROR: $1 invalid argument ... valid arguments are:"
   help
fi 

exit 0
#@@ ------------------------------------------------------------------------------------

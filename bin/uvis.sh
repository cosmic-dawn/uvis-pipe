#!/bin/bash 
#-----------------------------------------------------------------------------
# File: uvis.sh 
#-----------------------------------------------------------------------------
# Purpose:   Pipeline to process UltraVista DR5 data - by filter
# Requires: 
# - work directory with data, given by $WRK env. var.
# - python3, python scripts from terapix pipe adapted to python 3,
#            in ~/softs/uvis-pipe/python etc.
# - wrapper scripts in ~/softs/uvis-pipe/bin
# Author:    A. Moneti - Nov.19
#-----------------------------------------------------------------------------
# Versions:
# v3.00: initial version, from DR4 processing                      (22.nov.19)
# v3.01: updated up to conv. to WIRCAM, and more                   (29.nov.19)
# v3.02: updates for qFits pscamp, and more                        (21.oct.20)
# v3.03: split pscamp run by pairs of paws                         (09.dec.20)
# v3.04: general revision of swarp for pass 1                      (23.jan.21)
# v3.05: general debugging to end                                  (19.may.21)
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

node=$(echo $WRK | cut -c 2-4)
DR5=$(echo $WRK  | cut -d/ -f1-4)

REL="DR5-"$FILTER                      # used in names of some products

badfiles=$WRK/DiscardedFiles.list      # built and appended to during processing
fileinfo=$WRK/FileInfo.dat             # lists assoc files (bpm, sky, flat) and other info for each image

pipelog=${WRK}/uvis.log ; if [ ! -e $pipelog ]; then touch $pipelog; fi
Trash=zRejected         ; if [ ! -d $Trash ]; then mkdir $Trash; fi

#if [ -h $0 ]; then pn=$(readlink -f $0); else pn=$(ls $0); fi    # get the command line

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
    echo "  - Release area is   "$(ls -d $DR5)
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
DR5=/n08data/UltraVista/DR5/
bpmdir=/n08data/UltraVista/DR5/bpms

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
   
    bpmdir=/n08data/UltraVista/DR5/bpms
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
        ec "ATTN: images/ and calib/ dirs already exist ... delete them and restart"
        exit 0
    fi

if ([ $HOSTNAME != "candid01.iap.fr" ] || [ $HOSTNAME != "c02.iap.fr" ]) && [ $dry != 'T' ]; then  
    ec "#=========================================================#"
    ec "# ATTN: cannot start jobs from $HOSTNAME. Switched to dry mode  #"
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
        ec "PROBLEM: directory structure not setup ... quitting"
        exit 5
    fi
    mycd $WRK/images

    # rebuild list_images
    #ls -1 v20*_0????.fits > list_images
    nimages=$(cat list_images | wc -l)

    if [ $nimages -eq 0 ]; then 
        ec "!! ERROR: no images found ... "
        askuser
    fi
        
    ec "CHECK: found list_images with $nimages entries "
    ec "#-----------------------------------------------------------------------------"

    # check for existing _WIRCam files
    nwirc=$(ls v20*_WIRCam.fits 2> /dev/null | wc -l)
    if [ $nwirc -gt 0 ]; then
        ec "### ATTN: ###  found $nwirc _WIRCam files ... delete them and start over"
        exit 0
    fi
        
    # to check conv to WIRCam, look for IMRED_FF and IMRED_MK kwds exist in ext.16
    nkwds=$(dfits -x 16 origs/$(tail -1 list_images) | grep IMRED_ | wc -l) #; echo $nkwds
 
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
            echo "qsub $qfile" >> $WRK/convert_all
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
            while :; do                          # qsub wait loop for destripe
                ndone=$(ls pconv_?.out 2> /dev/null | wc -l)
                [ $njobs -eq $ndone ] && break          # jobs finished
                sleep 60
            done  
            ec "# convert jobs finished, walltime: $(wtime) - check results ..."
            npb=$(grep PROBLEM pconv_?.out | wc -l)
            if [ $npb -ne 0 ]; then
                ec "## PROBLEM ## problem(s) found in .out files ... please check"
                grep PROBLEM pconv_?.out
                askuser
            else
                ec "## All scripts terminated successfully ..."
            fi
            
            mycd $WRK/images   # back here to check products
            # check for existing _WIRCam files
            nwirc=$(ls v20*_WIRCam.fits 2> /dev/null | wc -l)
            if [ $nwirc -ne $nimages ]; then
                echo "### ATTN: ###  found only $nwirc _WIRCam files for $nimages expected. "
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
 
    \ls -1 *_flat_*.fits | grep -v norm > list_flats  
    nl=$(cat list_flats | wc -l)
    nf=$(ls *_flat_*_norm.fits 2> /dev/null | wc -l)    #;   echo $nl $nf

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
        while :; do                          # qsub wait loop for destripe
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
        ec "CHECK: Found $(echo $fileinfo | cut -d\/ -f 6) ... continue"
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
        for f in $(cat list_bpms);   do if [ ! -s $DR5/bpms/$f ];  then echo "#### ATTN: $f missing in calib";  echo $f >> list_missing; err=1; fi; done
        if [ $err -eq 1 ]; then ec "# missing files ... see images/list_missing"; askuser
        else ec "# All needed flats, bpm, skies, stacks available ... continue";  fi
        rm list_bpms list_flats list_skies list_stacks
 
        ec "#-----------------------------------------------------------------------------"
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
    
    if [ $HOSTNAME != "candid01.iap.fr" ] && [ $HOSTNAME != "c02" ]; then  
        ec "#=========================================================#"
        ec "# ATTN: cannot start jobs from $HOSTNAME. Switched to dry mode  #"
        ec "#=========================================================#"
        dry='T'
    fi

    #----------------------------------------------------------------------------------------------#
    #       pseudo qualityFITS
    #----------------------------------------------------------------------------------------------#

    mycd $WRK/images

    nl=$(ls v20*_0????.ldac 2> /dev/null | wc -l)
    if [[ -e $WRK/qFits.DONE ]] && [ $nl -ge 1 ] ; then 
        ec "CHECK: Found qFits.DONE and $nl ldacs ..."
        ec "CHECK: ==> qFits has been run ... skip to next step"
        ec "#-----------------------------------------------------------------------------"
    else
        rcurr=R5  
        if [ -s list_special ]; then 
            list=list_special; ec "#### ATTN: Using special list"
        else 
            list=list_images
        fi
        nims=$(cat $list | wc -l)

        ec "## - R5:  qFits.sh:  pseudo qFits on $list with $(cat $list | wc -l) entries"
        ec "#-----------------------------------------------------------------------------"
        rm -f $WRK/qFits*.* qFits_*.??? chunk_*.lst 
        
        # use small chunk size for testing
        if [ $nims -ge 120 ]; then 
            n_chunks=18
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
            echo "qsub $qfile; sleep 2" >> $WRK/qFits.submit
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

        ec "# Submit qsub files ... ";  source qFits.submit
        ec " >>>>   Wait for $nts qFits jobs ...  <<<<<"

        btime=$(date "+%s.%N");  sleep 30      # before starting wait loop
        while :; do                            # begin qsub wait loop for qFits
            nn=$(ls qFits_*.out 2> /dev/null | wc -l)   # jobs done
            [ $nn -eq $nts ] && break          # jobs finished

            ndone=$(ls -1 images/v20*_0????.ldac 2> /dev/null | wc -l)   # images processed
            ntodo=$(($nims - $ndone)) 
            nsec=${ntodo} 
            if [ $nsec -le   60 ]; then nsec=30; fi            # min: 1 min
            if [ $nsec -ge 3600 ]; then nsec=3600; fi          # max: 1 hr
            nmin=$(echo "scale=1; $nsec/60" | bc)
            nhrs=$(echo "scale=1; $nmin/60" | bc)
            if [ $nsec -gt 5400 ]; then wmsg="$nhrs hr"; else wmsg="$nmin min"; fi
            ec "# $ndone images processed, $ntodo remaining - next check in $wmsg"
            sleep $nsec
        done  
        ec "# qFits jobs finished, walltime: $(wtime) - check results ..."

        # check torque EXIT STATUS
        grep 'EXIT\ STATUS' qFits_??.out > estats
        ngood=$(grep STATUS:\ 0 estats | wc -l)
        if [ $ngood -ne $nts ]; then
            ec "#PROBLEM: some qFits' exit status not 0 ... "
            grep -v \ 0 estats;    askuser
        else
            ec "CHECK: qFits.sh exit status ok ... continue"
            rm estats
        fi

        # remove empty lines from torque logfiles
        for f in qFits_??.out; do strings $f > xx; mv xx $f; done

        cd images
        ec "# Build Nstars.dat file"
        for l in v2*[0-9].ldac; do 
            echo -n ${l%.ldac}\ \ ; dfits -x 0 $l | grep NAXIS2 | grep -v \ 1\  | \
            awk '{printf "%4i ",$3}; END{print " "}' | awk '{print $0, $1+$2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13+$14+$15+$16}' 
        done > Nstars.tab
        cd ..

        # ---------------------- Finished qFits run; check products ----------------------

        nldacs=$(ls -1 $WRK/images/v20*_0????.ldac        2> /dev/null | wc -l)
        nwghts=$(ls -1 $WRK/images/v20*_0????_weight.fits 2> /dev/null | wc -l)
        npsfex=$(ls -1 $WRK/images/v20*_0????_psfex.xml   2> /dev/null | wc -l)
        ec "# Found $nwghts weights, $nldacs ldacs, $npsfex psfex.xml files for $nims images "
        
        grep -ni ERROR   qFits_??.out | grep -v FutureWarn > qFits.errs ;  nwarn=$(cat qFits.warns 2> /dev/null | wc -l)
        grep -ni WARNING qFits_??.out | grep -v ATLAS > qFits.warns;  nerrs=$(cat qFits.errs 2> /dev/null| wc -l)

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

        mycd images   #--------- Back to images for various checks
        rm -f missing.ldacs  missing.weights  # to rebuild them
        if [ $nwghts -ne $nims ]; then 
            for f in $(cat list_images); do 
                if [ ! -e ${f%.fits}_weight.fits ]; then 
                    ec "#### ATTN: weight missing for $f" >> missing.weights
                fi
            done
            ec "# PROBLEM: $(($nims-$nwghts)) weights missing ... see missing.weights"
        fi
        
        if [ $nldacs -ne $nims ]; then 
            for f in $(cat list_images); do 
                if [ ! -e ${f%.fits}.ldac ]; then echo "#### ATTN: ldac missing for $f" >> missing.ldacs; fi
            done
            ec "# PROBLEM: $(($nims-$nldacs)) ldacs missing ... see missing.ldacs"
        fi
        
        if [ $npsfex -ne $nims ]; then ec "# PROBLEM: $(($nims-$npsfex)) _psfex's missing ... "; fi
        
        if [ -s missing.weights ] || [ -s missing.ldacs ]; then 
            cat missing.ldacs missing.weights 2> /dev/null | cut -d\  -f7,7 > list_missing
            nmiss=$(cat list_missing | wc -l)
            ec "# PROBLEM: see list_missing "; askuser
        fi
        # Build table with num stars per extension
        
        ec "# qFits runs successfull ...  GOOD JOB!! Clean-up and continue"
        
        # Now clean up .....
        rm qFits_??.lst v20*cosmic.fits v20*flag.fits v20*psfex.ldac
        if [ ! -d  ldacs  ]; then mkdir  ldacs  ; fi
        if [ ! -d weights ]; then mkdir weights ; fi
        ec "# move _weight files to weights/, ldac files to ldacs/, and build links"
        mv v20*_weight.fits weights;  ln -s weights/v20*_weight.fits .
        mv v20*_0????.ldac v20*_noSAT.ldac  ldacs  # original and flagged
        ln -s ldacs/v20*_0????_noSAT.ldac . 
        rename _noSAT.ldac .ldac v20*noSAT.ldac

        echo  "# qFits runs successfull ...  GOOD JOB!! " > $WRK/qFits.DONE
    fi

    #----------------------------------------------------------------------------------------------#
    #       Get psf stats from psfex
    #----------------------------------------------------------------------------------------------#
 
    mycd $WRK/images
#   ls Nstars.tab  $WRK/PSFsel.out
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
            #njobs=$(ls $WRK/PSFsel.out 2> /dev/null | wc -l)
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
            ec "PROBLEM: PSFsel.sh exit status not 0 ... check PSFsel.out"
            askuser
        fi
        ec "CHECK: PSFsel.sh exit status ok ... continue"

        nerr=$(grep Error PSFsel.out | wc -l)
        if [ $nerr -gt 0 ]; then
            ec "PROBLEM: found $nerr errors in PSFsel.out "
            askuser
        fi

        ndone=$(grep ^v20 $WRK/images/PSFsel.dat | wc -l)
        if [ $ndone -ne $nl ]; then
            ec "PROBLEM: found only $ndone lines of $nl in PSFsel.dat "
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

#% P2: scamp, swarp, destripe, p1 stack, its mask, etc.
#% - run scamp with gaia catal to build head files; rm ldac files
#% - destripe the casu images
#% - run swarp to build substacks - by paw for now, at firstpass resol'n
#% - merge the substacks into the pass1 stack
#% - and build its mask and associated products
#%------------------------------------------------------------------

    mycd $WRK/images
#    nn=$(ls UVIS*p1.fits UVIS*p1_weight.fits UVIS*p1_ob_flag.fits 2> /dev/null | wc -l )  # DR4
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
        ec "WARNING: 'list_images not found ... build it "
        cd origs; ls -1 v20*_0????.fits > ../list_images; cd -
    fi

    if [ ! -s list_ldacs  ]; then 
        ec "WARNING: list_ldacs not found ... build it"
        cd ldacs; ls -1 v20*.ldac > ../list_ldacs; cd -
    fi

    if [ ! -s list_weights  ]; then 
        ec "WARNING: list_weights not found ... build it"
        cd weights; ls -1 v20*_weight.fits > ../list_weights; cd -
    fi

    if [ ! -e list_heads  ]; then 
        ec "WARNING: list_heads not found ... build it"
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
        ec "!!! PROBLEM: Number of images, ldacs, weights not the same ..."
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
            echo  "qsub $qfile ; sleep 2" >> $WRK/pscamp.submit
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
            njobs=$(ls images/pscamp_p??.out 2> /dev/null | wc -l)
            [ $njobs -eq 3 ] && break               # jobs finished
            sleep $nsec
        done  
        mv images/pscamp_p??.out .
        chmod 644 pscamp_p??.out

        ec "# $njobs pscamp_p?? jobs finished, walltime $(wtime) - now check exit status"
        ngood=$(grep STATUS:\ 0 pscamp_p??.out | wc -l)
        if [ $ngood -ne 3 ]; then
            ec "PROBLEM: pscamp.sh exit status not 0 ... check pscamp.out"
            askuser
        fi

        #-----------------------------------------------------------------------------
        mycd $WRK/images    # to run other checks
        #-----------------------------------------------------------------------------
        # check number of .head files produced
        nheads=$(ls -1 v20*.head | wc -l)
        if [ $nheads -lt $nl ]; then
            ec "PROBLEM: built only $nheads head files for $nl ldacs ... "
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
        #rm GAIA*.cat scamp_dr5.conf vista_gaia.ahead
        rm -f v20*.ldac    # normally not there

        ec "#-----------------------------------------------------------------------------"
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
    
#   if [ $nl -lt 100 ]; then exit 0; fi   #  DEBUG ... stop here if testing

    #----------------------------------------------------------------------------------------#
    #       destripe casu images - p1 (maybe)
    #----------------------------------------------------------------------------------------#
    # NB: destriping requires masks, which we don't have yet ... skip this at first
    #     but could do it on a second run once flag is build
    # 
    #----------------------------------------------------------------------------------------#

    if [ ! -e $stout_flag ]; then
        ec "# $stout_flag not found "
        destripe=F
    else
        destripe=T
    fi

    # check if destripe already done
#   if [ -e destripe_p1/destripe.log ]; then
    if [ $destripe == 'F' ]; then
        ec "# ATTN: skip pass-1 destriping - requires masks not yet built"
#       ec "CHECK: Found destripe_p1/destripe.log - destriping already done ..."
#       ec "CHECK: ==> skip to next step "
        ec "#-----------------------------------------------------------------------------"
    else 
        rcurr=R8
        ec "## - R8:  destripe.sh: destripe $nimages casu images (SKIP) "
        ec "#-----------------------------------------------------------------------------"
        rm -f destripe.submit estats destripe_??.log  destripe_??.sh

        # build links to files

        # show the links of the image files
        ecn "CHECK: image files links:  " 
        ls -l $imroot.fits | tr -s ' ' | cut -d' ' -f9-13

        ec "# ==> Build ad-hoc processing scripts:"
        rate=75                  # typical num images processed / hr
        nexp=$((2*$rate/3 * 18)) # number expected per process in 18 hrs
        nts=$(echo "$nimages / $nexp + 1" | bc) ##;     echo "$nimages $nexp $nts"

        if [ $nts -le 5 ]; then nts=12; fi      # to avoid few very long lists
        if [ $nimages -lt 50 ]; then nts=5; fi  # for testing
        split -n l/$nts list_images --additional-suffix='.lst' destripe_
        
        for l in destripe_??.lst; do
            nl=$(cat $l | wc -l)
            if [ $nl -ge 1 ]; then
                id=$(echo $l | cut -d\_ -f2 | cut -d\. -f1) 
                qfile=$WRK/destripe_${id}.sh  ; touch $qfile; chmod 755 $qfile
                sed -e 's|@NODE@|'$node'|'  -e 's|@LIST@|'$l'|'  -e 's|@DRY@|0|' \
                    -e 's|@IDENT@|'$PWD/destripe_$id'|'   -e 's|@FILTER@|'$FILTER'|' \
                    -e 's|@ID@|'$id'|'  -e 's|@OSUFF@|'_des'|'  -e 's|@WRK@|'$WRK'|'  \
                    $bindir/destripe.sh > $qfile            
                ec "# Built $qfile with $nl entries"
            else
                ec "ATTN: list $l empty ..."
            fi
            echo "qsub $qfile" >> destripe.submit
        done   

        ec "# ==> written to file destripe.submit with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec "# Submit qsub files ... ";  source destripe.submit >> $pipelog
        ec " >>>>   wait for $nts destripe jobs ... first check in 1 min  <<<<<"
        
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        while :; do                        # qsub wait loop for destripe
            njobs=$(qstat -au moneti | grep destripe_${FILTER}_ | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished
            
            runjobs=$(qstat -au moneti | grep destripe_${FILTER}_ | grep \ R\  | wc -l) 
            ndone=$(ls -1 v20*_0????_des.fits 2> /dev/null | wc -l)
            ntodo=$(($nimages - $ndone)) ; nsec=$((2*$ntodo))
            if [ $nsec -le 60 ];   then nsec=60; fi            # min: 1 min
            if [ $nsec -ge 3600 ]; then nsec=3600; fi          # max: 1 hr
            nmin=$(echo "scale=1; $nsec/60" | bc); nhrs=$(echo "scale=1; $nmin/60" | bc)
            if [ $nsec -gt 5400 ]; then wmsg="$nhrs hr"; else wmsg="$nmin min"; fi
            ec "# $runjobs jobs running; $ndone images destriped, $ntodo remaining - next check in $wmsg "
            sleep $nsec
        done  
        ec "# destripe jobs  finished; walltime $(wtime) - check results ..."

        # ---------------------- check products ----------------------

        grep EXIT\ STATUS destripe_??.out >> estats
        nbad=$(grep -v STATUS:\ 0  estats | wc -l)
        if [ $nbad -gt 0 ]; then
            ec "PROBLEM: some destripe's exit status not 0 ... "
            grep -v \ 0  estats;   askuser
        else
            ec "CHECK: destripe.sh exit status ok ..."; rm -f estats destipe.submit
        fi

        # join destripe*.log files
        cat destripe_??.log >> destripe.log
        
        # check number of files produced:
        nnew=$(ls -1 v20*_des.fits | wc -l)
        if [ $nnew -lt $nimages ]; then
            ec "CHECK: found only $nnew mask files; $(($nimages-$nnew)) missing...."
            askuser
        fi
        
        outdir=destripe_p1
        if [ ! -d $outdir ]; then mkdir $outdir; fi
        mv v20*_des.fits destripe_??.*  destripe.log $outdir
        ln -s $outdir/v*_des.fits .; rename _des.fits .fits v*_des.fits
        ec "CHECK: destripe.sh done, products moved to ${outdir}/ and linked ... continue"
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
            ec "PROBLEM: pswarp1_pawx_xx.sh exit status not 0 "
            grep -v STATUS:\ 0 estats 
            askuser
        fi
        ec "# CHECK: pswarp1_pawx_xx.sh exit status ok"; rm estats

        # check num sustacks found
        nn=$(ls substack*paw?_??.fits | wc -l)
        if [ $nn -lt $nq ]; then
            ec "PROBLEM:  found only $nn substacks for $nq expected ..."
            askuser
        fi

        # check sizes of substacks
        ns=$(\ls -l substack_paw?_??.fits | \
            tr -s ' ' | cut -d ' ' -f5,5 | sort -u | wc -l)
        if [ $ns -gt 1 ]; then 
            ec "PROBLEM: substacks not all of same size .... "
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
            ec "ATTN: found warnings in pswarp logfiles"
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
        rm -f pmerge.??? pmerge.sh
        ls -1 substack_paw?_??.fits > pmerge.lst
        nsubstacks=$(cat pmerge.lst | wc -l)
        if [ $nsubstacks -eq 0 ]; then
            "ERROR: no substacks found - quitting"; exit 2
        fi

        qfile=$WRK/"pmerge.sh"; touch $qfile; chmod 755 $qfile
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
            njobs=$(ls $WRK/pmerge.out 2> /dev/null | wc -l) 
            #;  ec " DEBUG: njobs: $njobs"
            [ $njobs -eq 1 ] && break                   # jobs finished
            sleep 20
        done  
        ec "# pmerge finished - now check exit status"
        chmod 644 $WRK/pmerge.out
        
        ngood=$(tail -1 $WRK/pmerge.out | grep STATUS:\ 0 | wc -l)
        if [ $ngood -ne 1 ]; then
            ec "PROBLEM: pmerge.sh exit status not 0 ... check pmerge.out"
            askuser
        fi
          
        ec "# CHECK: pmerge.sh exit status ok ... continue"
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
elif [ $1 = 'p3' ]; then      # P3: build object masks, add CASU sky, find and remove best sky 
#-----------------------------------------------------------------------------------------------
#    echo "### QUIT HERE FOR NOW ###" ; exit 0       

#% P3: add CASU sky then determine and remove best sky 
#% - first add casu sky to wircam images to build with_sky images
#% - then build masks using current wircam images and 1st pass stack
#%   . now replace wircam images with with_sky images
#% - determine good skies and subtract, 
#%   . then do add'l clening: bgd removal and destriping
#%------------------------------------------------------------------

    # for DR5, use DR4 products as input for building local obMasks
    stout=UVISTA-DR4-RC2a_${FILTER}_full_lr     # name of pass1 stack w/o .fits extension (low res)
    stout_flag=${stout%.fits}_obFlag.fits  # and the object flag

    ncurr=3 ; pcurr="p$ncurr" ; pprev=$(($ncurr-1)) ; pnext="p$(($ncurr+1))"
    mycd $WRK/images

    # check that P2 finished properly
    nimages=$(ls -1L origs/v20*_0????.fits 2> /dev/null | wc -l)  
    nlist=$(cat list_images | wc -l)

    if [ $nimages -gt 0 ]; then 
        ec "CHECK:  found $nimages image files like: " 
        ecn " ==> "; ls -lh origs/v20*_0????.fits | head -1 | tr -s ' ' | cut -d' ' -f9-12
    else
#        ec "!!! BIG PROBLEM: No image files found, or links not valid"
#        ls -lh v20*_0????.fits | head -1  ; askuser
        ec "# Build links to images "
        ln -sf origs/v20*_?????.fits . 
#       ln -sf heads/v20*_?????.head . 
#       ln -sf weights/v20*_?????_weight.fits . 
        nimages=$(ls -1L v20*_0????.fits 2> /dev/null | wc -l)  
    fi
            
    if [ $nimages -ne $nlist ]; then
        ec "CHECK: list images contains $nlist files, != $nimages ... "; askuser
    fi

    ## build links to dr4 final products:
    #dr4=/n05data/UltraVista/RC2a
    #ln -sf $dr4/$stout.fits 
    #ln -sf $dr4/${stout%.fits}_weight.fits .

    p1stack=$(ls -1L ${stout}.fits ${stout}_weight.fits ${stout_flag}  2> /dev/null| wc -l)
    if [ $p1stack -eq 3 ]; then
        ec "CHECK: found expected p1 stack products ... "
    else 
        ec "PROBLEM: need the pass1 stacks to proceed ... quitting "
        ec " ... files like $stout.fits"; exit 1
    fi
    ec "# Looks like it's ok to continue ... " 
    ec "#-----------------------------------------------------------------------------"

    # ----------------------  Finished checking  ----------------------

    # build zeroes files: last valid bpm file
#   ln -sf $bpmdir/bpm*.fits .   # link to bpms
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

    nmsk=$(ls -1 Masks/v20*_mask.fits 2> /dev/null | wc -l )
    nl=$(ls Masks/mkMasks_*.log 2> /dev/null| wc -l)           #;    echo $nimages $nmsk

    if [ $nl -ge 1 ] && [ $nmsk -ge $nimages  ]; then
        ec "CHECK: found $nl mkMasks logfile and $nmsk _mask files ... "
        ec "CHECK: ===> skip to next step"
        ec "#-----------------------------------------------------------------------------"
    else     
        rcurr=R11

        if [ -e list_missing ]; then
            list=list_missing;  nimages=$(cat $list | wc -l)
        else
            list=list_images
        fi

        ec "## - R11: mkMasks.sh: build sky-subtraction masks for $nimages images "
        ec "#-----------------------------------------------------------------------------"
        rm -f qall estats mkMasks_??.??? mkMasks_??.sh  

        nexp=100
        nts=$(( $nimages/$nexp ))

#        if [ $nts -lt 55 ]; then nts=55; fi
        if [ $nimages -lt 210 ]; then 
            nts=9 
            nexp=$(( $nimages/$nts ))
        fi  # for testing
        split -n l/$nts $list --additional-suffix='.lst' mkMasks_
        ec "# split into $nts chunks of about $nexp images"

        for l in mkMasks_??.lst; do
            id=$(echo $l | cut -d\_ -f2 | cut -d\. -f1) 
            qfile=mkMasks_${id}.sh  ; touch $qfile; chmod 755 $qfile
            sed -e 's|@NODE@|'$node'|'  -e 's|@LIST@|'$l'|'  -e 's|@DRY@|0|' \
                -e 's|@IDENT@|'$PWD/mkMasks_$id'|'   -e 's|@FILTER@|'$FILTER'|' \
                -e 's|@ID@|'$id'|'  -e 's|@WRK@|'$WRK'|'  \
                $bindir/mkMasks.sh > $qfile
            
            ec "# Built $qfile with $(cat $l | wc -l) entries"
            echo "qsub ${qfile}; sleep 4 " >> qall
        done  
        ec "# ==> written to file 'qall' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi

        ec "# Submit qsub files ... ";  source qall
        ec " >>>>   wait for $nts mkMasks jobs ... first check in 1 min  <<<<<"
        
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        while :; do             # qsub wait loop 
            njobs=$(qstat -au moneti | grep Masks_${FILTER}_ | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished

            runjobs=$(qstat -au moneti | grep Masks_${FILTER}_ | grep \ R\  | wc -l)
            ndone=$(ls -1 /scratch??/mkMasks_*/v20*_0????_mask.fits v20*_0????_mask.fits 2> /dev/null | wc -l)
            ntodo=$(($nimages - $ndone)) ; nsec=$((5*$ntodo)) 
            if [ $nsec -le 60 ];   then nsec=60; fi            # min: 1 min
            if [ $nsec -ge 3600 ]; then nsec=3600; fi          # max: 1 hr
            nmin=$(echo "scale=1; $nsec/60" | bc); nhrs=$(echo "scale=1; $nmin/60" | bc)
            if [ $nsec -gt 5400 ]; then wmsg="$nhrs hr"; else wmsg="$nmin min"; fi
            ec "# $runjobs jobs running; $ndone _mask files done, $ntodo remaining - next check in $wmsg "
            sleep $nsec
        done  
        ec "# mkMasks finished; walltime $(wtime) - check results ..."
        
        grep EXIT\ STATUS mkMasks_??.out >> estats
        nbad=$(grep -v \ 0 estats | wc -l)
        if [ $nbad -gt 0 ]; then
            ec "PROBLEM: some mkMasks's exit status not 0 ... "
            grep -v \ 0  estats;    askuser
        else
            ec "CHECK: mkMask.sh exit status ok ... continue"; rm -f estats qall
        fi
        # check number of files produced:
        nnew=$(ls -1 v20*_mask.fits | wc -l)
        if [ $nnew -lt $nimages ]; then
            ec "CHECK: found only $nnew mask files; $(($nimages-$nnew)) missing...."
            askuser
        else
            ec "CHECK: found all $nnew expected mask files "
        fi
        
        # check for amonalies in masks: in mkMasks_??.dat
        rm -f mkMasks.dat                # clean up before building it
        cat mkMasks_??.dat > mkMasks.dat
        nn=$(grep '\ 0\.00\ ' mkMasks.dat | wc -l)
        if [ $nn -gt 0 ]; then
            ec "PROBLEM: $nn files with one or more chips fully masked: check mkMasks_??.dat"
            askuser
        fi

        # mv new _mask files to Masks dir for safekeeping; and make links 
        # ... needed for updateWeights later
        if [ ! -d Masks ]; then mkdir Masks; fi
        mv v20*_mask.fits mkMasks_*.* Masks 
        chmod 444 Masks/v20*_mask.fits

        ec "# mkMasks checks complete; no problem found ... continue" 
        ec "#-----------------------------------------------------------------------------"
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
    
    #----------------------------------------------------------------------------------------------#
    #       Add casu sky
    #----------------------------------------------------------------------------------------------#
    # Done locally; links to origs files are created, sky is added, results is _withSky
    # Links to the sky and bpm images are made locally and deleted when done
    # T
    # When done the _withSky files are moved to withSky dir, together with scripts, logs, etc.
    # 
    #----------------------------------------------------------------------------------------------#

    list=list_images
    nwsky=$(ls -1 withSky/v20*withSky.fits 2> /dev/null | wc -l )  
    if [ -e addSky.log ] && [ $nwsky -ge $nimages  ]; then
      ec "CHECK: found addSky.log ... and $nwsky withSky images ... "
      ec "CHECK: ===> skip to next step"
      ec "#-----------------------------------------------------------------------------"
    else     
        rcurr=R12

        if [ -e list_missing ]; then
            list=list_missing;  nimages=$(cat $list | wc -l)
        else
            list=list_images
        fi

        ec "## - R12: addSky.sh: add CASU sky to $nimages images in $list "
        ec "#-----------------------------------------------------------------------------"
        rm -f qall  addSky_??.??? addSky_??.sh   

        sky1=$(ls -1 $WRK/calib/sky*.fits | head -1)
        nn=$(ls v20*_?????.fits 2> /dev/null | wc -l)  
        nl=$(cat list_images | wc -l)
        if [ $nn -ne $nl ]; then
            ec "==> Found only $nn image files of $nl expected ..."
            ec "==> Build links to image files"
            ln -sf origs/v20*_?????.fits .
        fi

        # Build links to sky and bpm files
        rm -f sky_*.fits bpm_*.fits
        ec "# Build links to sky files."; ln -s $WRK/calib/sky_*.fits . 
        ec "# Build links to bpm files."; ln -s $DR5/bpms/bpm_*.fits .
       
        # split the list into chunks of max 1200 images, normally doable in 32 hrs hr:
        if [ $nimages -lt 220 ]; then nts=5; else nts=12; fi
        for f in $(cat $list); do grep $f ../FileInfo.dat >> fileinfo.tmp; done
        split -n l/$nts fileinfo.tmp --additional-suffix='.lst' addSky_
        rm fileinfo.tmp
        
        for l in addSky_??.lst; do
            id=$(echo $l | cut -d\_ -f2 | cut -d\. -f1) 
            qfile=addSky_${id}.sh  ; touch $qfile; chmod 755 $qfile
            sed -e 's|@NODE@|'$node'|'  -e 's|@LIST@|'$l'|'  -e 's|@DRY@|'$dry'|' \
                -e 's|@IDENT@|'$PWD/addSky_$id'|'   -e 's|@FILTER@|'$FILTER'|' \
                -e 's|@ID@|'$id'|'  -e 's|@WRK@|'$WRK'|'  $bindir/addSky.sh > $qfile
            
            ec "# Built $qfile with $(cat $l | wc -l) entries"
            echo "qsub $qfile" >> qall
        done   
        ec "# ==> written to file 'qall' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec "# Submit qsub files ... ";  source qall
        ec " >>>>   wait for $nts addSky jobs ... first check in 1 min  <<<<<"
        
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        while :; do             # qsub wait loop for addSky
            njobs=$(qstat -au moneti | grep addSky_${FILTER} | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished

            runjobs=$(qstat -au moneti | grep addSky_${FILTER} | grep \ R\  | wc -l)
            ndone=$(ls -1 v20*_0????_withSky.fits | wc -l)
            ntodo=$(($nimages - $ndone)) ; nsec=$((3*${ntodo}/2)) 
            if [ $nsec -le 60 ];   then nsec=60; fi            # min: 1 min
            if [ $nsec -ge 3600 ]; then nsec=3600; fi          # max: 1 hr
            nmin=$(echo "scale=1; $nsec/60" | bc); nhrs=$(echo "scale=1; $nmin/60" | bc)
            if [ $nsec -gt 5400 ]; then wmsg="$nhrs hr"; else wmsg="$nmin min"; fi
            ec "# $runjobs jobs running; $ndone withSky images done, $ntodo remaining - next check in $wmsg  " 
            sleep $nsec
        done  
        ec "# addSky finished; walltime $(wtime) - check results ..."
        
        rm -f estats         # before filling it
        for l in addSky_??.out; do tail -1 $l >> estats; done
        nbad=$(grep -v STATUS:\ 0  estats | wc -l)
        if [ $nbad -gt 0 ]; then
            ec "PROBLEM: some addSky's exit status not 0 ... "
            grep -v \ 0  estats;    askuser
        else
            ec "CHECK: addSky.sh exit status ok ... continue"; rm -f estats qall
        fi
        # check number of files produced:
        nnew=$(ls -1 v20*_withSky.fits | wc -l)
        if [ $nnew -lt $nimages ]; then
            ec "CHECK: found only $nnew withSky files; $(($nimages-$nnew)) missing...."
            askuser
        fi
        
        # build general log file:
        grep on\ v20 addSky_??.out | cut -d\: -f2 > addSky.log
        chmod 644 v20*withSky.fits addSky*.???

        # clean up:
        if [ ! -d withSky ]; then mkdir withSky; fi 
        mv v20*withSky.fits withSky  ;  chmod 444 withSky/v20*_sky.fits
        mv addSky_??.* withSky
        rm sky_20*.fits bpm*[0-9].fits    # links to ../calib/sky and bpm files

        ec "# addSky checks complete; no problem found ... continue" 
        ec "#-----------------------------------------------------------------------------"
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi


    #----------------------------------------------------------------------------------------------#
    #       Compute good sky (mkSky)
    #----------------------------------------------------------------------------------------------#
    # Don't need links to withSky images here (in images/) as work is done in subdirs and direct
    # links are created there.  
    # Inputs:  withSky images, heads, masks, weights
    # Outputs:
    #----------------------------------------------------------------------------------------------#

    nn=$(ls -1 mkSky_??.log 2> /dev/null | wc -l)
    ns=$(ls v20*_sky.fits 2> /dev/null | wc -l) #; echo $nn $ns
    if [ $nn -ge 1 ] && [ $ns -ge 5 ]; then
        ec "CHECK: Found $nn mkSky_xx.log files and $ns _sky.fits files for $nimages images. "
        ec "CHECK: ====> proper skies already built - skip to next step"
        ec "#-----------------------------------------------------------------------------"
    else     
        rcurr=R13

        if [ -e list_missing ]; then
            list=list_missing;  nimages=$(cat $list | wc -l)
        else
            list=list_images
        fi

        ec "## - R13: mkSky.sh: determine and subtract good sky from $nimages images"    
        ec "#-----------------------------------------------------------------------------"
        nout=$(ls -1 mkSky_??.out 2> /dev/null | wc -l)
        nlog=$(ls -1 mkSky_??.log 2> /dev/null | wc -l)
        if [ $nout -ge 1 ] || [ $nlog -ge 1 ]; then
            ec "ATTN: found $nout mkSky_??.out and $nlog mkSky_??.log files ... delete them and continue??"
            askuser
        fi
        rm -f qall mkSky_??.lst mkSky_??.skylst mkSky_??.sh   mkSky_??.out mkSky_??.log
        # Don't need links to withSky images here (images/): mkSky work is done in 
        # subdirs and needed links are created there. 
            
        nexp=99
        #ec "# split into chunks of max $nexp images, normally doable in 32 hrs"
        nts=$(echo "$nimages / $nexp + 1" | bc)

        if [ $nts -lt 24 ]; then nts=24; fi
        if [ $nimages -lt 150 ]; then nts=5; fi
        split -n l/$nts $list --additional-suffix='.lst' mkSky_

        # build sublists with images from which to choose skies:
        if [ ! -e list_missing ]; then      # ok when list is "continuous"
            ec "# Build skylists using full list of images..."
            ls -1 mkSky_??.lst > srclist   # list of lists
            for f in $(cat srclist); do
                e=$(grep -B1 $f srclist | head -1) #; echo $e
                g=$(grep -A1 $f srclist | tail -1) #; echo $g
                olist=${f%.lst}.skylst
                ec "# Build $olist for $f"
                
                if [ "$e" == "$f" ]; then 
                    cat $f       > $olist
                    head -20 $g >> $olist
                elif [ "$f" == "$g" ]; then 
                    tail -20 $e  > $olist
                    cat $f      >> $olist
                else 
                    tail -20 $e  > $olist
                    cat $f      >> $olist
                    head -20 $g >> $olist
                fi
            done
        else    # look for nearby frames in full list.
            ec "# Build skylsts using partial list ..."
            for l in mkSky_??.lst; do
                olist=${l%.lst}.skylst
                rm -rf tmplist $olist
                for f in $(cat $l); do grep $f -A20 -B20 list_images >> tmplist; done
                sort -u tmplist > $olist
            done
        fi

        ec "#-----------------------------------------------------------------------------"
        # build ad-hoc shell scripts
        for l in mkSky_??.lst; do
            id=$(echo $l | cut -d\_ -f2 | cut -d\. -f1) 
            qfile=mkSky_${id}.sh  ; touch $qfile; chmod 755 $qfile
            sed -e 's|@NODE@|'$node'|'  -e 's|@LIST@|'$l'|'  -e 's|@DRY@|0|' \
                -e 's|@IDENT@|'$PWD/mkSky_$id'|'   -e 's|@FILTER@|'$FILTER'|' \
                -e 's|@ID@|'$id'|'  -e 's|@WRK@|'$WRK'|'  \
                $bindir/mkSky.sh > $qfile
            
            ec "# Built $qfile with $(cat $l | wc -l) entries"
            echo "qsub ${qfile} ; sleep 1" >> qall
        done  
        ec "#-----------------------------------------------------------------------------"
        ec "# ==> written to file 'qall' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec "# Submit qsub files ... ";  source qall
        ec " >>>>   wait for $nts mkSky jobs ... first check in 1 min  <<<<<"
        
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        nimages=$(cat list_images | wc -l)
        while :; do             # qsub wait loop for mkSky
            njobs=$(qstat -au moneti | grep mkSky_${FILTER}_ | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished
            
            runjobs=$(qstat -au moneti | grep mkSky_${FILTER}_ | grep \ R\  | wc -l)
            ndone=$(ls -1 /scratch??/mkSky_??_*/v20*_sky.fits v20*_sky.fits 2> /dev/null | wc -l)
            ntodo=$(($nimages - $ndone)) ; nsec=$((6*$ntodo))
            if [ $nsec -le 60 ];   then nsec=60; fi            # min: 1 min
            if [ $nsec -ge 3600 ]; then nsec=3600; fi          # max: 1 hr
            nmin=$(echo "scale=1; $nsec/60" | bc); nhrs=$(echo "scale=1; $nmin/60" | bc)
            if [ $nsec -gt 5400 ]; then wmsg="$nhrs hr"; else wmsg="$nmin min"; fi
            ec "# $runjobs jobs running; $ndone skies done, $ntodo remaining - next check in $wmsg "
            sleep $nsec
        done  
        ec "# mkSky finished; walltime $(wtime) - check results ..."
        
        # ---------------------- check products ----------------------

        rm -f estats         # before filling it
        grep STATUS:\  mkSky_??.out | grep -v \ 0 > estats
        nbad=$(grep -v STATUS:\ 0  estats | wc -l)
        if [ $nbad -gt 0 ]; then
            ec "PROBLEM: some mkSky's exit status not 0 ... "
            cat estats;    askuser
        else
            ec "CHECK: mkSky.sh exit status ok ... continue"; rm -f estats qall
        fi

        ### handle frames which have insuffient nearby frames to determine sky ###
        grep CHECK: mkSky_??.log > mkSky.log
        grep skip mkSky.log | cut -d\: -f3 | tr -d ' '  > list_noSky
        nn=$(cat list_noSky | wc -l)
        if [ $nn -ge 1 ]; then
            ec "# Found $nn images with insufficient neighbours to build sky, see list_noSky"
            for f in $(cat list_noSky); do rm ${f%.fits}*.* ; done   # these are links
            echo "# $nn files with too few neighbours to build sky" >> $badfiles
            cat list_noSky >> $badfiles
            ec "# ... removed them (links) and added names to $badfiles"
            ls -1 v20*_0????.fits > list_images
            nimages=$(cat list_images | wc -l)
            ec "# ==> $nimages valid images left."
        else
            ec "# Found NO images with insufficient neighbours to build sky ... WOW!"
        fi

        ### check number of valid files produced:
        nnew=$(ls -1 v20*_sky.fits | wc -l)
        if [ $nnew -lt $nimages ]; then
            ec "CHECK: found only $nnew _sky files; $(($nimages-$nnew)) missing...."
            ls v20*_sky.fits | sed 's/_sky//' > list_done
            comm -23 list_images list_done > list_missing
            askuser
        fi

        chmod 644 v20*_sky.fits mkSky_??.???

        ### cleanup and make links
        if [ ! -d mkSky ]; then mkdir mkSky; fi
        mv mkSky_??.* mkSky
        # - leave logfiles - used to check if mkSky already done
        # - leave the _sky files for updateWeights; move afterwards to mkSky

        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
    

    #----------------------------------------------------------------------------------------------#
    #       Update weights: set _weight to 0 where no sky was determined
    #----------------------------------------------------------------------------------------------#
    # Inputs: weight.fits file and _sky.fits files; 
    # Output: new weight.fits, with unseen pixels (0 in _sky.fits) are also set to zero
    # 
    #----------------------------------------------------------------------------------------------#

    if [ -s updateWeights.log ]; then
        ec "CHECK: found updateWeights.log file ..."
        ec "CHECK: ====> weight files already updated - skip to next step"
    else     
        rcurr=R14
        nim=$(cat list_images | wc -l)
        ec "## - R14: updateWeights.sh: to exclude area where no sky is calculated for $nim images"  
        ec "#-----------------------------------------------------------------------------"
        nout=$(ls -1 updateWeights_??.out 2> /dev/null | wc -l)
        nlog=$(ls -1 updateWeights_??.log 2> /dev/null | wc -l) # ; echo $nout $nlog
        if [ $nout -ge 1 ] || [ $nlog -ge 1 ]; then
            ec "ATTN: found $nout updateWeights_??.out and $nlog update_weights_??.log files ... delete them and continue??"
            askuser
        fi
        
        # remove old files if any
        rm -f qall estats updateWeights_??.out  updateWeights_??.sh
        # build links to weight and mask files
        ln -sf Masks/v20*_mask.fits .
        ln -sf weights/v20*_weight.fits .

        if [ $nimages -lt 50 ]; then nts=4; else nts=12; fi
        split -n l/$nts list_images --additional-suffix='.lst' updateWeights_
        
        for l in updateWeights_??.lst; do
            id=$(echo $l | cut -d\_ -f2 | cut -d\. -f1) 
            qfile=updateWeights_${id}.sh  ; touch $qfile; chmod 755 $qfile
            sed -e 's|@NODE@|'$node'|'  -e 's|@LIST@|'$l'|'  -e 's|@DRY@|0|' \
                -e 's|@IDENT@|'$PWD/updateWeights_$id'|'   -e 's|@FILTER@|'$FILTER'|' \
                -e 's|@ID@|'$id'|'  -e 's|@WRK@|'$WRK'|'  \
                $bindir/updateWeights.sh > $qfile
            
            ec "# Built $qfile with $(cat $l | wc -l) entries"
            echo "qsub $qfile" >> qall
        done   
        ec "# ==> written to file 'qall' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec "# Submit qsub files ... ";  source qall
        ec " >>>>   wait for $nts updateWeights jobs ... first check in 1 min  <<<<<"
        
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        while :; do             # qsub wait loop for updateWeights
            njobs=$(qstat -au moneti | grep upWgts_${FILTER} | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished
 
            # No obvious way to monitor progress ... thus just wait ... pretty fast anyway           
            sleep 150
        done  
        ec "# updateWeights jobs  finished; walltime $(wtime) - check results ..."
        
        # ---------------------- check products ----------------------

#       ndone=$(ls -1 updateWeights_??.out | wc -l)   # number of .out files found
        grep EXIT\ STATUS updateWeights_??.out > estats
        nbad=$(grep -v STATUS:\ 0 estats | wc -l)
        if [ $nbad -gt 0 ]; then
            ec "PROBLEM: some updateWeights's exit status not 0 ... "
            grep -v \ 0  estats;    askuser
        else
            ec "CHECK: updateWeights.sh exit status ok ... continue"; rm -f qall
        fi
        
        # join the logfiles - contains number of new pixels masked for each frame
        cat updateWeights_??.log > updateWeights.log

        chmod 644 updateWeights_??.???
        # cleanup
        if [ ! -d UpdateWeights ]; then mkdir UpdateWeights; fi
        mv updateWeights_??.* UpdateWeights

        # mv _sky images to mkSky dir
        mv v20*_sky.fits mkSky
       
        # rm all links to images, weights, etc
        rm v20*_0????.fits v20*_weight.fits v20*_mask.fits zeroes.fits srclist # v20*.head
#        ec "#-----------------------------------------------------------------------------"
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi

    cd $WRK  
    if [ $pauto == 'T' ]; then                                 # END P3
        ec "#-----------------------------------------------------------------------------"      
        ec "# $pcurr finished; ==> auto continue to $pnext"
        ec "#-----------------------------------------------------------------------------"      
        $0 $pnext $pauto
    else 
        ec "#-----------------------------------------------------------------------------"
        ec "#                                End of $pcurr "
        ec "#-----------------------------------------------------------------------------"
    fi 


#-----------------------------------------------------------------------------------------------
elif [ $1 = 'p4' ]; then      # P4: subsky, destripe and bild final stack
#-----------------------------------------------------------------------------------------------

    ncurr=4 ; pcurr="p$ncurr" ; pprev=$(($ncurr-1)) ; pnext="p$(($ncurr+1))"
    mycd $WRK/images
    
    nims=$(ls -1 origs/v20*_0????.fits | wc -l)
    nimages=$(cat list_images | wc -l)

    ec "CHECK: found $nimages images"
    
    # ----------------------  Finished checking  ----------------------

    #----------------------------------------------------------------------------------------#
    #       Actual sky subtraction, destriping, and large-scale bgd removal
    #----------------------------------------------------------------------------------------#
    # Requires: _withSky, _sky _mask images
    # Produces _clean files
    # 
    #----------------------------------------------------------------------------------------#

    nn=$(ls cleaned/v20*_clean.fits 2> /dev/null | wc -l)

    if [ -e subSky.log ] && [ $nn -eq $nimages ]; then 
        ec "CHECK: Found $nn clean images in and subSky.log files "
        ec "CHECK: ====> skip to next step "
        ec "#-----------------------------------------------------------------------------"
    else     
        rcurr=R15
        ec "## - R15:  subSky.sh: pure sky-sub, destriping, and lsb-cleaning of $nimages images "
        ec "#-----------------------------------------------------------------------------"
        nout=$(ls -1 subSky_??.out 2> /dev/null | wc -l)
        nlog=$(ls -1 subSky_??.log 2> /dev/null | wc -l) # ; echo $nout $nlog
        if [ $nout -ge 1 ] || [ $nlog -ge 1 ]; then
            ec "ATTN: found $nout subSky_??.out and $nlog subSky_??.log files ... delete them and continue??"
            askuser
        fi
        rm -f qall estats subSky_??.??? subSky_??.sh

        # check (again?) the masks
        grep '\ 0.00' mkMasks.dat > badMasks.dat
        nn=$(cat badMasks.dat | wc -l)
        if [ $nn -gt 0 ]; then
            ec "PROBLEM: some masks contains chips that are entirely masked ... see badMasks.dat "
            askuser
        else
            ec "CHECK: _mask files ok: no fully masked chips ... continue "
            rm -f badMasks.dat
        fi

		if [ -s list_special ]; then 
			list=list_special
			ec "#### ATTN: Using special list"
		else
			list=list_images
		fi
		nimages=$(cat $list | wc -l)
		
        ec "# ==> Build ad-hoc processing scripts from $list:"
        rate=10         # typical num images processed / hr
        nexp=$((2*$rate/3 * 18)) # number expected per process in 32 hrs
        nts=$(echo "$nimages / $nexp + 1" | bc) ##;     echo "$nimages $nexp $nts"

        if [ $nts -le 5 ]; then nts=2; fi      # to avoid few very long lists
        if [ $nimages -lt 50 ]; then nts=5; fi  # for testing
        split -n l/$nts $list --additional-suffix='.lst' subSky_
        
        for l in subSky_??.lst; do
            nl=$(cat $l | wc -l)
            if [ $nl -ge 1 ]; then
                id=$(echo $l | cut -d\_ -f2 | cut -d\. -f1) 
                qfile=subSky_${id}.sh  ; touch $qfile; chmod 755 $qfile
                sed -e 's|@NODE@|'$node'|'  -e 's|@LIST@|'$l'|'  -e 's|@DRY@|0|' \
                    -e 's|@IDENT@|'$PWD/subSky_$id'|'   -e 's|@FILTER@|'$FILTER'|' \
                    -e 's|@ID@|'$id'|'  -e 's|@WRK@|'$WRK'|'  \
                    $bindir/subSky.sh > $qfile
            
                ec "# Built $qfile with $nl entries"
				echo "qsub $qfile; sleep 1" >> qall
            else
                ec "ATTN: list $l empty ..."
            fi
        done   
		nts=$(cat qall | wc -l)
        ec "# ==> written to file 'qall' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec "# Submit qsub files ... ";  source qall
        ec " >>>>   wait for $nts subSky jobs ... first check in 1 min  <<<<<"
        
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        while :; do             # qsub wait loop for subSky
            ndone=$(ls $WRK/images/subSky_??.out 2> /dev/null | wc -l)
            [ $ndone -eq $nts ] && break          # jobs finished
            
            runjobs=$(qstat -au moneti | grep subSky_${FILTER}_ | grep \ R\  | wc -l) 
            idone=$(ls -1 /scratch??/subSky_*${FILTER}/v20*_0????_clean.fits $WRK/images/subSky_*/v20*_0????_clean.fits 2> /dev/null | wc -l)
            ntodo=$(($nimages - $idone)) ; nsec=$((2*$ntodo))
            if [ $nsec -le 60 ];   then nsec=60; fi            # min: 1 min
            if [ $nsec -ge 3600 ]; then nsec=3600; fi          # max: 1 hr
            nmin=$(echo "scale=1; $nsec/60" | bc); nhrs=$(echo "scale=1; $nmin/60" | bc)
            if [ $nsec -gt 5400 ]; then wmsg="$nhrs hr"; else wmsg="$nmin min"; fi
            ec "# $runjobs jobs running; $idone images processed, $ntodo remaining - next check in $wmsg "
            sleep $nsec
        done  
        ec "# subSky jobs  finished; walltime $(wtime) - check results ..."
        
        # ---------------------- check products ----------------------

        grep EXIT\ STATUS subSky_??.out >> estats
        nbad=$(grep -v STATUS:\ 0  estats | wc -l)
        if [ $nbad -gt 0 ]; then
            ec "PROBLEM: some subSky's exit status not 0 ... "
            grep -v \ 0  estats;    askuser
        else
            ec "CHECK: subSky.sh exit status ok ..."; rm -f estats qall
        fi

        # join subSky*.log files
        grep -e Begin\ work -e output subSky*??*.log > subSky.log
        
        # check number of files produced:
        nnew=$(ls -1 v20*_clean.fits | wc -l)
        if [ $nnew -lt $nimages ]; then
            ec "CHECK: found only $nnew mask files; $(($nimages-$nnew)) missing...."
            askuser
        fi

        chmod 644 v20*_*.fits subSky_??.???

        if [ ! -d cleaned ]; then mkdir cleaned; fi
        mv v20*_clean.fits subSky_??.* cleaned            # and other subSky files
        # leave subSky.log to check if done
        ec "CHECK: subSky.sh done, clean images moved to cleaned/ and linked ... continue"
        ec "#-----------------------------------------------------------------------------"
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
 

    #----------------------------------------------------------------------------------------#
    #       swarp - p2
    #----------------------------------------------------------------------------------------#
    #   For each paw swarp is run in a separate, temprary directory.  The links to the data
    # files and other control files are created directly there by the wrapper script.  At the
    # end the products are copied to images/  
    #   A first run (of swarp and merge) is done to build the low-res stack, and a second one
    # follows automatically for the high-res one.  When done, it's the end of the pipeline.
    #----------------------------------------------------------------------------------------#

    pass=2
    # update paw lists?
    npaws=$(ls list_paw? 2> /dev/null | wc -l)  

    if [ $npaws -ge 3 ]; then
        nn=$(wc list_paw? 2> /dev/null | grep total | tr -s ' ' | cut -d' ' -f2 )
        #ec "# Found $npaws paws with a total of $nn images"
    else
        ec "# No paw lists found or lists out of date ... (re)build them from list_images"
        file=$(mktemp)
        paws=" paw1 paw2 paw3 paw4 paw5 paw6 COSMOS"
        for i in $(cat list_images); do grep $i ../FileInfo.dat >> $file; done
        for p in $paws; do grep $p $file | cut -d \   -f1 > list_${p}; done
        rm $file

        # if present, convert to paw0
        if [ -e list_COSMOS ]; then mv list_COSMOS list_paw0; fi

        # remove empty lists
        for f in list_paw?; do if [ ! -s $f ]; then rm $f; fi; done

        npaws=$(ls list_paw? 2> /dev/null | wc -l)
        ec "# Built $npaws paw lists as follows:"
        wc -l list_paw? 
    fi

    # check if swarp alreay done:
    nsubima=$(ls -1 substack_paw?_??.fits 2> /dev/null | wc -l)   

    stout=UVISTA_${REL}_p2_lr   # output name of low-res pass 2 stack w/o .fits extension
    # NB. if if low-res stack already there, then do the high res (cosmos) version 
    if [ -e ${stout}.fits ]; then stout=${stout%lr}hr ; fi  

    # define the name of the directory for substacks and other files, and created it
    if [[ $stout =~ 'lr' ]]; then 
        resol=lr
    else
        resol=hr
    fi
    prod_dir=swarp_p2_$resol
    if [ ! -d $prod_dir ]; then mkdir $prod_dir; fi
	# see if there are the expected number of scripts there
	nscripts=$(ls $prod_dir/pswarp2*.sh 2> /dev/null | wc -l)

          
    if [[ $nsubima -eq $nscripts  &&  $nsubima -gt 0 ]] || [ -e $stout.fits ]; then 
		if [ ! -e $stout.fits ]; then 
			ec "CHECK: Found $nsubima substacks for $nscripts expected - swarp done ..."
		else 
			ec "CHECK: Found $stout.fits - Nothing to doe!!! "
		fi
        ec "CHECK: ==> skip to next step "
        ec "#-----------------------------------------------------------------------------"
    else 
        rcurr=R16
        ec "## - R16:  pswarp.sh: swarp pass2 for $npaws paws, $nn images ... by sub-paws "
        ec "#-----------------------------------------------------------------------------"
        nout=$(ls -1 pswarp2_??.out 2> /dev/null | wc -l)
        nlog=$(ls -1 pswarp2_??.log 2> /dev/null | wc -l)
        if [ $nout -ge 1 ] || [ $nlog -ge 1 ]; then
            ec "ATTN: found $nout pswarp2_??.out and $nlog pswarp2_??.log files ... delete them and continue??"
            askuser
        fi
        rm -f qall estats pswarp2_paw_??.lst  pswarp2_paw_??.sh     # just in case
		rm pswarp2_paw_??.out  pswarp2_paw_??.log  
        
        # if no p2 stack, then do one at low res, 
        if [[ $stout =~ "lr" ]]; then
            ec "# First build low-res (0.6 arcsec/pix) p2 stack ..."
            #headfile=firstpass.head
            headfile=cosmos_lr.head   #std1G.head
        else
            ec "# Found low-res p2 stack ... build stack high-res (0.3 arcsec/pix) one"
            #headfile=cosmos.head
            headfile=cosmos_hr.head   #std4G.trim.head
        fi

        subsky=N                             # for pass2 DO NOT subtract sky

        ec "#-----------------------------------------------------------------------------"
        ec "# output:    $stout"
        ec "# head-file: $headfile"
        ec "# subsky:    $subsky"
        ec "#-----------------------------------------------------------------------------"

        nim=450  # approx num of images in each sublist
        for list in list_paw[0-9]; do  
            nl=$(cat $list | wc -l)
            ppaw=$(echo $list | cut -d\_ -f2)       # NEW tmporary name for full paw
            split -n l/$(($nl/$nim+1)) $list --additional-suffix='.lst' pswarp2_${ppaw}_
            for slist in pswarp2_${ppaw}_??.lst; do
                nl=$(cat $slist | wc -l)    
                paw=$(echo $slist | cut -d\_ -f2-3 | cut -d\. -f1)   
                outname=substack_${paw}
                #ec "DEBUG:  For paw $paw, $nl images ==> $outname with subsky $subsky"
            
                # ---------------------- Local run by sublist ----------------------
                
                qfile="pswarp2_$paw.sh"; touch $qfile; chmod 755 $qfile
                sed -e 's|@NODE@|'$node'|'  -e 's|@IDENT@|'$PWD/pswarp2'|'  -e 's|@DRY@|'$dry'|'  \
                    -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'$slist'|'  -e 's|@WRK@|'$WRK'|' \
                    -e 's|@PAW@|'$paw'|'  -e 's|@PASS@|'$pass'|'  -e 's|@HEADFILE@|'$headfile'|'                     \
                    -e 's/@SUBSKY@/'$subsky'/'  $bindir/pswarp.sh > $qfile
            
                ec "# Built $qfile with $nl images for paw $paw ==> $outname"
                echo "qsub $qfile; sleep 1" >> qall
            done
        done 
        njobs=$(cat qall | wc -l)
        ec "# ==> written to file 'qall' with $njobs entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi

        ec "# Submit qsub files ... ";  source qall
        ec " >>>>   Wait $njobs pswarp jobs ... first check in 1 min  <<<<<"

        ostr="ddd"              # a dummy string for comparisons within the loop
        btime=$(date "+%s.%N");  sleep 60           # begin time
        while :; do           #  begin qsub wait loop for pswarp
            ndone=$(ls $WRK/images/pswarp2_paw?_??.out 2> /dev/null | wc -l)
            [ $njobs -eq $ndone ] && break          # jobs finished

            # check every 30 sec, and if a new substack is done then print this message
			nrun=$(ls -d /scratch??/paw*_$FILTER  $WRK/images/paw*_$FILTER 2> /dev/null | wc -l )
            str=$(ls -lthr substack_paw?_??.fits 2> /dev/null | tr -s ' ' | cut -d' ' -f4-9 | tail -1)
            if [[ $str != $ostr ]]; then echo "status: $nrun running or queued; last substack:  $str " ; fi
            ostr=$str
            sleep 30
        done  
        ec "# pswarp finished; walltime $(wtime)"
        
        # check exit status
        grep EXIT\ STATUS pswarp2_paw?_??.out > estats
        nbad=$(grep -v STATUS:\ 0  estats | wc -l)  # files w/ status != 0
        if [ $nbad -gt 0 ]; then
            ec "PROBLEM: pswarp2_paw?_??.sh exit status not 0: "
            grep -v STATUS:\ 0 estats 
            askuser
        else
            ec "CHECK: pswarp2_paw?_??.sh exit status ok"; rm estats
        fi

        # check num substacks found
        nn=$(ls substack_paw?_??.fits | wc -l)
        if [ $nn -lt $njobs ]; then
            ec "PROBLEM:  found only $nn substacks for $njobs expected ..."
            askuser 
        fi

        # check sizes of sustacks
        ns=$(\ls -l substack_paw?_??.fits | \
            tr -s ' ' | cut -d ' ' -f5,5 | sort -u | wc -l)
        if [ $ns -gt 1 ]; then 
            ec "PROBLEM: substacks not all of same size .... "
            ls -l substack_paw?_??.fits 
            askuser
        fi

        # check for WARNINGS in logfiles
        warn=0
        for f in pswarp2_paw?_??.log; do   # log files not yet renamed
            grep WARNING $f | wc -l > ${f%.log}.warn
            if [ $(wc ${f%.log}.warn | wc -l) -gt 1 ]; then warn=1; fi
        done
        if [ $warn -eq 1 ]; then 
            ec "ATTN: found warnings in pswarp logfiles"
            askuser
        fi

        chmod 644 subst*.* pswarp2*.log pswarp2*.out

        mv -f pswarp2*.sh pswarp2*.warn pswarp2_paw?_??.???  $prod_dir
#        mv -f substack*clip.log substack*.xml                $prod_dir
        rm -f qall substack*.head  # name built in script

        ec "#-----------------------------------------------------------------------------"
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
   
    #----------------------------------------------------------------------------------------#
    #          Merge p2 substacks
    #----------------------------------------------------------------------------------------#


    if [ -e $WRK/images/$stout.fits ]; then 
        ec "#CHECK: stack $stout already built; "
        ec "#       ==> skip to next step"
    else 
        rcurr=R17
        # rm pmerge.* from previous runs
        rm -f pmerge.??? pmerge.sh

        nsubstacks=$(ls -1 substack_paw?_??.fits 2> /dev/null | wc -l)
        if [ $nsubstacks -eq 0 ]; then
            ec "ERROR: no substacks found - quitting"; exit 2
        fi
        
        if [ $nsubstacks -ne $npaws ]; then
            ec "#### ATTN: Number of substacks not same as number of paws"
            askuser
        fi

        ec "## - R17:  pmerge.sh: Merge $nsubstacks substacks into $stout ..."
        ec "#-----------------------------------------------------------------------------"
        ec "# Number of substacks same as $npaws paws"

        ls -1 substack_paw?_??.fits > pmerge.lst

        qfile="pmerge.sh"; touch $qfile; chmod 755 $qfile
        sed -e 's|@NODE@|'$node'|'   -e 's|@IDENT@|'$PWD/pmerge'|'  -e 's|@DRY@|'$dry'|'  \
            -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'pmerge.lst'|'  -e 's|@WRK@|'$WRK'|'  \
            -e 's|@STOUT@|'$stout'|'   -e 's|@PASS@|'$pass'|'  $bindir/pmerge.sh > ./$qfile
        
        ec "# Built $qfile with $nsubstacks entries"
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec "# submitting $qfile ... "; qsub $qfile
        ec " >>>>   wait for pmerge to finish ...   <<<<<"
        
        btime=$(date "+%s.%N");  sleep 60   # before starting wait loop
        while :; do           #  begin qsub wait loop for pmerge
            ndone=$(ls $WRK/images/pmerge.out 2> /dev/null | wc -l)
            [ $ndone -eq 1 ] && break          # jobs finished
            sleep 30
        done  
        ec "# pmerge finished - now check exit status"
        chmod 644 pmerge.out
        
        ngood=$(tail -1 pmerge.out | grep STATUS:\ 0 | wc -l)
        if [ $ngood -ne 1 ]; then
            ec "PROBLEM: pmerge.sh exit status not 0 ... check pmerge.out"
            askuser
        fi

        ec "CHECK: pmerge.sh exit status ok ... continue"
        ec "# $stout and associated products built:"
        ls -lrth UVISTA*p2*${resol}*.*
        ec "# ..... GOOD JOB! "
        mv substack_paw?_??.fits substack_paw?_??_weight.fits pmerge.* $prod_dir
    fi

    if [[ $resol == 'lr' ]]; then
        mycd $WRK
        ec "# Low-res stack done, now begin high-res one"
        $0 p4 auto
    fi

    ec "##------------------------  END OF DR4 PIPELINE ------------------------------"
	ec "##                   Non senza fatiga si giunge al fine "
    ec "#-----------------------------------------------------------------------------"


#-----------------------------------------------------------------------------------------------
#elif [ $1 = 'p7' ]; then      #    -------------- END OF PIPELINE --------------
#-----------------------------------------------------------------------------------------------

#@@ ------------------------------------------------------------------------------------
#@@  And options for status checking
#@@ ------------------------------------------------------------------------------------

elif [[ $1 =~ 'env' ]]; then   # env: check environment
   procenv

elif [[ $1 =~ 'fil' ]]; then   # files: list files for an image in current work space
   curfiles

elif [[ $1 == 'help' ]] || [[ $1 == '-h'  ]] ; then  # help, -h: list pipeline options
    pipehelp

elif [ $1 = 'plists' ]; then  # plists: rebuild paw lists and check

    mycd $WRK/images
    ec " >> Rebuild list_images and list_paw? ..."
    \ls -1 v20*_00???.fits > list_images    
    
    file=$(mktemp)
    paws=" paw1 paw2 paw3 paw4 paw5 paw6 COSMOS"
    for i in v20*_00???.fits; do grep $i ../FileInfo.dat >> $file; done
    for p in $paws; do grep $p $file | cut -d \   -f1 > list_${p}; done
    rm $file

    # if present, convert to paw0
    if [ -e list_COSMOS ]; then mv list_COSMOS list_paw0; fi

    for f in list_paw?; do if [ ! -s $f ]; then rm $f; fi; done
    wc -l list_images ; wc -l list_paw?

    # check that filename of a random file are links and if so remove them first:
    root=$(tail -9 list_images | head -1 | cut -d\. -f1)
    if [ -h ${root}.head ] && [ -h ${root}_weight.fits ]; then 
        ec " >> Delete links to weight and head files ..."
        rm v20*_00???.head v20*_00???_weight.fits
        ec " >> Rebuild links for available images only ..."
        for f in v20*_00???.fits; do 
            ln -s scamp/${f%.fits}.head .
            ln -s weights/${f%.fits}_weight.fits .
        done
    else
        ec " >> ATTN: Files are not links as expected ..."
        askuser
    fi


  ## ec "##-----------------------------------------------------------------------------"

#-----------------------------------------------------------------------------------------------
else
#-----------------------------------------------------------------------------------------------
   echo "!! ERROR: $1 invalid argument ... valid arguments are:"
   help
fi 

exit 0
#@@ ------------------------------------------------------------------------------------

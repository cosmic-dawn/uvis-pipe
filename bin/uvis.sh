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
##-----------------------------------------------------------------------------
set -u  # exit if a variable is not defined - recommended by Stephane

if [[ "${@: -1}" =~ 'dry' ]] || [ "${@: -1}" == 'test' ]; then dry=T; else dry=F; fi
if [[ "${@: -1}" =~ 'aut' ]]; then auto=T; else auto=F; fi
if [[ "${@: -1}" =~ 'int' ]]; then int=T;  else int=F;  fi

#-----------------------------------------------------------------------------

vers="3.00 (22.nov.19)"
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


#-----------------------------------------------------------------------------
# To do or not to do ... that is the question
#-----------------------------------------------------------------------------
# - need to use calib dir?  probably not, but it works ... leave as is for now
# - possibly define error codes
#
# - dc and bc commands not installed; dthr=$(echo "2 k  $dt 3600 / p" | dc) 
#   or dthr=$(echo "scale=2; $dt/3600" | bc) replaced by
#   dthr=$(echo "$dt 3600" | awk '{printf "%0.2f\n", $1/$2}')
#
#-----------------------------------------------------------------------------
# Some variables
#-----------------------------------------------------------------------------

auto=F; pauto=F  # automatic: proceed automatically to next process
int=0             # interactive ... barely implemented

uvis=/home/moneti/softs/uvis-pipe

bindir=$uvis/bin
pydir=$uvis/python
confdir=$uvis/config

if [ -h $0 ]; then pn=$(readlink -f $0); else pn=$(ls $0); fi    # get the command line
sdate=$(date "+%h%d@%H%M") 

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
        nimages=$(ls -1 $WRK/images/v20*_00???.fits 2> /dev/null | wc -l) #;  echo $nimages
        if [ $nimages -eq 0 ]; then
            echo " ---------- Dirs still empty ---------- "
        else
            froot=$(cd $WRK ; ls images/v20*_00???.fits | head -$(($nimages / 2)) | tail -1 | cut -d\. -f1 | cut -d\/ -f2 )
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

relcheck() {   # Print some parameters for user to check
    echo "#-----------------------------------------------------------------------------"
    echo "# Processing environment: "
    if [ $nofilt -eq 1 ]; then 
        echo "  - Filter is       NOT SET!!"; else echo "  - Filter is         "$FILTER; fi
    if [ -z ${WRK+x} ]; then
        echo "  - Release tag is  NOT SET!!"; else echo "  - Release tag is    "$REL; fi
    echo "  - Working area is   "$WRK
    echo "  - python scripts in "$pydir
    echo "  - config files in   "$confdir
    echo "  - pipe logfile is   "$(ls $pipelog )
    echo "  - Found images dir with "$nimages" image .fits files"
#   echo "  - PYTHONPATH is     "$PYTHONPATH 
#   echo "  - linux PATH is     "$PATH 
    echo "#-----------------------------------------------------------------------------"  
}

erract() { # what to do in case of error
    echo ""
    ec "!!! PROBLEM "; tail $logfile
#   ec "# Linux PATH is: $PATH"
#   ec "# PYTHONPATH is: $PYTHONPATH"
    exit 5
}
testmsg() {
    echo ""
    ec "#############################################################################"
    ec "#                                                                           #"
    ec "##  Begin pred $pn in test mode  ##"
    ec "#                                                                           #"
    ec "#############################################################################"
}
help() {
    egrep -n '^elif' $0 | tr -s ' ' | grep -v ' : ' > t1
    egrep -n '## - R' $0 | tr -s ' ' | grep -v egrep > t2
    egrep -n '^#@@' $0 | tr -d '@' > t3  
    cat t1 t2 t3 | sort -k1 -n | tr -s '#' | cut -d \# -f2
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

if [ -z ${WRK+x} ]; then echo "!! ERROR: must export WRK variable before starting" ; exit 2; fi

if [ -z ${FILTER+x} ]; then 
    echo "WARNING: FILTER variable not set" ; nofilt=1
else
    nofilt=0
    case  $FILTER in
        N | NB118 ) FILTER=NB118 ;;
        Y       ) FILTER=$FILTER ;;
        J       ) FILTER=$FILTER ;;
        H       ) FILTER=$FILTER ;;
        K | Ks  ) FILTER=Ks      ;;
        P       ) FILTER=$FILTER ;; # a bogus N filter for test purposes
        Q       ) FILTER=$FILTER ;; # a bogus Y filter for test purposes
        R       ) FILTER=$FILTER ;; # a bogus J filter for test purposes
        S       ) FILTER=$FILTER ;; # a bogus H filter for test purposes
        T       ) FILTER=$FILTER ;; # a bogus K filter for test purposes
        *          ) ec "# ERROR: invalid filter $FILTER"; exit 3 ;;
    esac
    pipelog="${WRK}/uvis.log" 
fi
# echo "## debug: \FILTER: $FILTER   \$FILTER: $FILTER" ; exit
if [ ! -e $pipelog ]; then touch $pipelog; fi

if [ $# -eq 0 ] || [ $1 = 'help' ] || [ $1 = '-h' ]; then 
    echo "#-----------------------------------------------------------------------------"
    ec "This script is  "$(which $0)" to process UltraVista data"
    echo "#-----------------------------------------------------------------------------"
    echo "| Syntax: "
    echo "| - pipe step [auto]  begin at step pN of the processing; "
    echo "|                     if auto is given, then continue automatically "
    echo "| - pipe -h or help    print this help "
    echo "#-----------------------------------------------------------------------------"
    help
    exit 0
fi 

# exec nodes
case $FILTER in
    NB118 | J | H ) node=n08 ;;
    Y | Ks        ) node=n09 ;;
    P | R | S     ) node=n08 ;;   # test on n08
    Q | T         ) node=n09 ;;   # test on n09
esac

# current node
node=$(pwd | cut -c 2-4)

#root="subima"                         # root name for swarp subimages
REL="DR4-"$FILTER                      # used in names of some products

badfiles=$WRK/DiscardedFiles.list      # built and appended to during processing
fileinfo=$WRK/FileInfo.dat             # lists assoc files (bpm, sky, flat) and other info for each image
Trash=Rejected                         # in images
RunTimes=$WRK/RunTimes.log

if [ ! -e $WRK/images/list_images ]; then 
    cd $WRK/images
    echo "list_images not found - rebuild it ...."
    \ls -1 v20*_00???.fits > list_images
    ls -l $WRK/images/list_images
    cd $WRK
fi

nimages=$(cat $WRK/images/list_images | wc -l)
imroot=$(head -$(($nimages / 2))  $WRK/images/list_images | tail -1 | cut -d\. -f1 | cut -d\/ -f2 )

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
#@@  The data processing steps:
#@@ ------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------
elif [[ $1 =~ 'prelim' ]]; then      # P0: link fit files to WRK area, decompress, build casuinfo table
#-----------------------------------------------------------------------------------------------
   
#    ec "#-----------------------------------------------------------------------------"
#    ec "## P0: Prepare CASU files"
#    ec "#-----------------------------------------------------------------------------"
#    exit 0
 
    ncurr=0 ; pcurr="p$ncurr" ; pnext="p$(($ncurr+1))"
    mycd $WRK
    
    ec "##----------------------------------------------------------------------------"
    ec "##          ======  Preliminaries  ======"
    ec "##----------------------------------------------------------------------------"
    
    ec " - Number of casu files in images:  $(ls -1 images/*.fit  | wc -l) "
    ec " - Number of casu files in stacks:  $(ls -1 stacks/*.fit  | wc -l) "
    ec " - Number of casu files in calib:   $(ls -1 calib/*.fit   | wc -l) "
 
    ec "#-----------------------------------------------------------------------------"
    ec "##-R0/ Remove known bad images and build info table"
    ec "#-----------------------------------------------------------------------------"

    #_# # ---------------------- remove bad data graded C in DR1; if there.  
    #_# mycd $WRK/images
    #_# grep ",C," $confdir/DR1_file_info.txt | grep $FILTER > LocalBadFiles.list
    #_# nbad=$(cat LocalBadFiles.list | wc -l)
    #_# if [ $nbad -ne 0 ]; then
    #_#     count=0
    #_#     for f in $(cut -d\, -f1 LocalBadFiles.list); do 
    #_#         if [ -e ${f}.fit ]; then 
    #_#             if [ $count -eq 0 ]; then echo "Files graded C in DR1" > $badfiles; fi
    #_#             rm ${f}.fit; echo "- ${f}.fit" >> $badfiles
    #_#             count=$(($count+1))
    #_#         fi
    #_#     done
    #_#     if [ $count -ne 0 ]; then ec "# Deleted $count files graded C in DR1; list in \$WRK/DiscardedFiles.list "
    #_#     else ec "# No C-graded files found ...";  fi
    #_# fi
    #_# rm LocalBadFiles.list
    #_# mycd $WRK
 
    ec "#-----------------------------------------------------------------------------"
    ec "##- decompress CASU files"
    ec "#-----------------------------------------------------------------------------"
    
    \ls -1 [i,c,s]*/*.fit | awk '{print "imcopy ",$1, $1"s"}' > to_imcopy
	if [ $dry == 'T' ]; then 
		ec " >> wrote to_imcopy to run inparallel mode"
		ec "----  EXITING PIPELINE DRY MODE         ---- "
		exit 10
	fi

	cat to_imcopy | parallel  -j 8
	mkdir raw
	mv [i,c,s]*/*.fit to_imcopy raw
	ec "# All .fit files decompressed to .fits, then moved to raw dir"
    
    #-----------------------------------------------------------------------------
    
    if [ $auto == 'T' ]; then                                 # END P0
        ec "#-----------------------------------------------------------------------------"
        ec "# $pcurr finished; ==> auto continue to $pnext"
        ec "#-----------------------------------------------------------------------------"
        $0 $pnext $pauto ; exit 0
    else
        ec "#-----------------------------------------------------------------------------"
        ec "#                                End of $pcurr "
        ec "#-----------------------------------------------------------------------------"
    fi
 
exit
#-----------------------------------------------------------------------------------------------
#elif [ $1 = 'p0' ]; then      # P0: link Converted data (not fully implemented)
#-----------------------------------------------------------------------------------------------
   
    if [ $dry != 'T' ]; then cppipe; else testmsg; fi
    ncurr=0 ; pcurr="p$ncurr" ; pnext="p$(($ncurr+1))"
 
    case $FILTER in
        NB118 | J | H ) bpmdir='/n08data/UltraVista/DR4/bpms'
                        rootdir="/n08data/UltraVista/ConvertedData/$FILTER" ;;
        Y | Ks        ) bpmdir='/n09data/UltraVista/DR4/bpms'   
                        rootdir="/n09data/UltraVista/ConvertedData/$FILTER" ;;
        * ) ec "# Unknown filter - quitting"; exit 5 ;;
    esac

    cd $WRK
    if [ ! -d images ]; then 
        ec "##----------------------------------------------------------------------------"
        ec "#"
        ec "##          ======  GENERAL SETUP  ======"
        ec "#"
        ec "##----------------------------------------------------------------------------"
        ec "## P0: Prepare converted data files"
        ec "#-----------------------------------------------------------------------------"

        mkdir images calib #stacks 
        ln -sf $rootdir/images/v20*_00???.fits   images
#       ln -s $rootdir/stacks/*.fits    stacks  
        ln -sf $rootdir/calib/*.fits     calib   
        ln -sf $bpmdir/bpm*.fits        calib   # some were already in calib
        cp $rootdir/FileInfo.dat  FileInfo.full

        cd images
        \ls -1 v20*_00???.fits > list_images; nimages=$(cat list_images | wc -l)
        for f in v20*.fits; do grep $f ../FileInfo.full >> ../FileInfo.dat; done
        #echo HERE
        cd -
        ec "#-----------------------------------------------------------------------------"
        ec "## - built links to $nimages ${FILTER} images and their ancillary flats and skies:  "
        ec "#-----------------------------------------------------------------------------"
        ec " - Number of images files:      $(ls -1L images/v20*_00???.fits | wc -l) "
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

if [ $HOSTNAME != "candid01.iap.fr" ]  && [ $dry -eq 0 ]; then  
    ec "#=========================================================#"
    ec "# ATTN: cannot start jobs from $HOSTNAME. Switched to dry mode  #"
    ec "#=========================================================#"
    dry=1
fi

#-----------------------------------------------------------------------------------------------
elif [ $1 = 'p1' ]; then      # P1: convert to WIRCam, fix flats, qFits, flag satur in ldacs
#-----------------------------------------------------------------------------------------------

#% P1: convert to WIRCam, fix flats, qFits, flag satur'n in ldacs
#% - convert various kwds to WIRCam (terapix) format
#% - convert kwds in flats, and produce normalised flags
#% - qFits:
#%   . builds weight files, 
#%   . builds ldacs for psfex and runs psfex to get psf size and reject files with bad psf
#%   . builds an ldac for use with scamp
#% - determine saturation level from the scamp ldacs and flag saturated sources
#%------------------------------------------------------------------

    if [ $dry -eq 0 ]; then cppipe; else testmsg; fi
    ec "#-----------------------------------------------------------------------------"
    ec "## P1: convert to WIRCam, fix flats, pseudo-qFits, and flag saturation in ldacs"
    ec "#-----------------------------------------------------------------------------"
    ncurr=1 ; pcurr="p$ncurr" ; pprev=p$(($ncurr-1)) ; pnext="p$(($ncurr+1))"
 
    if [ ! -d$WRK/images ]; then ec "PROBLEM: directory structure not setup ... quitting";  exit 5; fi
    mycd $WRK/images
        
    ec "CHECK: found list_images with $nimages entries "
    ec "#-----------------------------------------------------------------------------"

    # to check conv to WIRCam, look for IMRED_FF and IMRED_MK kwds exist in ext.16
    nkwds=$(dfits -x 16 $(tail -1 list_images) | grep IMRED_ | wc -l) #; echo $nkwds
 
    ###################### ATTN: not yet qsub'd ######################

    if [ -e convert_to_WIRCAM.log ] && [ $nkwds -eq 2 ]; then
        ec "CHECK: found convert_images.log, and files seem to contain IMRED_?? keywds"
        ec "CHECK: ==> conversion to WIRCam already done ... skip it"
        ec "#-----------------------------------------------------------------------------"
    else
        rcurr=R3
        if [ $dry == 'T' ]; then ec "====> Ready for $rcurr: conv to WIRCam on $nimages images"; exit 5; fi
        ec "#-----------------------------------------------------------------------------"
        ec "## - R3/ convert $nimages images to WIRCam format (keyword conversion)"
        ec "#-----------------------------------------------------------------------------"
        
        if [ $nimages -eq 0 ]; then ec "!! ERROR: no images found. "; askuser; fi
        
        exec="$pydir/convert_images.py"  ; ec "# exec is: $exec"
        args=" -s ../stacks/ --stack_addzp=Convert_keys.list"
        logfile=convert_full.log
        
        cp -a $confdir/Convert_keys.list . 
        
        python2 $exec -l list_images $args > $logfile 2>&1 
        if [ $? -ne 0 ]; then erract ; fi
 
        grep -v -e Impossible -e non-existing -e VerifyWarn $logfile > convert_short.log
        rm convert_full.log
        rm v20*_00???.fits                             # delete original CASU files
        rename _WIRCam.fits .fits v20*WIRCam.fits           # rename them to 'simple' names
 
        ec "#-----------------------------------------------------------------------------"
        if [ $int -eq 1 ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
 
    #----------------------------------------------------------------------------------------------#
 
    mycd $WRK/calib
 
    ###################### ATTN: not yet qsub'd ######################
 
    \ls -1 *_flat_*.fits | grep -v norm > list_flats  ; nflats=$(cat list_flats | wc -l)
    nfnorm=$(ls *_flat_*_norm.fits 2> /dev/null | wc -l)    #;   echo $nfnorm $nflats
    # check if flats already handled already done, if so skip to next step
    #echo $nfnorm $nflats  ;  ls norm_flat.log
    if [ -e norm_flat.log ] && [ $nfnorm -eq $nflats ]; then
        ec "CHECK: found norm_flat.log and normalised flats ... "
        ec "CHECK: ==> flat handling already done ... skip it"
        ec "#-----------------------------------------------------------------------------"
    else
        rcurr=R4
        if [ $dry == 'T' ]; then ec "====> Ready for $rcurr: rm PV from headers of flats"; exit 5; fi
        ec "#-----------------------------------------------------------------------------"
        ec "## - R4/ on flats: remove PV from the headers and normalise" 
        ec "#-----------------------------------------------------------------------------"
 
        exec="$pydir/convert_flats.py"  ; ec "# exec is: $exec"
        args=" --verbosity=INFO --log=clean_flats_int.log"
        logfile="clean_flats.log"
       
        if [ $nflats -eq 0 ]; then ec "!! ERROR: no flats found. "; askuser; fi
       ## ATTN: build run-file and run with parallel -j ???
        python $exec -l list_flats $args > $logfile 2>&1 
        if [ $? -ne 0 ]; then erract; fi
        
        exec="$pydir/norm_flats.py"  ; ec "# exec is: $exec"
        logfile="norm_flats.log"
 
        rename  _noPV.fits .fits ${FILTER}_flat*noPV.fits
        python $exec -l list_flats > $logfile 2>&1 
        if [ $? -ne 0 ]; then erract; fi
        ec "# Flat handing completed: 'PV' removed from headers and normalised"

        ec "#-----------------------------------------------------------------------------"
        if [ $int -eq 1 ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
    
    #----------------------------------------------------------------------------------------------#
 
    mycd $WRK/images
 
    if [ -e ${fileinfo} ]; then
        ec "CHECK: Found $(echo $fileinfo | cut -d\/ -f 6) ... continue"
        ec "#-----------------------------------------------------------------------------"
    else
        if [ $dry == 'T' ]; then ec "====> Ready to build FileInfo.dat"; exit 5; fi
 
        ec "# Build fileinfo table ..."
        dfits -x 1 v20*_00???.fits | fitsort -d OBJECT FILTER IMRED_FF IMRED_MK STACK SKYSUB | \
            sed -e 's/Done with //' -e 's/\[1\]/s/' -e 's/_st/_st.fits/' -e 's/\t/  /g' -e 's/   /  /g' > ${fileinfo}
        
        # Check that all support files are present: build lists 
        # rm consecutive spaces in order to be able to use space as separator
        cat ${fileinfo} | tr -s \   > fileinfo.tmp   
        cut -d' ' -f4,4 fileinfo.tmp | sort -u  > list_flats
        cut -d' ' -f5,5 fileinfo.tmp | sort -u  > list_bpms
        cut -d' ' -f6,6 fileinfo.tmp | sort -u  > list_stacks
        cut -d' ' -f7,7 fileinfo.tmp | sort -u  > list_skies
 
        err=0
        for f in $(cat list_flats);  do if [ ! -s ../calib/$f ];  then echo "ATTN: $f missing in calib";  echo $f >> list_missing; err=1; fi; done
        for f in $(cat list_bpms);   do if [ ! -s ../calib/$f ];  then echo "ATTN: $f missing in calib";  echo $f >> list_missing; err=1; fi; done
        for f in $(cat list_skies);  do if [ ! -s ../calib/$f ];  then echo "ATTN: $f missing in calib";  echo $f >> list_missing; err=1; fi; done
        if [ $err -eq 1 ]; then ec "# missing files ... see list_missing"; askuser
        else ec "# All needed flats, bpm, skies, stacks available ... continue";  fi
        rm list_bpms list_flats list_skies list_stacks
 
        ec "#-----------------------------------------------------------------------------"
        if [ $int -eq 1 ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
    
    if [ $HOSTNAME != "candid01.iap.fr" ]  && [ $dry -eq 0 ]; then  
        ec "#=========================================================#"
        ec "# ATTN: cannot start jobs from $HOSTNAME. Switched to dry mode  #"
        ec "#=========================================================#"
        dry=1
    fi

    #----------------------------------------------------------------------------------------------#
    #       pseudo qualityFITS
    #----------------------------------------------------------------------------------------------#

    nn=$(ls qFits_??.out 2> /dev/full | wc -l)
    if [ $nn -ge 1 ] || [ -e qFits.outs ] ; then  # 2nd if for compatibility
        ec "CHECK: Found $nn qFits_xx.out files and $(ls v20*_00???.ldac | wc -l) ldacs ..."
        ec "CHECK: ==> qFits has been run ... skip to next step"
        ec "#-----------------------------------------------------------------------------"
    else
        rcurr=R5  
        if [ $dry == 'T' ]; then ec "====> Ready for $rcurr: qFits"; fi
        if [ -s list_special ]; then list=list_special; else list=list_images; fi
      
        ec "## - R5:  qFits.sh:  pseudo qFits on $list with $(cat $list | wc -l) entries"
        ec "#-----------------------------------------------------------------------------"
        rm -f qall qFits_??.??? qFits_??.sh
        
        # N threads: list is split into these many sublists: 12 appropriate for many jobs; 
        #            use 2 for testing with few images
        if [ $nimages -lt 50 ]; then nts=5; else nts=12;fi
        split -n l/$nts $list --additional-suffix='.lst' qFits_
        ec "# Build $nts qsub files with about $(cat qFits_aa.lst | wc -l ) each ..."
        
        for l in qFits_??.lst; do
            id=$(echo $l | cut -d\_ -f2 | cut -d\. -f1) # ; echo $id
            qfile=qFits_${id}.sh ; touch $qfile ; chmod 755 $qfile
            sed -e 's|@LIST@|'$l'|'  -e 's|@ID@|'$id'|'  -e 's|@FILTER@|'$FILTER'|' \
                -e 's|@NODE@|'$node'|'  -e 's|@WRK@|'$WRK'|'  -e 's|@DRY@|'$dry'|'  \
                -e 's|@IDENT@|'$PWD/qFits_$id'|'  $bindir/qFits.sh > ./$qfile

            ec "# Built $qfile with $(cat $l | wc -l) entries"
            echo "qsub $qfile" >> qall
        done   
        ec "# ==> written to file 'qall' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then 
            ec "   >> EXITING TEST MODE << "
            ec "# to clean up: "; ec "% rm \$WRK/images/qFits_*.* \$WRK/images/qall ";  exit 0
        fi
 
        ec "# Submit qsub files ... ";  source qall >> $pipelog
        ec " >>>>   Wait for $nts qFits jobs ... first check in 1 min  <<<<<"

        btime=$(date "+%s.%N");  sleep 60      # before starting wait loop
        while :; do           #  begin qsub wait loop for qFits
            njobs=$(qstat -au moneti | grep qFits_${FILTER} | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished

			runjobs=$(qstat -au moneti | grep qFits_${FILTER} | grep \ R\  | wc -l)
            ndone=$(ls -1 v20*_00???.ldac 2> /dev/null | wc -l)
            ntodo=$(($nimages - $ndone)) ;  nsec=$((2*${ntodo})) 
            if [ $nsec -le 60 ];   then nsec=60; fi            # min: 1 min
			if [ $nsec -ge 3600 ]; then nsec=3600; fi          # max: 1 hr
            nmin=$(echo "scale=1; $nsec/60" | bc); nhrs=$(echo "scale=1; $nmin/60" | bc)
            if [ $nsec -gt 5400 ]; then wmsg="$nhrs hr"; else wmsg="$nmin min"; fi
            ec "# $runjobs jobs running; $ndone images processed, $ntodo remaining - next check in $wmsg"
            sleep $nsec
        done  
        ec "# qFits jobs finished, walltime: $(wtime) - check results ..."

        grep 'EXIT\ STATUS' qFits_??.out > estats
        ngood=$(grep STATUS:\ 0 estats | wc -l)
        if [ $ngood -ne $nts ]; then
            ec "#PROBLEM: some qFits' exit status not 0 ... "
            grep -v \ 0 estats;    askuser
        else
            ec "CHECK: qFits.sh exit status ok ... continue"
            rm estats
        fi

        # ---------------------- Finished qFits run; check products ----------------------
 
        nldacs=$(ls -1 $WRK/images/v20*_00???.ldac        2> /dev/null | wc -l)
        nwghts=$(ls -1 $WRK/images/v20*_00???_weight.fits 2> /dev/null | wc -l)
        npsfex=$(ls -1 $WRK/images/v20*_00???_psfex.xml   2> /dev/null | wc -l)
        ec "# Found $nwghts weights, $nldacs ldacs, $npsfex psfex.xml files for $nimages images "
        
        grep -ni ERROR   qFits_??.out > qFits.errs
        grep -ni WARNING qFits_??.out > qFits.warns
        nerr=$(cat qFits.errs | wc -l)
        nwarns=$(cat qFits.warns | wc -l)

        if [ $nerr -ge 1 ]; then ec "# ATTN: Found $nerr errors in qFits_xx.out files; see qFits.errs"; fi

        rm -f missing.ldacs  missing.weights  # to rebuild them
        if [ $nwghts -ne $nimages ]; then 
            for f in $(cat list_images); do 
                if [ ! -e ${f%.fits}_weight.fits ]; then ec "ATTN: weight missing for $f" >> missing.weights; fi
            done
            ec "# PROBLEM: $(($nimages-$nwghts)) weights missing ... see missing.weights"
        fi
        
        if [ $nldacs -ne $nimages ]; then 
            for f in $(cat list_images); do 
                if [ ! -e ${f%.fits}.ldac ]; then echo "ATTN: ldac missing for $f" >> missing.ldacs; fi
            done
            ec "# PROBLEM: $(($nimages-$nldacs)) ldacs missing ... see missing.ldacs"
        fi
        
        if [ $npsfex -ne $nimages ]; then ec "# PROBLEM: $(($nimages-$npsfex)) _psfex's missing ... "; fi
        
        if [ -s missing.weights ] || [ -s missing.ldacs ]; then 
            cat missing.ldacs missing.weights 2> /dev/null | cut -d\  -f7,7 > list_missing
            nmiss=$(cat list_missing | wc -l)
            ec "# PROBLEM: see list_missing "; askuser
        fi
        ec "# qFits runs successfull ...  GOOD JOB!! Clean-up and continue"
            # ---------------------- create subdirs for products
        if [ ! -d qFits ]; then mkdir qFits weights ldacs Rejected ; fi
        
        mv v20*_cosmic.fits v20*_flag.fits v20*psfex.ldac v20*_sex.xml  qFits
        mv qFits_??.sh qFits_??.log   qFits      # keep .out 
    fi
       
    #----------------------------------------------------------------------------------------------#
    #       Get psf stats from psfex
    #----------------------------------------------------------------------------------------------#
 
    if [ -s badPSF.dat ] && [ -s PSFsel.out ]; then
        ec "CHECK: PSF selection already done and $(grep v20 badPSF.dat | wc -l) images discarded"
        ec "CHECK: ==> skip to next step"
        ec "#-----------------------------------------------------------------------------"
    else
        npsfx=$(ls v20*psfex.xml | wc -l) 
        ec "## - R6:  PSFsel.sh: get psf stats from $npsfx xml files and discard ones with bad PSF"
        ec "#-----------------------------------------------------------------------------"
        rm -f PSFsel.??? PSFsel.sh
        # NB: single file - not working on list (could change)

        ls -1 v20*_00???_psfex.xml > PSFsel.lst
        qfile="PSFsel.sh"; touch $qfile; chmod 755 $qfile
        sed -e 's|@NODE@|'$node'|'  -e 's|@WRK@|'$WRK'|'  -e 's|@DRY@|'$dry'|' \
            -e 's|@FILTER@|'$FILTER'|'  $bindir/PSFsel.sh > ./$qfile

        ec "# Built $qfile with $(cat list_images | wc -l) entries"
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi

        ec "# submitting $qfile ...  "; qsub $qfile
        ec " >>>>   Wait for PSFsel job to finish ...   <<<<<" 
        
        btime=$(date "+%s.%N"); sleep 60   # before starting wait loop
        while :; do           #  begin qsub wait loop for PSFsel
            njobs=$(qstat -au moneti | grep PSFsel_${FILTER} | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished

            ndone=$(cat psfstats.full 2> /dev/null | wc -l)
            ntodo=$(($nimages - $ndone)) ;  nsec=$(($ntodo/10))
            if [ $nsec -le 60 ];   then nsec=60; fi            # min: 1 min
			if [ $nsec -ge 3600 ]; then nsec=3600; fi          # max: 1 hr
            nmin=$(echo "scale=1; $nsec/60" | bc); nhrs=$(echo "scale=1; $nmin/60" | bc)
            if [ $nsec -gt 5400 ]; then wmsg="$nhrs hr"; else wmsg="$nmin min"; fi
            ec "# $ndone PSFsels done, $ntodo remaining - next check in $wmsg "
            sleep $nsec
        done  
        ec "# PSFsel job finished; wallime $(wtime). "
        if [ ! -e PSFsel.out ]; then
            ec "# PSFsel.out not found ... aborted?? quitting "; exit 5
        fi  #;  ec "# PSFsel finished - now check exit status"

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
        nbad=$(grep v20 badPSF.dat | wc -l)
        ec "# PSF selection done, exit status ok; no errors found; see PSFsel.out for details "
        ec "# $nbad files with bad PSF found and removed ... clean-up and continue"

        best=$(sort -k2,3 psfstats.dat | head -1)
        ec "# Select highest quality image: $best"
        ### TBD: write ??? kwd to indicate ref photom image for scamp
        
        mv v20*_psfex.xml qFits_??.lst  qFits   # DELETE?? not sure worth keeping further
        ec "# move _weight files to weights/, ldac files to qFits/, and build links"
        mv v20*_weight.fits weights;  ln -s weights/v20*_weight.fits .
        mv v20*_00???.ldac  qFits;    ln -s qFits/v20*_00???.ldac .

        ls -1 v20*_00???.ldac > list_ldacs  ; nldacs=$(cat list_ldacs | wc -l)
        ls -1 v20*_00???.fits > list_images ; nimages=$(cat list_images | wc -l)
#       ec "# list_images and list_ldacs rebuild with $nimages images" 

        ec "#-----------------------------------------------------------------------------"
        if [ $int -eq 1 ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi  
    
    #----------------------------------------------------------------------------------------------#
    #       Flag saturated sources in ldacs
    #----------------------------------------------------------------------------------------------#
 
    nn=$(ls -1 FlagSat_*.out 2> /dev/null | wc -l)
    if [ $nn -gt 0 ]; then
        ec "CHECK: Found $nn FlagSat .out files - saturation already flagged in ldacs ..."
        ec "CHECK: ==> skip to next step "
        ec "#-----------------------------------------------------------------------------"
    else 
        rcurr=R6
        ec "## - R6:  FlagSat.sh: flag saturation in ldac files ==> new ldacs"
        ec "#-----------------------------------------------------------------------------"
        rm -f qall  FlagSat_??.??? FlagSat_??.sh
        
        # -----------------------------------------------------------------------------
        # Requires: ldac's from qFits (normally links to files in qFits dirs
        # Produces: new ldacs with saturation flagged (regular files) -noSAT, which are
        #           then renamed clobbering the originals (links)
        # ATTN: added --noplot option .... we never look at them .... at least for testing
        # -----------------------------------------------------------------------------
 
        if [ ! -s list_ldacs ]; then ls -1 v20*_00???.ldac > list_ldacs; fi  
        nldacs=$(cat list_ldacs | wc -l)

        if [ -s list_special ]; then list=list_special; else list=list_ldacs; fi
        # split the list:
        if [ $nimages -lt 50 ]; then nts=4; else nts=8; fi
        split -n l/$nts $list --additional-suffix='.lst' FlagSat_
        
        for l in FlagSat_??.lst; do
            id=$(echo $l | cut -d\_ -f2 | cut -d\. -f1) # ; echo $id
            qfile=FlagSat_${id}.sh  ; touch $qfile; chmod 755 $qfile
            sed -e 's|@NODE@|'$node'|'  -e 's|@LIST@|'$l'|'  -e 's|@DRY@|'$dry'|' \
                -e 's|@IDENT@|'$PWD/FlagSat_$id'|'   -e 's|@FILTER@|'$FILTER'|' \
                -e 's|@ID@|'$id'|'  -e 's|@WRK@|'$WRK'|'  $bindir/FlagSat.sh > $qfile

            ec "# Built $qfile with $(cat $l | wc -l) entries"
            echo "qsub $qfile" >> qall
        done   
        ec "# ==> written to file 'qall' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi

        ec "# Submit qsub files ... ";  source qall >> $pipelog
        ec " >>>>   wait for $nts FlagSat jobs ... first check in 1 min  <<<<<"
        
        bdate=$(date "+%s.%N"); sleep 60     # before starting wait loop
        while :; do           # qsub wait loop for FlagSat
            njobs=$(qstat -au moneti | grep FlagSat_${FILTER}_ | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished

			runjobs=$(qstat -au moneti |grep FlagSat_${FILTER}_ | grep \ R\  | wc -l)
            ndone=$(ls -1 v20*_00???_noSAT.ldac | wc -l)
            ntodo=$(($nimages - $ndone)) ;  nsec=$((${ntodo}/3)) 
            if [ $nsec -le 60 ];   then nsec=60; fi            # min: 1 min
			if [ $nsec -ge 3600 ]; then nsec=3600; fi          # max: 1 hr
            nmin=$(echo "scale=1; $nsec/60" | bc); nhrs=$(echo "scale=1; $nmin/60" | bc)
            if [ $nsec -gt 5400 ]; then wmsg="$nhrs hr"; else wmsg="$nmin min"; fi
            ec "# $runjobs jobs running; $ndone FlagSats done, $ntodo remaining - next check in $wmsg "
            sleep $nsec
        done  
        ec "# FlagSat finished; walltime $(wtime) - check results ..."

        rm -f estats         # before filling it
        for l in FlagSat_??.out; do tail -1 $l >> estats; done
        nbad=$(grep -v STATUS:\ 0  estats | wc -l)
        if [ $nbad -gt 0 ]; then
            ec "PROBLEM: some FlagSat's exit status not 0 ... "
            grep -v \ 0  estats;    askuser
        else
            ec "CHECK: FlagSat.sh exit status ok ... continue"; rm -f estats
        fi
        # check number of files produced:
        nnew=$(ls -1 v20*_noSAT.ldac | wc -l)
        if [ $nnew -lt $nimages ]; then
            ec "CHECK: found only $nnew noSAT ldacs; $(($nimages-$nnew)) missing...."
        fi

        # rm FutureWarning errors and check the rest:
        grep -v  -e 'FutureWarning' -e 'None:' -e '^*' FlagSat_??.out > FlagSat.outs
        grep -v -e '-----' -e 'runtime' FlagSat.outs > FlagSat.summ

        # Traceback (python) errors
        grep -B1 -A3 Traceback FlagSat.summ > FlagSat_python.errs
        nerrs=$(grep Traceback FlagSat_python.errs | wc -l)
        if [ $nerrs -gt 0 ]; then
            ec "PROBLEM: Found $nerrs python Traceback erros in FlagSat files ... see FlagSat_python.errs"
            askuser
        else
            rm -f FlagSat_python.errs
        fi

        # Other errors:
        grep -i -e ^% -e error FlagSat_??.out | grep -v FutureWarn | grep -B1 Error > FlagSat.errs
        grep python FlagSat.errs | tr -s \  | cut -d\  -f5,5 > FlagSat.missing
        nerr=$(grep Error FlagSat.errs | wc -l)
        if [ $nerr -gt 0 ]; then
            ec "CHECK: found $nerr errors ... see flagsat.errs & flagsat.missing"
            askuser
            
            ec "# Move $nerr images and related files to Rejected/; add names to $badfiles"
            for f in $(grep v20 FlagSat.missing | cut -d\. -f1); do 
                mv ${f}*.* Rejected
#               grep -v $f list_images > newlist; mv newlist list_images
            done
            ec "Problem with saturation flagging: " >> $badfiles
            sed 's/v20/- v20/' FlagSat.missing >> $badfiles
            ec "Rebuild list_images and list_ldacs"
            \ls -1 v20*_00???.fits > list_images ; nimages=$(cat list_images | wc -l)
            \ls -1 v20*_00???.ldac > list_ldacs  ; nldacs=$(cat list_ldacs | wc -l)
			ec "# Now have $nimages images and $nldacs ldacs. "
        else 
            rm FlagSat.errs FlagSat.missing
        fi 
        
        ec "#-----------------------------------------------------------------------------"
        ec "# rm previous ldacs (links) and build new ones to _noSAT ones with root name"
        
        rm -f v20*_00???.ldac  qall                  # these are the links
        mv v20*_00???_noSAT.ldac ldacs; ln -s ldacs/v20*noSAT.ldac .        
        rename _noSAT.ldac .ldac v20*noSAT.ldac   # rename ldacs; keep them local for scamp
		mv FlagSat_??.lst FlagSat_??.sh qFits
        mv qFits ..
    fi
    
    if [ $auto == 'T' ]; then                                 # END P1
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
	nn=$(ls UVIS*p1.fits UVIS*p1_weight.fits UVIS*p1_ob_flag.fits 2> /dev/null | wc -l )
	if [ $nn -eq 3 ]; then
		ec "# Found $nn UVIS*.fits files - looks like P2 has been done already ... quitting"
		exit 0
	fi
    if [ $dry -eq 0 ]; then cppipe; else testmsg; fi

    ec "#-----------------------------------------------------------------------------"
    ec "## P2: scamp, swarp, and first-pass stack: check available data ..."
    ec "#-----------------------------------------------------------------------------"
    
    ncurr=2 ; pcurr="p$ncurr" ; pprev=$(($ncurr-1)) ; pnext="p$(($ncurr+1))"
    
    if [ ! -e list_images ]; then 
		ec "WARNING: 'list_images not found ... build it "
        ls -1 v20*_00???.fits > list_images
    fi
    if [ ! -e list_ldacs  ]; then 
        ec "WARNING: list_ldacs not found ... build it"
        ls -1 v20*.ldac > list_ldacs
    fi

    if [ ! -e list_weights  ]; then 
        ec "WARNING: list_weights not found ... build it"
        ls -1 v20*_weight.fits > list_weights
    fi

     if [ ! -e list_heads  ]; then 
        ec "WARNING: list_heads not found ... build it"
        ls -1 v20*.head > list_heads
    fi

   # ls -1 v20*_00???.fits > list_images ; ls -1 v20*_00???.ldac > list_ldacs  ; 
	nldacs=$(cat list_ldacs   | wc -l)
	nimages=$(cat list_images | wc -l)
    nwghts=$(cat list_weights | wc -l)
    nheads=$(cat list_heads   | wc -l)
    
    if [ $nimages -eq $nldacs ]; then 
        ec "CHECK: found $nimages images, $nwghts weights, $nldacs ldacs ... " 
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
    

    #----------------------------------------------------------------------------------------#
    #       scamp
    #----------------------------------------------------------------------------------------#
    # check whether scamp has already been run ... 

    nn=$(ls -1 $WRK/images/pscamp.out 2> /dev/null | wc -l)
    if [ $nn -ne 0 ] && [ $nheads -eq $nimages ]; then
        ec "CHECK: scamp logfile already exists and found $nheads head files ..." 
        ec "CHECK:  ==> scamp already done skip to R8/ swarp "
        ec "#-----------------------------------------------------------------------------"
    else
        rcurr=R7

        if [ -s list_some ]; then cp list_some pscamp.lst; else cp list_ldacs pscamp.lst; fi
        nl=$(cat pscamp.lst | wc -l)
        if [ $nl -lt 2000 ]; then 
			wtime=48; nsec=30    # useful in testing
		else 
			wtime=100; nsec=300
		fi

        ec "## - R7:  pscamp.sh: run scamp on pscamp.lst with $nl entries ... "
        ec "#-----------------------------------------------------------------------------"
        rm -f pscamp.out pscamp.log pscamp.sh   # do not delete pscamp.lst, just build above
        ec "# using scamp  ==> $(scamp -v)"

        qfile="pscamp.sh"; touch $qfile; chmod 755 $qfile
        sed -e 's|@NODE@|'$node'|'     -e 's|@IDENT@|'$PWD/pscamp'|'  -e 's|@DRY@|0|'  \
            -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'pscamp.lst'|'    -e 's|@WRK@|'$WRK'|'  \
            -e 's|@WTIME@|'$wtime'|' $bindir/pscamp.sh > ./$qfile
        
        if [ $nl -lt 100 ]; then    # short (test) run - decrease resources
            sed -i -e 's|ppn=22|ppn=8|' -e 's|time=48|time=06|' $qfile
        fi

        ec "# Built $qfile with $nl entries"
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then 
			echo ""
			echo "   >> BEGIN dry-run of pscamp.sh:  << "
			echo ""
			$PWD/pscamp.sh list_ldacs dry
			echo "   >> Dry-run of $0 finished .... << "; exit 0
		fi
        
        ec "# submitting $qfile ... "; qsub $qfile      
        ec " >>>>   wait for pscamp to finish ...   <<<<<"
        
        btime=$(date "+%s"); sleep 60   # before starting wait loop
        while :; do           #  begin qsub wait loop for pscamp
            njobs=$(qstat -au moneti | grep pscamp_${FILTER} | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished
			tail -2 pscamp.log | strings 
            sleep $nsec
        done  
        if [ ! -e pscamp.out ]; then
            ec "# pscamp.out not found ... pscamp aborted?? "; exit 5
        fi  
        ec "# pscamp finished, walltime $(wtime) - now check exit status"
        
        ngood=$(tail -1 pscamp.out | grep STATUS:\ 0 | wc -l)
        if [ $ngood -ne 1 ]; then
            ec "PROBLEM: pscamp.sh exit status not 0 ... check pscamp.out"
            askuser
        fi
        nheads=$(ls -1 v20*.head | wc -l)
        ec "# scamp done; exit status ok; produced $nheads .head files and some png check images ... "

        # check number of .head files produced
        if [ $nheads -lt $nl ]; then
            ec "PROBLEM: built only $nheads head files for $nl ldacs ... "
            askuser
        fi

        # check warnings 
        nwarn=$(cat scamp.warn | wc -l)
        if [ $nwarn -ge 1 ]; then 
            ec "# WARNING: $nwarn warnings found in logfile for $nl files"; head scamp.warn
        fi   

        # check fluxscale table built by pscamp script
        ec "#-----------------------------------------------------------------------------"
        ec "#       Scamp flux-scale results"
        ec "#-----------------------------------------------------------------------------"
        $0 fscale
        ec "#-----------------------------------------------------------------------------"

        ec "CHECK: pscamp.sh successful, $nheads head file built ... clean-up and continue"
        mkdir scamp; mv v20*.head scamp; ln -s scamp/v*.head .
		mv *.png pscamp* fluxscale.dat list_low-contrast scamp
        rm v20*.ldac list_ldacs   # don't need these anymore

        ec "#-----------------------------------------------------------------------------"
        if [ $int -eq 1 ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
    

    #----------------------------------------------------------------------------------------#
    #       destripe casu images - p1
    #----------------------------------------------------------------------------------------#
    #  
    # 
    # 
    #----------------------------------------------------------------------------------------#

	# check if destripe already done
	if [ -e destripe_p1/destripe.log ]; then
		ec "CHECK: Found destripe_p1/destripe.log - destriping already done ..."
        ec "CHECK: ==> skip to next step "
        ec "#-----------------------------------------------------------------------------"
    else 
        rcurr=R8
        ec "## - R8:  destripe.sh: destripe $nimages casu images        "
        ec "#-----------------------------------------------------------------------------"
        rm -f qall estats destripe_??.log  destripe_??.sh

		# show the links of the image files
		ecn "CHECK: image files links:  " 
		ls -l $imroot.fits | tr -s ' ' | cut -d' ' -f9-13

		ec "# ==> Build ad-hoc processing scripts:"
 		rate=75                  # typical num images processed / hr
		nexp=$((2*$rate/3 * 18)) # number expected per process in 18 hrs
		nts=$(echo "$nimages / $nexp + 1" | bc) ##; 	echo "$nimages $nexp $nts"

		if [ $nts -le 5 ]; then nts=12; fi      # to avoid few very long lists
		if [ $nimages -lt 50 ]; then nts=5; fi  # for testing
        split -n l/$nts list_images --additional-suffix='.lst' destripe_
        
        for l in destripe_??.lst; do
			nl=$(cat $l | wc -l)
			if [ $nl -ge 1 ]; then
				id=$(echo $l | cut -d\_ -f2 | cut -d\. -f1) 
				qfile=destripe_${id}.sh  ; touch $qfile; chmod 755 $qfile
				sed -e 's|@NODE@|'$node'|'  -e 's|@LIST@|'$l'|'  -e 's|@DRY@|0|' \
					-e 's|@IDENT@|'$PWD/destripe_$id'|'   -e 's|@FILTER@|'$FILTER'|' \
					-e 's|@ID@|'$id'|'  -e 's|@OSUFF@|'_des'|'  -e 's|@WRK@|'$WRK'|'  \
					$bindir/destripe.sh > $qfile            
				ec "# Built $qfile with $nl entries"
			else
				ec "ATTN: list $l empty ..."
			fi
            echo "qsub $qfile" >> qall
        done   

        ec "# ==> written to file 'qall' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec "# Submit qsub files ... ";  source qall >> $pipelog
        ec " >>>>   wait for $nts destripe jobs ... first check in 1 min  <<<<<"
        
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        while :; do                        # qsub wait loop for destripe
            njobs=$(qstat -au moneti | grep destripe_${FILTER}_ | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished
            
			runjobs=$(qstat -au moneti | grep destripe_${FILTER}_ | grep \ R\  | wc -l) 
            ndone=$(ls -1 v20*_00???_des.fits 2> /dev/null | wc -l)
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
            ec "CHECK: destripe.sh exit status ok ..."; rm -f estats qall
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
        if [ $int -eq 1 ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi



    #----------------------------------------------------------------------------------------#
    #       swarp - p1
    #----------------------------------------------------------------------------------------#
    #  
    # 
    # 
    #----------------------------------------------------------------------------------------#

	stout=UVISTA_${REL}_p1   # output intermediate stack w/o .fits extension - for low res

    # check if swarp alreay done:
    nsubima=$(ls -1 substack_paw?_??.fits 2> /dev/null | wc -l) # ; echo $nsubima ; echo $npaws
    if [ $nsubima -ge 2 ]; then 
        ec "CHECK: Found $nsubima substacks - swarp done ??? "
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

        rm -f qall estats pswarp1_paw?.??? pswarp1_paw?.sh     # just in case
		headfile=firstpass.head
		subsky=Y                             # for pass1 DO subtract sky
		subsky=N                             # for pass1 destriped, DO NOT subtract sky
        ecn "# image files links:  " 
		ls -l $imroot.fits | tr -s ' ' | cut -d' ' -f9-13
		ecn "# head files links:   " 
		ls -l $imroot.head | tr -s ' ' | cut -d' ' -f9-13
		ecn "# weight files links: " 
		ls -l ${imroot}_weight.fits | tr -s ' ' | cut -d' ' -f9-13
		ec "#-------------------------------------------------------#"
		ec "#### ATTN: head-file: $headfile"
		ec "#### ATTN: subsky:    $subsky"
		ec "#### ATTN: output:    $stout"
        ec "#-------------------------------------------------------#"

		nim=280  # approx num of images in each sublist
		nim=480  # approx num of images in each sublist
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
				
				qfile="pswarp1_$paw.sh"; touch $qfile; chmod 755 $qfile
				sed -e 's|@NODE@|'$node'|'  -e 's|@IDENT@|'$PWD/pswarp1'|'  -e 's|@DRY@|0|'  \
					-e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'$slist'|'  -e 's|@WRK@|'$WRK'|' \
					-e 's|@PAW@|'$paw'|'  -e 's|@HEAD@|'$headfile'|'                         \
					-e 's/@SUBSKY@/'$subsky'/'  $bindir/pswarp.sh > $qfile
            
				ec "# Built $qfile with $nl images for paw $paw ==> $outname"
				echo "qsub $qfile" >> qall
			done
        done 
		nq=$(cat qall | wc -l)
        ec "# ==> written to file 'qall' with $nq entries "		
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi

        ec "# Submit qsub files ... ";  source qall >> $pipelog
        ec " >>>>   Wait for $nq pswarp jobs ... first check in 1 min  <<<<<"

        if [ $nimages -lt 500 ]; then nsec=30; else nsec=600; fi    # useful in testing
		ostr="ddd"                     # a dummy string for comparisons within the loop
        btime=$(date "+%s.%N");  sleep 60 
        while :; do           #  begin qsub wait loop for pswarp
            njobs=$(qstat -au moneti | grep pswarp_${FILTER}_paw | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished

			str=$(ls -lthr substack_paw?.fits paw?/substack_paw?.fits 2> /dev/null | tr -s ' ' | cut -d' ' -f4-9 | tail -1)
			if [[ $str != $ostr ]]; then ec " $njobs running or queued; last substack:  $str " ; fi
			ostr=$str
            sleep $nsec
        done  
        ec "# pswarp finished; walltime $(wtime)"
        
        grep EXIT\ STATUS pswarp1_paw?_??.out  >> estats
        nbad=$(grep -v STATUS:\ 0  estats | wc -l)  # files w/ status != 0
        if [ $nbad -gt 0 ]; then
            ec "PROBLEM: pswarp1_pawx.sh exit status not 0 "
			grep -v STATUS:\ 0 estats 
			askuser
        fi
        ec "CHECK: pswarp1_pawx.sh exit status ok"; rm estats

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
            ls -l substack_paw?.fits
            askuser
        fi

		# check for WARNINGS in logfiles
		warn=0
		for f in pswarp_paw?_??_??.log; do
			grep WARNING $f | wc -l > ${f%.log}.warn
			if [ $(wc ${f%.log}.warn | wc -l) -gt 0 ]; then warn=1; fi
		done
		if [ $warn -eq 1 ]; then 
			ec "ATTN: found warnings in pswarp logfiles"
			askuer
		fi

        mkdir swarp_p1
        mv pswarp_*.log paw?/substack*.xml pswarp1*.sh pswarp*.warn list_paw? swarp_p1
		rename swarp_paw swarp1_paw pswarp_paw?.???
        #mv paw?/list_paw? .
        rm -rf paw? qall 

        ec "#-----------------------------------------------------------------------------"
        if [ $int -eq 1 ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi


    #----------------------------------------------------------------------------------------#
    #          merge p1 substacks
    #----------------------------------------------------------------------------------------#
    
    stout=UVISTA_${REL}_p1   # output intermediate stack w/o .fits extension

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

        qfile="pmerge.sh"; touch $qfile; chmod 755 $qfile
        sed -e 's|@NODE@|'$node'|'  -e 's|@IDENT@|'$PWD/pmerge'|'  -e 's|@DRY@|'$dry'|'  \
            -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'pmerge.lst'|'  -e 's|@WRK@|'$WRK'|'  \
            -e 's|@STOUT@|'$stout'|'  -e 's|@HEAD@|firstpass.head|'  $bindir/pmerge.sh > ./$qfile
        
        ec "# Built $qfile with $nsubstacks entries"
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec "# submitting $qfile ... "; qsub $qfile
        ec " >>>>   wait for pmerge to finish ...   <<<<<"
        
        btime=$(date "+%s.%N");  sleep 60   # before starting wait loop
        while :; do           #  begin qsub wait loop for pmerge
            njobs=$(qstat -au moneti | grep pmerge_${FILTER} | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished
            sleep 30
         done  
        ec "# pmerge finished - now check exit status"
        
        ngood=$(tail -1 pmerge.out | grep STATUS:\ 0 | wc -l)
        if [ $ngood -ne 1 ]; then
            ec "PROBLEM: pmerge.sh exit status not 0 ... check pmerge.out"
            askuser
        fi
          
		ec "CHECK: pmerge.sh exit status ok ... continue"
        ec "# $stout and associated products built:"
        ls -lrth UVISTA*p1*.*
        ec "# ..... GOOD JOB! "
		mv substack_paw?_??.fits substack_paw?_??_weight.fits swarp_p1
    fi

    if [ $auto == 'T' ]; then                                 # END P2
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
elif [ $1 = 'p3' ]; then      # P3: add CASU sky then determine and remove best sky 
#-----------------------------------------------------------------------------------------------

#% P3: add CASU sky then determine and remove best sky 
#% - first add casu sky to wircam images to build with_sky images
#% - then build masks using current wircam images and 1st pass stack
#%   . now replace wircam images with with_sky images
#% - determine good skies and subtract, 
#%   . then do add'l clening: bgd removal and destriping
#%------------------------------------------------------------------

    if [ $dry -eq 0 ]; then cppipe; else testmsg; fi
#    ec "#-----------------------------------------------------------------------------"
#    ec "## P3: add CASU sky then determine and remove best sky with proper mask"
#    ec "#-----------------------------------------------------------------------------"
    ncurr=3 ; pcurr="p$ncurr" ; pprev=$(($ncurr-1)) ; pnext="p$(($ncurr+1))"
    mycd $WRK/images

    # check that P2 finished properly
    nimages=$(ls -1L v20*_00???.fits 2> /dev/null | wc -l)  
    nlist=$(cat list_images | wc -l)

    if [ $nimages -gt 0 ]; then 
        ec "CHECK:  found $nimages image files like: " 
        ecn " ==> "; ls -lh v20*_00???.fits | head -1 | tr -s ' ' | cut -d' ' -f9-12
    else
        ec "!!! BIG PROBLEM: No image files found, or links not valid"
        ls -lh v20*_00???.fits | head -1  ; askuser
    fi
   
    if [ $nimages -ne $nlist ]; then
        ec "CHECK: list images contains $nlist files, != $nimages ... "; askuser
    fi

    p1stack=$(ls -1L UVIS*p1.fits UVIS*p1_weight.fits UVIS*p1_ob_flag.fits  2> /dev/null| wc -l)
    if [ $p1stack -eq 3 ]; then
        ec "CHECK: found expected p1 stack products ... "
    else 
        ec "PROBLEM: need the pass1 stacks to proceed ... quitting "; exit 1
    fi
    ec "# Looks like it's ok to continue ... " 
    ec "#-----------------------------------------------------------------------------"

    # ----------------------  Finished checking  ----------------------

    # build zeroes files:
    if [ ! -e zeroes.fits ]; then
        lbpm=$(\ls -t ../calib/bpm* | head -1)
        ln -s $lbpm zeroes.fits
    fi


    #----------------------------------------------------------------------------------------------#
    #       Build masks for sky subtraction
    #----------------------------------------------------------------------------------------------#
    # 
    # 
    # 
    # 
    #----------------------------------------------------------------------------------------------#

    nmsk=$(ls -1 v20*_mask.fits 2> /dev/null | wc -l )
    nl=$(ls mkMasks_*.log 2> /dev/null| wc -l)           #;    echo $nimages $nmsk

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
        ec "# split into $nts chunks of about $nexp images"

        if [ $nts -lt 55 ]; then nts=55; fi
        if [ $nimages -lt 50 ]; then nts=3; fi  # for testing
        split -n l/$nts $list --additional-suffix='.lst' mkMasks_

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

        ec "# Submit qsub files ... ";  source qall >> $pipelog
        ec " >>>>   wait for $nts mkMasks jobs ... first check in 1 min  <<<<<"
        
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        while :; do             # qsub wait loop 
            njobs=$(qstat -au moneti | grep Masks_${FILTER}_ | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished

			runjobs=$(qstat -au moneti | grep Masks_${FILTER}_ | grep \ R\  | wc -l)
            ndone=$(ls -1 /scratch??/mkMasks_*/v20*_00???_mask.fits v20*_00???_mask.fits 2> /dev/null | wc -l)
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
        fi
        
        # check for amonalies in masks: in mkMasks_??.dat
        rm -f mkMasks.dat                # clean up before building it
        cat mkMasks_??.dat >> mkMasks.dat
        nn=$(grep '\ 0\.00\ ' mkMasks.dat | wc -l)
        if [ $nn -gt 0 ]; then
            ec "PROBLEM: $nn files with one or more chips fully masked: check mkMasks_??.dat"
            askuser
        fi

        ec "#-----------------------------------------------------------------------------"
        if [ $int -eq 1 ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
    

    #----------------------------------------------------------------------------------------------#
    #       Add casu sky
    #----------------------------------------------------------------------------------------------#
    # 
    # 
    # 
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

        sky1=$(ls -1 ../calib/sky*.fits | head -1)
        if [ ! -e ${sky1#../calib/} ]; then
            ec "# Build links to sky and bpm files."
            ln -sf ../calib/sky_*.fits . ; ln -sf ../calib/bpm_*.fits .
        fi
        
        # split the list into chunks of max 1200 images, normally doable in 32 hrs hr:
        if [ $nimages -lt 50 ]; then nts=5; else nts=12; fi
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
        
        ec "# Submit qsub files ... ";  source qall >> $pipelog
        ec " >>>>   wait for $nts addSky jobs ... first check in 1 min  <<<<<"
        
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        while :; do             # qsub wait loop for addSky
            njobs=$(qstat -au moneti | grep addSky_${FILTER} | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished

			runjobs=$(qstat -au moneti | grep addSky_${FILTER} | grep \ R\  | wc -l)
            ndone=$(ls -1 v20*_00???_withSky.fits | wc -l)
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
        
        # clean up:
        if [ ! -d withSky ]; then mkdir withSky; fi 
        mv v20*withSky.fits withSky
        ln -s withSky/v20*_withSky.fits . ; rename _withSky.fits .fits v20*_withSky.fits
        rm sky_20*.fits bpm*[0-9].fits  # links to ../calib/sky and bpm files

        ec "#-----------------------------------------------------------------------------"
        if [ $int -eq 1 ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi


    #----------------------------------------------------------------------------------------------#
    #       Compute good sky 
    #----------------------------------------------------------------------------------------------#
    # Don't need links to withSky images here (in images/) as work is done in subdirs and direct
    # links are created there.  
    # Inputs:  withSky images, heads, masks, weights
    # Outputs:
    #----------------------------------------------------------------------------------------------#

    nn=$(ls -1 mkSky_??.log 2> /dev/null | wc -l)
    ns=$(ls mkSky/v20*_sky.fits 2> /dev/null | wc -l)
    if [ $nn -ge 1 ] && [ $ns -ge 5 ]; then
        ec "CHECK: Found $nn mkSky_xx.log files and $ns _sky.fits files for $nimages images. "
        ec "CHECK: ====> sky subtraction already done - skip to next step"
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
		nout=$(ls -1 mkSky_??.out 2> /dev/full | wc -l)
		nlog=$(ls -1 mkSky_??.log 2> /dev/full | wc -l)
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
		if [ ! -e list_missing ]; then   	# ok when list is "continuous"
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
            echo "qsub ${qfile} ; sleep 4" >> qall
        done  
        ec "#-----------------------------------------------------------------------------"
        ec "# ==> written to file 'qall' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec "# Submit qsub files ... ";  source qall >> $pipelog
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
        for l in mkSky_??.out; do tail -1 $l >> estats; done
        nbad=$(grep -v STATUS:\ 0  estats | wc -l)
        if [ $nbad -gt 0 ]; then
            ec "PROBLEM: some mkSky's exit status not 0 ... "
            grep -v \ 0  estats;    askuser
        else
            ec "CHECK: mkSky.sh exit status ok ... continue"; rm -f estats qall
        fi
        
        ### handle frames which have insuffient nearby frames to determine sky ###
        grep CHECK: mkSky_??.log | grep skip | cut -d\: -f3 | tr -d ' '  > list_noSky
        nn=$(cat list_noSky | wc -l)
        if [ $nn -ge 1 ]; then
            ec "# Found $nn images with insufficient neighbours to build sky, see list_noSky"
            for f in $(cat list_noSky); do mv ${f%.fits}*.* Rejected; done
            echo "# $nn files with too few neighbours to build sky" >> $badfiles
            cat list_noSky >> $badfiles
            ec "# ... removed them (to Rejected dir) and added names to $badfiles"
            ls -1 v20*_00???.fits > list_images
            nimages=$(cat list_images | wc -l)
            ec "# ==> $nimages valid images left."
        fi

        ### check number of valid files produced:
        nnew=$(ls -1 v20*_sky.fits | wc -l)
        if [ $nnew -lt $nimages ]; then
            ec "CHECK: found only $nnew _sky files; $(($nimages-$nnew)) missing...."
			ls v20*_sky.fits | sed 's/_sky//' > list_done
			comm -23 list_images list_done > list_missing
            askuser
        fi

        if [ $int -eq 1 ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
    

    #----------------------------------------------------------------------------------------------#
    #       Update weights: set _weight to 0 where no sky was determined
    #----------------------------------------------------------------------------------------------#
    # Inputs: weight.fits file and .sky.fits files; 
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
		nout=$(ls -1 updateWeights_??.out 2> /dev/full | wc -l)
		nlog=$(ls -1 updateWeights_??.log 2> /dev/full | wc -l) # ; echo $nout $nlog
		if [ $nout -ge 1 ] || [ $nlog -ge 1 ]; then
			ec "ATTN: found $nout updateWeights_??.out and $nlog update_weights_??.log files ... delete them and continue??"
			askuser
		fi
        rm -f qall estats updateWeights_??.out  updateWeights_??.sh

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
        
        ec "# Submit qsub files ... ";  source qall >> $pipelog
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

#		ndone=$(ls -1 updateWeights_??.out | wc -l)   # number of .out files found
		grep EXIT\ STATUS updateWeights_??.out > estats
        nbad=$(grep -v STATUS:\ 0 estats | wc -l)
        if [ $nbad -gt 0 ]; then
            ec "PROBLEM: some updateWeights's exit status not 0 ... "
            grep -v \ 0  estats;    askuser
        else
            ec "CHECK: updateWeights.sh exit status ok ... continue"; rm -f qall
        fi
        
		# mv _sky images to mkSky dir
		if [ ! -d mkSky ]; then mkdir mkSky; fi
		mv v20*_sky.fits mkSky
#        ec "#-----------------------------------------------------------------------------"
        if [ $int -eq 1 ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi


    if [ $auto == 'T' ]; then                                 # END P3
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

    cd $WRK; if [ $dry -eq 0 ]; then cppipe; else testmsg; fi
    ncurr=4 ; pcurr="p$ncurr" ; pprev=$(($ncurr-1)) ; pnext="p$(($ncurr+1))"
    mycd $WRK/images
    
    nimages=$(ls -1 v20??????_00???.fits | wc -l)
    ec "# CHECK: found $nimages images"
    
    # ----------------------  Finished checking  ----------------------

    #----------------------------------------------------------------------------------------#
    #       Actual sky subtraction, destriping, and large-scale bgd removal
    #----------------------------------------------------------------------------------------#
    # Requires: _withSky, _sky _mask images
    # Produces _clean files
    # 
    #----------------------------------------------------------------------------------------#

	nn=$(ls cleaned/v20*_clean.fits 2> /dev/null | wc -l)
	nl=$(ls subSky_??.log 2> /dev/null | wc -l)
    if [ $nl -ge 1 ]  && [ $nn -eq $nimages ]; then 
        ec "CHECK: Found $nn clean images in and $nl subSky_??.log files "
        ec "CHECK: ====> skip to next step "
        ec "#-----------------------------------------------------------------------------"
    else     
		rcurr=R15
        ec "## - R15:  subSky.sh: pure sky-sub, destriping, and lsb-cleaning of $nimages images "
        ec "#-----------------------------------------------------------------------------"
		nout=$(ls -1 subSky_??.out 2> /dev/full | wc -l)
		nlog=$(ls -1 subSky_??.log 2> /dev/full | wc -l) # ; echo $nout $nlog
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

		ec "# ==> Build ad-hoc processing scripts:"
 		rate=10         # typical num images processed / hr
		nexp=$((2*$rate/3 * 18)) # number expected per process in 32 hrs
		nts=$(echo "$nimages / $nexp + 1" | bc) ##; 	echo "$nimages $nexp $nts"

		if [ $nts -le 5 ]; then nts=24; fi      # to avoid few very long lists
		if [ $nimages -lt 50 ]; then nts=5; fi  # for testing
        split -n l/$nts list_images --additional-suffix='.lst' subSky_
        
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
			else
				ec "ATTN: list $l empty ..."
			fi
            echo "qsub $qfile" >> qall
        done   
        ec "# ==> written to file 'qall' with $nts entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec "# Submit qsub files ... ";  source qall >> $pipelog
        ec " >>>>   wait for $nts subSky jobs ... first check in 1 min  <<<<<"
        
        btime=$(date "+%s.%N"); sleep 60     # before starting wait loop
        while :; do             # qsub wait loop for subSky
            njobs=$(qstat -au moneti | grep subSky_${FILTER}_ | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished
            
			runjobs=$(qstat -au moneti | grep subSky_${FILTER}_ | grep \ R\  | wc -l) 
            ndone=$(ls -1 /scratch??/subSky_*/v20*_00???_clean.fits v20*_00???_clean.fits 2> /dev/null | wc -l)
            ntodo=$(($nimages - $ndone)) ; nsec=$((2*$ntodo))
            if [ $nsec -le 60 ];   then nsec=60; fi            # min: 1 min
			if [ $nsec -ge 3600 ]; then nsec=3600; fi          # max: 1 hr
            nmin=$(echo "scale=1; $nsec/60" | bc); nhrs=$(echo "scale=1; $nmin/60" | bc)
            if [ $nsec -gt 5400 ]; then wmsg="$nhrs hr"; else wmsg="$nmin min"; fi
            ec "# $runjobs jobs running; $ndone images processed, $ntodo remaining - next check in $wmsg "
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
		cat subSky*.log >> subSky.log
        
        # check number of files produced:
        nnew=$(ls -1 v20*_clean.fits | wc -l)
        if [ $nnew -lt $nimages ]; then
            ec "CHECK: found only $nnew mask files; $(($nimages-$nnew)) missing...."
            askuser
        fi
		
		mkdir cleaned; mv v20*_clean.fits cleaned;
		ln -s cleaned/v*clean.fits .; rename _clean.fits .fits v*_clean.fits
        ec "CHECK: subSky.sh done, clean images moved to cleaned/ and linked ... continue"
		ec "#-----------------------------------------------------------------------------"
        if [ $int -eq 1 ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi
 

    #----------------------------------------------------------------------------------------#
    #       swarp - p2
    #----------------------------------------------------------------------------------------#
    #  
    # 
    # 
    #----------------------------------------------------------------------------------------#

	# update paw lists?
    npaws=$(ls list_paw? 2> /dev/null | wc -l)  # ; echo " Found $npaws paw lists"
	if [ $npaws -gt 0 ]; then
		nn=$(wc list_paw? 2> /dev/null | grep total | tr -s ' ' | cut -d' ' -f2 )
	else
		nn=0
	fi
	if [ $nn -ne $nimages ]; then
        ec "# No paw lists found or lists out of date ... (re)build them "
        $0 plists  
		npaws=$(ls list_paw? 2> /dev/null | wc -l)
    fi

    # check if swarp alreay done:
    nsubima=$(ls -1 substack_paw?_??.fits 2> /dev/null | wc -l)   

    stout=UVISTA_${REL}_p2lr   # output intermediate stack w/o .fits extension - for low res
	# if already there, then do the high res (cosmos) version 
	if [ -e ${stout}.fits ]; then stout=${stout%lr}hr ; fi  

    if [ $nsubima -ge $npaws ]; then 
        ec "CHECK: Found $nsubima substacks - swarp done ..."
        ec "CHECK: ==> skip to next step "
        ec "#-----------------------------------------------------------------------------"
    else 
        rcurr=R16
        ec "## - R16:  pswarp.sh: swarp pass2 for $npaws paws ... by sub-paws "
        ec "#-----------------------------------------------------------------------------"
		nout=$(ls -1 pswarp2_??.out 2> /dev/full | wc -l)
		nlog=$(ls -1 pswarp2_??.log 2> /dev/full | wc -l)
		if [ $nout -ge 1 ] || [ $nlog -ge 1 ]; then
			ec "ATTN: found $nout pswarp2_??.out and $nlog pswarp2_??.log files ... delete them and continue??"
			askuser
		fi
        rm -f qall estats pswarp2_paw_??.lst  pswarp2_paw_??.sh     # just in case
		
		# if no p2 stack, then do one at low res, 
		if [[ $stout  =~ "lr" ]]; then
			ec "# low-res p2 stack not found ... build it"
			headfile=firstpass.head
		else
			ec "# found low-res p2 stack ... now build stack at cosmos res."
			headfile=cosmos.head
		fi

		subsky=N                             # for pass2 DO NOT subtract sky
		ecn "# image files links:  " 
		ls -l $imroot.fits | tr -s ' ' | cut -d' ' -f9-13
		ecn "# head files links:   " 
		ls -l $imroot.head | tr -s ' ' | cut -d' ' -f9-13
		ecn "# weight files links: " 
		ls -l ${imroot}_weight.fits | tr -s ' ' | cut -d' ' -f9-13
		ec "#-------------------------------------------------------#"
		ec "#### ATTN: head-file: $headfile"
		ec "#### ATTN: subsky:    $subsky"
		ec "#### ATTN: output:    $stout"
        ec "#-------------------------------------------------------#"

		nim=280  # approx num of images in each sublist
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
				sed -e 's|@NODE@|'$node'|'  -e 's|@IDENT@|'$PWD/pswarp2'|'  -e 's|@DRY@|0|'  \
					-e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'$slist'|'  -e 's|@WRK@|'$WRK'|' \
					-e 's|@PAW@|'$paw'|'  -e 's|@HEADFILE@|'$headfile'|'                     \
					-e 's/@SUBSKY@/'$subsky'/'  $bindir/pswarp.sh > $qfile
            
				ec "# Built $qfile with $nl images for paw $paw ==> $outname"
				echo "qsub $qfile" >> qall
			done
        done 
		nq=$(cat qall | wc -l)
        ec "# ==> written to file 'qall' with $nq entries "
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi

        ec "# Submit qsub files ... ";  source qall >> $pipelog
        ec " >>>>   Wait $nq pswarp jobs ... first check in 1 min  <<<<<"

        if [ $nimages -lt 500 ]; then nsec=30; else nsec=600; fi    # useful in testing
		ostr="ddd"                     # a dummy string for comparisons within the loop
        btime=$(date "+%s.%N");  sleep 60           # begin time
        while :; do           #  begin qsub wait loop for pswarp
            njobs=$(qstat -au moneti | grep swarp_${FILTER} | wc -l)
			#echo "DEBUG $njobs running or queued ... waiting ... "    # DEBUG
            [ $njobs -eq 0 ] && break          # jobs finished

			# check every $nsec sec, and if a new substack is done then print this message
			str=$(ls -lthr substack_paw?_??.fits 2> /dev/null | tr -s ' ' | cut -d' ' -f4-9 | tail -1)
		#	str=$(ls -lthr paw?_??_*/substack_paw?_??.fits 2> /dev/null | tr -s ' ' | cut -d' ' -f4-9 | tail -1)
			if [[ $str != $ostr ]]; then ec " $njobs running or queued; last substack:  $str " ; fi
			ostr=$str
            sleep $nsec
        done  
        ec "# pswarp finished; walltime $(wtime)"
        
		# check exit status
        grep EXIT\ STATUS pswarp2_paw?_??.out >> estats
        nbad=$(grep -v STATUS:\ 0  estats | wc -l)  # files w/ status != 0
        if [ $nbad -gt 0 ]; then
            ec "PROBLEM: pswarp2_paw?_??.sh exit status not 0: "
			grep -v STATUS:\ 0 estats 
			askuser
        fi
        ec "CHECK: pswarp2_xxx.sh exit status ok"; rm estats

		# check num substacks found
 		nn=$(ls substack_paw?_??.fits | wc -l)
		if [ $nn -lt $nq ]; then
			ec "PROBLEM:  found only $nn substacks for $nq expected ..."
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
		for f in pswarp_paw?_??.log; do   # log files not yet renamed
			grep WARNING $f | wc -l > ${f%.log}.warn
			if [ $(wc ${f%.log}.warn | wc -l) -gt 1 ]; then warn=1; fi
		done
		if [ $warn -eq 1 ]; then 
			ec "ATTN: found warnings in pswarp logfiles"
			askuser
		fi

        if [ ! -d swapr_p2 ]; then mkdir swarp_p2; fi
        mv pswarp2*.sh pswarp*.warn  pswarp2_paw?_??.lst swarp_p2
		rename swarp_paw swarp2_paw pswarp_paw*.log  # name built in script

        rm -rf paw?_?? qall 

        ec "#-----------------------------------------------------------------------------"
        if [ $int -eq 1 ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi

   
    #----------------------------------------------------------------------------------------#
    #          Merge p2 substacks
    #----------------------------------------------------------------------------------------#
    
    if [ -e $stout ]; then 
        ec "#CHECK: stack $stout already built; "
        ec "#       ==> continue with building its mask and flag"
    else 
        rcurr=R17
        rm -f pmerge.??? pmerge.sh
#        ls -1 substack_paw?.fits substack_paw?_??.fits > pmerge.lst
        ls -1 substack_paw?_??.fits > pmerge.lst
        nsubstacks=$(cat pmerge.lst | wc -l)
		if [ $nsubstacks -eq 0 ]; then
			"ERROR: no substacks found - quitting"; exit 2
		fi

        ec "## - R17:  pmerge.sh: Merge $nsubstacks substacks into $stout ..."
        ec "#-----------------------------------------------------------------------------"
        
        qfile="pmerge.sh"; touch $qfile; chmod 755 $qfile
        sed -e 's|@NODE@|'$node'|'   -e 's|@IDENT@|'$PWD/pmerge'|'  -e 's|@DRY@|'$dry'|'  \
            -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'pmerge.lst'|'  -e 's|@WRK@|'$WRK'|'  \
            -e 's|@STOUT@|'$stout'|'    $bindir/pmerge.sh > ./$qfile
        
        ec "# Built $qfile with $nsubstacks entries"
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi
        
        ec "# submitting $qfile ... "; qsub $qfile
        ec " >>>>   wait for pmerge to finish ...   <<<<<"
        
        btime=$(date "+%s.%N");  sleep 60   # before starting wait loop
        while :; do           #  begin qsub wait loop for pmerge
            njobs=$(qstat -au moneti | grep pmerge_${FILTER} | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished
            sleep 30
        done  
        ec "# pmerge finished - now check exit status"
        
        ngood=$(tail -1 pmerge.out | grep STATUS:\ 0 | wc -l)
        if [ $ngood -ne 1 ]; then
            ec "PROBLEM: pmerge.sh exit status not 0 ... check pmerge.out"
            askuser
        fi
          
		ec "CHECK: pmerge.sh exit status ok ... continue"
        ec "# $stout and associated products built:"
        ls -lrth UVISTA*p2*.*
        ec "# ..... GOOD JOB! "
		mv substack_paw?_??.fits substack_paw?_??_weight.fits swarp_p2
    fi
    

	ec "##------------------------  END OF DR4 PIPELINE ------------------------------"
    ec "#-----------------------------------------------------------------------------"


#-----------------------------------------------------------------------------------------------
#elif [ $1 = 'p7' ]; then      #    -------------- END OF PIPELINE --------------
#-----------------------------------------------------------------------------------------------

   xx=1  # another dummy step ... not sure why

#@@ ------------------------------------------------------------------------------------
#@@  And options for status checking
#@@ ------------------------------------------------------------------------------------

elif [ $1 = 'env'   ]; then   # env:   check environment
   relcheck  


elif [ $1 = 'files' ]; then   # files: list files for an image in current work space
   curfiles


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

    
elif [ $1 = 'fscale'   ]; then   # fluxscale stuff
    fsfile=fluxscale.dat    
    if [ ! -e $fsfile ]; then 
        ec ">> Rebuild $fsfile"
        grep FLXSCALE v20*.head | cut -d\/ -f1 | sed 's/.head:FLXSCALE=//' > $fsfile
    fi
 
    nfs=$(cat $fsfile | wc -l)   
    nun=$(sort -u -k2 $fsfile | wc -l)    # number of unique values
    if [ $nun -le $((2*$nfs/3)) ]; then
        ec "# ATTN $fsfile has $nun unique values of about $(($nimages * 16)) expected"
    fi

    nbad=$(\grep 0.0000000 $fsfile |  wc -l)
    if [ $nbad != 0 ]; then echo "# ATTN: found $nbad chips with FLUXSCALE = 0.00"; fi
    nbad=$(\grep INF $fsfile |  wc -l)
    if [ $nbad != 0 ]; then echo "# ATTN: found $nbad chips with FLUXSCALE = INF"; fi
    res=$(grep -v -e INF -e 0.00000000 $fsfile | tr -s ' ' | cut -d' ' -f2 | awk -f $confdir/awk/std.awk )
    ec "# mean flux scale: $res"


elif [ $1 = 'headinfo'   ]; then   # env:   check environment
    if [ $(pwd | cut -d\/ -f4) != 'RawData' ]; then
        echo " Must be run in RawData dir to work on .fit files"
        exit
    fi
    keys="OBS.ID MJD-OBS DET.DIT DET.NDIT ARCFILE RA DEC ESOGRADE OBS.NAME INS.FILT1.NAME OBS.PROG.ID CASUVERS"
    dfits v20*_00???.fit | fitsort $keys > ../HeadInfo.full

  # #exit 0
  ## ec "##-----------------------------------------------------------------------------"

#-----------------------------------------------------------------------------------------------
else
#-----------------------------------------------------------------------------------------------
   echo "!! ERROR: $1 invalid argument ... valid arguments are:"
   help
fi 

exit 0
#@@ ------------------------------------------------------------------------------------

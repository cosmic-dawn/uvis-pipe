#!/bin/bash 
#-----------------------------------------------------------------------------
# File: pssm.sh 
#-----------------------------------------------------------------------------
# Purpose:   Pipeline to run scamp/swarp/merge; short version of uvis.sh
# Syntax:  pssm.sh ppp {dry}
#          where ppp is a 3-letter "pass" of which 
#          - the first letter can be 'p' for pass or other
#          - the 2nd char is 1 or 2 for pass 1 or 2
#          - ast letter should be 's' or 'm' for single or multiple 
#            (season specific) GAIA reference catalog
#          file. 
# Requires: 
# - work directory with data, given by $WRK env. var.
# - python3, python scripts from terapix pipe adapted to python 3,
#            in ~/softs/uvis-pipe/python etc.
# - wrapper scripts in ~/softs/uvis-pipe/bin
# Author:    A. Moneti
#-----------------------------------------------------------------------------
# Versions:
# v2.00: initial version, from DR5 processing                      (13.jan.22)
# v2.10: adapted for DR6                                           (24.jan.23)
# v2.11: implemented swarp/merge by season/paw                     (15.feb.23)
# v2.12: new processing options; other fixes                       (27.jun.23)
# v2.13: processing options moved to beg fo file, other fixes      (27.jul.23)
# v2.14: processing opts to param file, given on cmd line          (14.aug.23)
#-----------------------------------------------------------------------------

set -u  # exit if a variable is not defined - recommended by Stephane

vers=$(grep '^# v2.' $0 | tail -1 | cut -c 3-7,67-79)

echo 

if [ $# -eq 0 ] || [ $1 == 'help' ] || [ $1 == '-h' ]; then 
    echo "| SYNTAX: "
    echo "| - pssm.sh param-file  {dry}  "
    echo "|   param-file contains the needed processing parameter; no defaults given "
    echo "#-----------------------------------------------------------------------------"
    exit 0
else
    parfile=$1
    if [ ! -e $parfile ]; then
        echo " >> ERROR: param file $1 not found ... quitting"
        echo "#-----------------------------------------------------------------------------"
        exit 9
    fi
fi 

echo "           #=================================================#"
echo "           #                                                 #"
echo "           #            This is pssm.sh                      #"
echo "           #    i.e. pipeline scamp / swarp / merge          #"
echo "           #           $vers                     #"
echo "           #                                                 #"
echo "           #=================================================#"
echo
#-----------------------------------------------------------------------------

if [ -z ${WRK+x} ]; then 
    echo "!! ERROR: must export WRK variable before starting" ; exit 2; 
else
   FILTER=$(echo $WRK | cut -d/ -f5)
fi

if [[ "${@: -1}" =~ 'dry' ]] || [ "${@: -1}" == 'test' ]; then dry=T; else dry=F; fi
if [[ "${@: -1}" =~ 'int' ]]; then int=T;  else int=F;  fi
if [[ "${@: -1}" =~ 'env' ]]; then dry=T; fi

#-----------------------------------------------------------------------------

module() { eval $(/usr/bin/modulecmd bash $*); }
module purge ; module load intelpython/3-2019.4 cfitsio
export LD_LIBRARY_PATH=/lib64:${LD_LIBRARY_PATH}

#-----------------------------------------------------------------------------
# Processing related options read from local pssm.par file
#-----------------------------------------------------------------------------

pass=$(grep  ^pass $parfile  | tr -d ' ' | cut -d = -f2)    # code for pass
runID=$(grep ^runID $parfile | tr -d ' ' | cut -d = -f2)    # Release name 

NewLDACS=$(grep ^NewLDACS $parfile | tr -d ' ' | cut -d = -f2)
doImSel=$(grep  ^doImSel $parfile  | tr -d ' ' | cut -d = -f2)
doHiRes=$(grep  ^doHiRes $parfile  | tr -d ' ' | cut -d = -f2)

cltag=$(grep  ^cl_tag $parfile | tr -d ' ' | cut -d = -f2)

doFull=$(grep ^doFull $parfile | tr -d ' ' | cut -d = -f2)
doSesn=$(grep ^doSesn $parfile | tr -d ' ' | cut -d = -f2)
doPaws=$(grep ^doPaws $parfile | tr -d ' ' | cut -d = -f2)

#-----------------------------------------------------------------------------
# Some not so variable variables
#-----------------------------------------------------------------------------

uvis=/home/moneti/softs/uvis-pipe
bindir=$uvis/bin
pydir=$uvis/python
confdir=$uvis/config
#
imdir=${WRK}/images
bpmdir=/n08data/UltraVista/DR6/bpms

node=$(echo $WRK | cut -c 2-4)         # 

badfiles=$WRK/DiscardedFiles.list      # built and appended to during processing
fileinfo=$WRK/FileInfo.dat             # lists assoc files (bpm, sky, flat) and other info for each image

pipelog=${WRK}/pssm.log ; if [ ! -e $pipelog ]; then touch $pipelog; fi
Trash=zRejected         ; if [ ! -d $Trash ]; then mkdir $Trash; fi
   
#-----------------------------------------------------------------------------
# directoires of input files

scampdir=$WRK/images/scamp_$pass
headsdir=$WRK/images/heads_$pass
swarpdir=$WRK/images/swarp_$pass     # only used in proc_dir below

# directory for products
prod_dir=${swarpdir}                 #initial name

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

askuser() {   # ask user if ok to continue
    echo -n " ==> Is this ok? (yes/no):  "  >> $pipelog
    while true; do read -p " ==> Is this ok? (yes/no): " answer
        echo $answer >> $pipelog
        case $answer in
            [yY]* ) ec "# ==> Continue ..."; break;;
            *     ) ec "# Quitting ..."; exit 3;;
        esac
    done  
}

mkplists() {  # build paw lists
    if [ $(pwd) != $WRK/images ]; then mycd $WRK/images; fi
    list=$1
#    echo "## DEBUG: Build paw lists from $list with $(cat $list | wc -l) files ..."

    file=$(mktemp) 
    paws=" paw1 paw2 paw3 paw4 paw5 paw6 "   # COSMOS"

    rr=$(for f in $(cat $list); do echo -n " -e ^${f%.fits}" ; done)
    grep $rr ../FileInfo.dat | grep -v \#  > $file

    for p in $paws; do 
        olist=list_${p}  
        grep $p $file | cut -d \   -f1 > $olist
    done   #;   head -2 $file    # DEBUG
    rm $file

    for f in list_paw?; do if [ ! -s $f ]; then rm $f; fi; done
    nl=$(ls list_paw? | wc -l)
#    echo "## DEBUG: Built $nl paw lists from $list"
#    echo -n "## DEBUG: len $list "; wc -l $list 
#   echo "## DEBUG: len list_paw? "; wc -l list_paw? 
}

ec "#-----------------------------------------------------------------------------"
ec "# Parameters from local param file $parfile "
ec "#-----------------------------------------------------------------------------"
ec "# - pass       $pass       3-char code for pass; 2nd char is pass number (1/2)"
ec "# - runID      $runID      tag attached to stacks"
ec "# - cltag      $cltag       clean for tarditional skies, cln for alt skies"
ec "# - NewLDACS   $NewLDACS     T/F for ldac processing"
ec "# - doHiRes    $doHiRes     T/F"
ec "# - doImSel    $doImSel     T/F  for image selection"
ec "# - Full       $doFull     T/F  to build full stack"
ec "# - Season     $doSesn     T/F  to build season stacks"
ec "# - Paws       $doPaws     T/F  to build paw stacks"
ec "#-----------------------------------------------------------------------------"

#-----------------------------------------------------------------------------
# Misc. checks
#-----------------------------------------------------------------------------
# check / set params
#-----------------------------------------------------------------------------

if [ $# -eq 8 ]; then
    echo "#-----------------------------------------------------------------------------"
    echo " Running  $(which $0) to process UltraVista data, step "$1
    echo "#-----------------------------------------------------------------------------"
fi

if [ ! -e $WRK/images/list_images ]; then 
    ec "list_images not found - rebuild it ...."
    if [ -e $WRK/images/list_accepted ]; then 
        ec "# First file rejection done; using list_accepted to build list_images"
        cp $WRK/images/list_accepted $WRK/images/list_images
    else
        ec "# Initial run; using list_origs to build list_images"
        cp $WRK/images/list_origs $WRK/images/list_images
    fi
fi

nimages=$(cat $WRK/images/list_images | wc -l)
imroot=$(head -$(($nimages / 2))  $WRK/images/list_images | tail -1 | cut -d\. -f1 | cut -d\/ -f2 )

#-----------------------------------------------------------------------------------------------

mycd $WRK/images
nn=$(ls UVIS*p1.fits UVIS*p1_weight.fits UVIS*p1_ob_flag.fits 2> /dev/null | wc -l )
if [ $nn -eq 3 ]; then
    ec "# Found $nn UVIS*.fits files - looks like P2 has been done already ... quitting"
    exit 0
fi

norigs=$(cat list_origs | wc -l)
naccep=$(cat list_accepted | wc -l)
nldacs=$(cat list_ldacs 2> /dev/null | wc -l)
nwghts=$(cat list_weights | wc -l)

if [ $naccep -eq $nldacs ]; then 
    ec "CHECK: found $naccep images, $nwghts weights, $nldacs ldac files ... " 
    ec "CHECK: ... seems ok to continue with first pass."
else
    ec "!!! ATTN: Number of images, weights, ldacs not the same ..."
    ec "!!! found $naccep images, $nwghts weights, $nldacs ldacs"
    ec "!!! ... continue anyway ..."
fi  

ec "##----------------------------------------------------------------------------"
ec "#"
ec "##          ======  PREPARE FOR SCAMP/SWARP/MERGE  ======"
ec "#"
ec "##----------------------------------------------------------------------------"

if [[ $doHiRes == 'T' ]]; then
    resol=hr
else
    resol=lr
fi
prod_dir=${swarpdir}_${resol}_${runID}

if [ $dry != 'T' ]; then 
    if [ ! -d $headsdir ] ; then mkdir $headsdir ; fi
    if [ ! -d $scampdir ] ; then mkdir $scampdir ; fi
fi

# for p1, use origs, for p2 used cleaned
if [ ${pass:1:1} -le 1 ]; then
    list=list_origs
    root=$(head $list | tail -1 | cut -c1-15)
    ex=$(ls -l origs/${root}.fits 2> /dev/null | tr -s ' ' | cut -d' ' -f10 )
else 
    sed -e 's/_'${cltag}.'/./' cleaned/list_cleaned > list_cleaned
    list=list_cleaned 
    root=$(head $list | tail -1 | cut -c1-15)
    ex=$(ls -l cleaned/${root}_${cltag}.fits 2> /dev/null | tr -s ' ' | cut -d\  -f9 )
fi

if [ -e list_special ]; then 
    ec "# ATTN: found list_special"
    ec "##----------------------------------------------------------------------------"
    list=list_special
    root=$(head $list | tail -1 | cut -c1-15) 
    ex=$(ls -lL origs/${root}.fits | tr -s ' ' | cut -d' ' -f9 )
fi

nf=$(cat $list | wc -l)
if [ $nf -eq 0 ]; then
    ec "#### ERROR: empty list $list ... quitting"
    exit 9
fi

ec "# pass is .............. $pass"
ec "# cleaned files tag .... $cltag "
ec "# input list is ........ $list with $nf entries"
ec "# input image files .... \$DR6/${FILTER}/images/$ex"
ec "# input ldac files ..... \$DR6/$(ls -lL $WRK/images/ldacs/${root}.ldac | cut -d\/ -f5-9)"
ec "# scamp logs etc in .... \$DR6/$(ls -ld $scampdir | cut -d\/ -f5-9)"
ec "# head files in ........ \$DR6/$(ls -ld $headsdir | cut -d\/ -f5-9)"
ec "# swarp products in .... \$DR6/$(echo ${prod_dir} | cut -d\/ -f5-9)"
#ec "# swarp products in .... \$DR6/${prod_dir}"
ec "##----------------------------------------------------------------------------"
ec "# New ldacs? ........... $NewLDACS"
ec "# Do image selection ... $doImSel"
ec "# Do hi-res stacks ..... $doHiRes"
ec "##----------------------------------------------------------------------------"
ec "# ==========> Continue?   "; sleep 3   # time to check info

#----------------------------------------------------------------------------------------#
#       scamp
#----------------------------------------------------------------------------------------#
# check whether scamp has already been run ... 

if [ -e $scampdir/pscamp_s01a.log ] ; then
    ec "CHECK: scamp logfiles found in $scampdir and found head files ..." 
    ec "CHECK:  ==> scamp already done skip to swarp "
    ec "#-----------------------------------------------------------------------------"
else
    ec "#-----------------------------------------------------------------------------"
    ec "##              ======  BEGIN SCAMP  ======"
    ec "#-----------------------------------------------------------------------------"

    # special case to process only new ldac files, i.e., build head files
    if [ -e list_special ]; then # convert to list_ldacs
        ec "# ATTN: using list_special"
        sed 's/fits/ldac/' list_special > list_ldacs
    fi

    nl=$(cat list_ldacs | wc -l)  # total num of files to process
    ec "CHECK: found $nl ldac files to process"
    if [ $nl -eq 0 ]; then exit; fi
    if [ $nl -lt 100 ]; then # set walltime - in hrs!!
        wtime=4    # useful in testing
    else 
        wtime=12
    fi

    # split by season, then into chunks of about 500 files
    # build list of all ldacs first then split it:

    rm -f pscamp_s???.lst             # delete residuals ones if any
    nl=$(cat list_ldacs | wc -l)      # total num of files to process

    # first build seasons list for scamp, i.e. all files ... for each file, a line with
    # root  season# (1-14)
    ec "# build seasons lists"
    cut -c1-15 list_ldacs > names ; cut -c2-5 list_ldacs > yy ; cut -c6-7 list_ldacs > mm 
    paste names yy mm > dates
    awk '{printf "%s   %4i \n" ,$1, $2-2009 + $3/10}' dates > seasons_for_scamp 
    rm mm yy dates names


    # Build ldac lists by season and split if large
    for y in $(seq 14); do  
        if [ ${y} -le 9 ]; then z=0$y; else z=$y; fi 
        slist=list_s${z}
        plist=pscamp_s${z}
        tt=$(mktemp)
        grep \ ${y}\  seasons_for_scamp | cut -d\  -f1  > $tt
        awk '{printf "%s.ldac\n", $1}' $tt > $slist
        if [ ! -s $slist ]; then 
            rm -f $slist ;  ec "###  remove empty $slist ###"
        else
            nf=$(cat $slist | wc -l)
            ec "# $slist contains $nf files"
            nmax=2200
            if [ $nf -gt $nmax ]; then
                # split the list
                nc=$(($nf/$nmax + 1))
                split -a 1 -n l/$nc $slist --additional-suffix='.lst' $plist
                rm $slist
            else
                mv $slist ${plist}a.lst
            fi
        fi
        rm $tt 
    done

    #-----------------------------------------------------------------------------
    # Select files to use as photref and build the needed .ahead files (links):
    # If first run, find files with FWHM close to median value, and set them as
    # photref= T use files with 0.71 < fwhm < 0.73, where 0.72 is about the
    # median value.  Otherwise select the files with ZPcorr close to 0.0 from
    # the ZPcorr.dat file
    #-----------------------------------------------------------------------------
    ec "# Build .ahead files for photometric reference files (links)"

    if [ ! -e ZPcorr.dat ]; then 
        cut -c1-22 PSFsel.dat | grep -e v2010 -e v2023 > PSFtmp.dat     ##############
        photlist=$(grep \ 0.7[123]$  PSFtmp.dat | cut -d\  -f1)  # list of files with median fwhm
        ec "# Using $(echo "$photlist" | \wc -w) files with FWHM near 0.72 to set as photref"
#       rm PSFtmp.dat
        # also build ZPcorr for future runs
        if [ -e  scamp_p1m/pscamp_z1m.dat ]; then 
            cut -c1-16,30-40 scamp_p1m/pscamp_p1m.dat | grep -v File > ZPcorr.dat
        fi
    else
        photlist=$(awk '{if ($2 > -0.002 && $2 < 0.002) print $0}' ZPcorr.dat | cut -c1-15)
        ec "# Using $(echo "$photlist" | \wc -w) files with ZPcorr near zero to set as photref"
    fi
    # build photref files from those with ZPcorr from previous scamp run close to 0
    for f in grep $photlist; do 
        ln -sf $confdir/photom_ref.ahead ${f}.ahead
    done
    ec "# ==> Done $(ls v20*.ahead | wc -l) photref .ahead files"
    ec "#-----------------------------------------------------------------------------"

#   echo "#  DONT BUILD psub FILES "; exit   ##########################

    # Build qsub files
    IMDIR=$WRK/images
    rm -rf $IMDIR/pscamp.submit  
    for plist in pscamp_s???.lst; do
        ptag=$( echo $plist | cut -c8-11)  # tag to build output file names

        rm -f $IMDIR/pscamp_$ptag.out $IMDIR/pscamp_$ptag.log $IMDIR/pscamp_$ptag.sh   
        nn=$(cat $plist | wc -l)

        qfile=$WRK/images/pscamp_$ptag.sh; touch $qfile; chmod 755 $qfile
        sed -e 's|@NODE@|'$node'|'     -e 's|@IDENT@|'$PWD/pscamp_$ptag'|'  -e 's|@DRY@|'$dry'|'  \
            -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'$plist'|'    -e 's|@WRK@|'${WRK}'/images|'  \
            -e 's|@WTIME@|'$wtime'|'   -e 's|@PTAG@|'$ptag'|'  -e 's|@PASS@|'${pass}'|' \
            $bindir/pscamp.sh > $qfile
    
        if [ $nn -lt 100 ]; then    # short (test) run - decrease resources
            sed -i -e 's|ppn=22|ppn=8|' -e 's|time=48|time=06|' $qfile
        fi

        ec "# Built $qfile for $plist with $nn entries"
        echo  "qsub $qfile ; sleep 1" >> $IMDIR/pscamp.submit
    done
    njobs=$(cat ./pscamp.submit | wc -l)

    ec "# ==> Built pscamp.submit with $njobs entries"
    ec "#-----------------------------------------------------------------------------"

    if [ $dry == 'T' ]; then 
        ls v20*.ahead > list_ahds
        for qfile in $(cat $WRK/images/pscamp.submit | cut -d\  -f2); do
            qq=$(echo $qfile | cut -d\/ -f7)   # filename w/o path
            tag=${qq:7:4} 
            nls=$(cat pscamp_${tag}.lst | wc -l)
            # find number of photref files for each list:
            for f in $(cut -c1-15 pscamp_${tag}.lst ); do grep $f list_ahds 2> /dev/null ; done > ${tag}.aheads
            naheads=$(cat ${tag}.aheads 2> /dev/null | wc -l) 
            rm ${tag}.aheads 
            ec "#   >> BEGIN dry-run of $qq with $nls files and $naheads photrefs << "
            if [ $qq == 'pscamp_s01a.sh' ]; then $qfile dry; echo ; fi
        done
        rm list_ahds #s???.aheads
        ec "#   >> FINISHED dry-run of $0 - stopped after scamp .... << "
        exit 0
    fi
    rm -f list_ahds
    #-----------------------------------------------------------------------------
    ec "# Now for real work ...."
    #-----------------------------------------------------------------------------

    ec "# - Clean up: rm flxscale.dat v20*.head"
    rm -rf fluxscale.dat v20*.head PSFtmp.dat

    #-----------------------------------------------------------------------------
    # submit jobs and wait for them to finish
    #-----------------------------------------------------------------------------

    ec "# - Submitting $njobs pscamp_s??? jobs ..."

    source $IMDIR/pscamp.submit
    ec " >>>>   wait for pscamp to finish ...   <<<<<"
    
    nsec=30  # wait loop check interval
    btime=$(date "+%s"); sleep 20   # before starting wait loop
    while :; do              #  begin qsub wait loop for pscamp
        ndone=$(ls $IMDIR/pscamp_s???.out 2> /dev/null | wc -l)
        [ $ndone -eq $njobs ] && break               # jobs finished
        sleep $nsec
    done  
    chmod 644 pscamp_s???.out

    ec "# $njobs pscamp_s??? jobs finished, walltime $(wtime) - now check exit status"
    ngood=$(grep STATUS:\ 0 pscamp_s???.out | wc -l)
    if [ $ngood -ne $njobs ]; then
        ec "PROBLEM: pscamp.sh exit status not 0 ... check pscamp.out"
        askuser
    fi

    #-----------------------------------------------------------------------------
    # check products
    #-----------------------------------------------------------------------------
    # check number of .head files produced
    nheads=$(ls -1 v20*.head | wc -l)
    if [ $nheads -lt $nl ]; then
        ec "PROBLEM: built only $nheads head files for $nl ldacs ... "
        askuser
    fi

    #-----------------------------------------------------------------------------
    # WARNINGs in scamp log files 
    #-----------------------------------------------------------------------------

    grep WARNING pscamp_s*.log | grep -v -e FLAGS -e ATLAS | sed 's/have non-zero/n-z/' > scamp_${pass}.warn
    nwarn=$(cat pscamp_${pass}.warn 2> /dev/null | wc -l)

    if [ $nwarn -ge 1 ]; then 
        ec "# ATTN: $nwarn warnings found in scamp logfiles ... see scamp_${pass}.warn"
        ec "# The following files have warnings:"
        sort -k8,8 -u scamp_${pass}.warn | cut -d\  -f8
    fi   

    # summary of scamp astrometric accuracy:
    # Modif to make it work with single logfile: make a dummy logfile, and remove it later
    cp $(ls pscamp_????.log | tail -1) pscamp_wxyz.log    # dummy logfile
    grep -A4 Astrometric\ stats\ \(ext  pscamp*.log | grep Group | cut -c8-11,26-99 | tr -d \" > x
    echo "    Summary of scamp_${pass} astrometric accuracy " > scamp_${pass}.summ
    echo " group  dAXIS1  dAXIS2   chi2  ndets | dAXIS1  dAXIS2   chi2  ndets" >> scamp_${pass}.summ
    awk '{printf " %s   %6.4f  %6.4f %6.1f %6i | %6.4f  %6.4f %6.1f %6i\n",$1,$2,$3,$4,$5,$6,$7,$8,$9}' x |\
         grep -v wxyz >> scamp_${pass}.summ
    rm pscamp_wxyz.log                 # delete dummy logfile
    cat scamp_${pass}.summ
    

    # check fluxscale table built by pscamp script
    ec "#-----------------------------------------------------------------------------"
    ec "#       Scamp flux-scale results "
    ec "#-----------------------------------------------------------------------------"
    mycd $WRK/images
    fsfile=fluxscale.dat    
#    grep FLXSCALE v20*.head | cut -d\/ -f1 | sed 's/.head:FLXSCALE=//' | sort -k1 -u | \
#        awk '{printf "%-16s %10.6f %8.4f \n", $1, $2, 2.5*log($2)/log(10) }' > $fsfile
    # 9.feb.23: remove sort -u (incorrect anyway) and look for 0.0 and INF before continuing
    grep FLXSCALE v20*.head | cut -d\/ -f1 | sed 's/.head:FLXSCALE=//' > $fsfile
    nfs=$(cat $fsfile | wc -l)   
    if [ $nfs -eq 0 ]; then
        ec "#### ATTN: $fsfile empty!!  FLXSCALE kwd not written by scamp??"
    else
        nun=$(sort -u -k2 $fsfile | wc -l)    # number of values
        if [ $nun -lt $((16*$nfs)) ]; then
            ec "#### ATTN: $fsfile has $nun values of about $(($nimages * 16)) expected"
        fi
        
        nbad=$(\grep 0.0000000 $fsfile |  wc -l)
        if [ $nbad != 0 ]; then echo "#### ATTN: found $nbad chips with FLUXSCALE = 0.00"; fi
        nbad=$(\grep INF $fsfile |  wc -l)
        if [ $nbad != 0 ]; then echo "#### ATTN: found $nbad chips with FLUXSCALE = INF"; fi
        nbad=$(\grep .....[0-9][0-9][0-9]\.000000\ [0-9] $fsfile |  wc -l)
        if [ $nbad != 0 ]; then echo "#### ATTN: found $nbad chips with FLUXSCALE = huge unphysical value"; fi
        
        res=$(grep -v -e INF -e 0.00000000 -e .....[0-9][0-9][0-9]\.000000\ [0-9] $fsfile | tr -s ' ' | \
            cut -d' ' -f2 | awk -f $uvis/scripts/std.awk )
        ec "# mean fluxscale of regular value: $res"
    fi

    #-----------------------------------------------------------------------------
    # Combine pscamp_s???.dat files into single global one, and select problem files:
    #-----------------------------------------------------------------------------

    echo "# File            contrast    ZPcorr chi2-int   chi2-ref   x-shift  y-shift     shift" > pscamp_${pass}.dat
    grep -v File pscamp_s???.dat | cut -d\: -f2 >> pscamp_${pass}.dat

    grep v20 pscamp_${pass}.dat | awk '{if ($2 < 35) print $0}'      >> low_contrast.dat
    grep v20 pscamp_${pass}.dat | awk '{if ($3*$3 > 0.25) print $0}' >> large_ZPcorr.dat

    cut -c1-15 low_contrast.dat | grep v20 > low_contrast.lst
    cut -c1-15 large_ZPcorr.dat | grep v20 > large_ZPcorr.lst

    ec ">> Found $(cat low_contrast.dat | wc -l) files with scamp contrast < 35"
    ec ">> Found $(cat large_ZPcorr.dat | wc -l) files ZP-corr > 0.50 mag"
    cat low_contrast.dat large_ZPcorr.dat | sort -k1,1 -u > scamp_issue.dat

    ec ">> ... for a total of $(cat scamp_issue.dat | wc -l) files with possible problem"
    ec ">> ... see scamp_issue.dat for details; Number of issues per season:"
    ec "# season  N(lc)  N(ls)   N(lz)"
    for y in $(seq 2010 2023); do z=$(($y-1)); 
        ss="${z:2:2}-${y:2:2}"
        lc=$(grep -e v${z}1 -e v${y}0 low_contrast.lst | wc -l)  
        lz=$(grep -e v${z}1 -e v${y}0 large_ZPcorr.lst | wc -l)
        echo "$ss $lc $lz" | awk '{printf "# 20%-6s %5i %6i\n", $1,$2,$3}' | tee -a $pipelog
    done


    ec "#-----------------------------------------------------------------------------"
    ec "CHECK: pscamp.sh successful, $nheads head files built ... clean-up and continue"
    mv v20*.head $headsdir

    mv [fgipr]*_s???_$FILTER.png pscamp_s???.* fluxscale.dat pscamp.submit $scampdir
    mv scamp_${pass}.warn pscamp_${pass}.dat $scampdir
    mv low_contrast.* large_ZPcorr.* scamp_issue.dat $scampdir
    rm v20??????_?????.ahead      # links
    ec "#-----------------------------------------------------------------------------"

    if [ $NewLDACS == "T" ]; then
        echo "    ########  QUIT HERE AFTER SCAMP FOR NEW LDACS  #######" ; exit 0
    fi
fi

#echo "    ########  QUIT HERE AFTER SCAMP FOR NOW  #######" ; exit 0
#-----------------------------------------------------------------------------

if [[ $doHiRes == 'T' ]]; then 
    ec "## Skipping image selection for hi-res stacks"
    doImSel=F
fi

if [ "$doImSel" != "T" ]; then
    slist=$list
    ns=$(cat $slist | wc -l)
    ec "#### ATTN: SKIPPING IMAGE SELECTION #### all $ns files used"
    ec "#-----------------------------------------------------------------------------"
else
    ec "# Image selection:  "
    ec "#-----------------------------------------------------------------------------"
    
    #----------------------------------------------------------------------------------------#
    #       Image selection:
    # ####  NB. Done also in DRY mode  #####
    #----------------------------------------------------------------------------------------#
    # build list_toSwarp by removing rejected files from list_images.  Rejected files are:
    # - files with large ZP-corr from scamp
    # - files with low contrast from scamp
    # - files with large / elongated PSF
    # - files rejected (grade C) in DR1
    #----------------------------------------------------------------------------------------#
    
    mycd $WRK/images
    slist=list_toSwarp_$pass      # new list for swarping; $list remains the input list 
    
    mycd $scampdir
    # ATTN: in DR6 large shifts are OK
    ec "## Found $(cat large_ZPcorr.lst | wc -l) files with large ZP correction from scamp to exclude ..."
    ec "## Found $(cat low_contrast.lst | wc -l) files with low contrast from scamp to exclude ..."
    
    # ATTN: in DR6, PSFsel.dat is a 7 column file
    awk '/v20/{if ($2 > 1.0 || $5 > 0.1) printf "%-18s %6.3f  %6.4f\n", $1, $2, $3 }' ../PSFsel.dat | cut -c1-15 > badPSF.lst
    ec "## Found $(cat $scampdir/badPSF.lst | wc -l) files with bad PSF to exclude ..."
    # grade C files from DR1
    grep \,C\, $confdir/DR1_file_info.txt | grep $FILTER | sed 's/, /; /g' | cut -d\, -f1,6,8,12-20 | tr \, \  > badDR1.dat
    cut -d\  -f1 badDR1.dat  > badDR1.lst
    ec "## Found $(cat badDR1.lst | wc -l) DR1 grade C files to exclude ..."
    
    cat badPSF.lst badDR1.lst large_ZPcorr.lst low_contrast.lst | cut -c1-15 | sort -u > ../toRemove.lst
    
    cd ..
    
    # remove the rejected files
    rr=$(for f in $(cat toRemove.lst); do echo -n " -e $f" ; done)
    grep -v $rr $list | sed 's/_'${cltag}'//' > $slist 
    nslist=$(cat $slist | wc -l)         # files to swarp
    
    # $nimages should be length of list_accepted or cleaned or special
    # some files rejected here were already removed, and not in cleaned
    nRemoved=$(cat toRemove.lst | wc -l)
    ec ">> Image selection results: "
    ec ">> - removed $nRemoved files from $list; "
    ec ">> - $nslist files left to swarp in $slist"
    ec "#-----------------------------------------------------------------------------"
fi

#echo "    ########  QUIT HERE AFTER IMAGE SELECTION FOR NOW  #######" ; exit 0

#========================================================================================#   

#----------------------------------------------------------------------------------------#
#       swarp 
#----------------------------------------------------------------------------------------#

#   The list of images to swarp is split by season and then by paw and then again in
# chunks of max NNN files, and a separate run of swarp is done for each sublist. Each
# run is done in its own, temprary directory.  The links to the input data files, to the
# control/config files, and to the products (substacks) are created directly there by
# the wrapper script.  A first run (of swarp and merge) is done to build the low-res
# stack, and a second one follows automatically for the high-res one.  When done, it's
# the end of the pipeline.  

#   The temporary directories are built in the /scratch space of the node on which the
# job lands.  These have a minimum size of 4.4 TB (for n08 and n17, which have even
# smaller scratch spaces, the temporary directory is built in the /nXXdata).  Each job
# requires 0.65 GB of disk space for the resampled input files (signal and weight) to
# build the low-res stack, and 4x as much, 2.6 GB, for the hi-res stacks.  This means
# that a total of ~1700 hi-res resampled files can be stored on the smallest /scratch
# areas, which are on nodes n19-n22.  So to limit the numb
#
# DR6: for testing purpose, do a pass 1 'p1' stack from the CASU data, after
# DR5-like image selection (hence set pass = 1 just below)
#
# ATTN: input list files are like v20nnmmdd_nnnnn.fits for both pass 1 and 2; 
#       it's pswarp that then links those to either the origs or the cleaned files.
#----------------------------------------------------------------------------------------#

#-----------------------------------------------------------------------------
# check for final products:
#-----------------------------------------------------------------------------

##### NOT SURE WHAT WAS THE PURPOSE OF THIS CHECK HERE ....
## ls UVISTA_${pass}-${FILTER}_full_${resol}*.fits 2> /dev/null | grep -v weight > stacks_done.txt
## nstacks_done=$(cat stacks_done.txt | wc -l)
## if [ $nstacks_done -ge 1 ]; then
##  ec "## ATTN: found $nstacks_done full stack(s) ..."
##  cat stacks_done.txt
##  askuser
## else
##  ec "# CHECK: NO full stacks found ... "
## fi
#######################################################
# name of directory for substacks products
#prod_dir=${prod_dir}_${resol}_${runID}

# see if there are the expected number of scripts there
nscripts=$(ls $prod_dir/pswarp*.sh 2> /dev/null | wc -l)

#-----------------------------------------------------------------------------
# check if swarp alreay done:
#-----------------------------------------------------------------------------

nsubima=$(ls -1 $prod_dir/substack_paw?_s???.fits 2> /dev/null | wc -l)   
#echo $prod_dir $nscripts $nsubima ; exit

if [[ $nsubima -eq $nscripts  &&  $nsubima -gt 0 ]]; then   # || [ -e $stout.fits ]; then 
    ec "CHECK: Found $nsubima substacks for $nscripts expected - swarp done ..."
    ec "#-----------------------------------------------------------------------------"
else 
    nn=$(cat $slist 2> /dev/null | wc -l)     # number of images to swarp

    ec "##  Prepare swarp pass ${pass} using $slist with $nn images"
    ec "#-----------------------------------------------------------------------------"
    nout=$(ls -1 pswarp_paw?_??.out 2> /dev/null | wc -l)
    nlog=$(ls -1 pswarp_paw?_??.log 2> /dev/null | wc -l)
    if [ $nout -ge 1 ] || [ $nlog -ge 1 ]; then
        ec "#### ATTN: found $nout pswarp_paw?_??.out and $nlog pswarp_paw?_??.log files ... delete them and continue??"
        askuser
    fi

    rm -f qall estats pswarp_paw?_s???.lst  pswarp_paw?_s???.sh     # just in case
    rm -f pswarp_paw?_s???.out  pswarp_paw?_s???.log  pswarp.submit

    # look for old temporary work dirs that were not removed
    ls -d /*/psw*_$FILTER 2> /dev/null > odirs.tmp
    ndirs=$(cat odirs.tmp | wc -l ) 
    if [ $ndirs -gt 0 ]; then
        ec "### ATTN: Found $ndirs pswarp work directories ... see -/images/odirs.tmp".
        ec "###       yes to delete them, then continue"
        askuser
    fi
    for d in $(cat odirs.tmp); do rm -rf $d; done  ;  rm odirs.tmp
    
    # build directory for substacks if it does not exists, or reuse current
    if [ ! -d $prod_dir ]; then 
        mkdir $prod_dir 
    else
        ec "# ATTTN: $prod_dir already exists ... reuse?"
        askuser
    fi

    if [ $doHiRes != 'T' ]; then
        ec "# First build low-res (0.6 arcsec/pix) ${pass} substacks ..."
        resol=lr
    else
        ec "# Now build high res (0.30 arcsec/pix) ${pass} substacks ..."
        resol=hr
    fi

    headfile=cosmos_${resol}.head   # = std4G.trim.head;  improved cosmos.head
    stout=UVISTA_${pass}-${FILTER}_full_${resol}_${runID}          # name of full stack

    if [ ${pass:1:1} -le 1 ]; then
        subsky=Y
    else
        subsky=N                             # for pass2 DO NOT subtract sky
    fi

    ec "#-----------------------------------------------------------------------------"
    ec "# output like: $stout"
    ec "# head-file:   $headfile"
    ec "# subsky:      $subsky"
    ec "# input list:  $slist"     #; wc $list
    ec "# ... begins with $(head -1 $slist)"
    ec "# hi/lo resol: $resol"
    ec "#-----------------------------------------------------------------------------"
    ec "# ==========> Continue?   "; sleep 3   # time to check info

    # each file, when resampled, produces 16 resampled files of 21MB each (lr) / 84 MB 
    # (hr). Images and weights are resampled, thus each image in list produces
    # 673MB (lr) / 2.7GB (hr) of data per input file. 

    # build seasons list for swarp: i.e. from new $list = list_toSwarp
    # actual season is deduced from the date info in the filenames
    ec "# build seasons_for_swarp from $list"
    cut -d\  -f1 $slist > names ; cut -c2-5 $slist > yy ; cut -c6-7 $slist > mm 
    paste names yy mm > dates
    awk '{printf "%s   %4i \n" ,$1, $2-2009 + $3/10}' dates > seasons_for_swarp
    rm mm yy dates names

    # Build image lists by season and split if large
    for y in $(seq 14); do  
        if [ ${y} -le 9 ]; then z=0$y; else z=$y; fi 
        ilist=list_s${z}         # image list for season y
        tt=$(mktemp)
        grep \ ${y}\  seasons_for_swarp | cut -d\  -f1  > $tt
        awk '{printf "%s\n", $1}' $tt > $ilist
        if [ ! -s $ilist ]; then 
            rm -f $ilist ;  ec "###  remove empty $ilist ###"
        else
            nf=$(cat $ilist | wc -l)
            ec "# $ilist contains $nf files"
        fi
    done
    rm $tt
    ec "# ==> Season lists contain $(wc -l list_s?? | grep total | tr -s \  | cut -d\  -f2) files "


    # Now loop over lists by season (pswar_s{nn}{ax}.lst 
    ec "##  Begin loop over seasons ... "

    for imlist in list_s??; do 
        tail=${imlist: -3}                  # season suffix to use in script names (like s??)
        nimages=$(cat $imlist | wc -l)      # num images in list
        ec "#-----------------------------------------------------------------------------"
        ec "## Season $tail, list is $imlist with $nimages files"  #, $npaws paws: Build qsub files"

        mkplists $imlist                    # split season list by paws; build list_paw{n}
        npaws=$(ls list_paw? 2> /dev/null | wc -l)
        ec "## ... has $npaws paws: Build qsub files"

        # -----------  Check inputs  -----------

        ec "## Will use files like:"
        imroot=$(head $WRK/images/$slist | tail -1)     
        imroot=${imroot:0:15}
        if [ ${pass:1:1} -le 1 ]; then
            ex=$(ls -l origs/${imroot}.fits | tr -s ' ' | cut -d' ' -f9-13)
        else 
            ex=$(ls -l cleaned/${imroot}_${cltag}.fits | tr -s ' ' | cut -d' ' -f9-13)
        fi

        
        ec "  - images .... $ex" 
        ex=$(ls -l weights/${imroot}_weight.fits | tr -s ' ' | cut -d' ' -f9-13)
        ec "  - weights ... $ex" 
        ex=$(ls -l $headsdir/${imroot}.head | tr -s ' ' | cut -d' ' -f9-13 | cut -d\/ -f7-9)
        ec "  - heads ..... $ex" 

        # -----------  Finished setting up -----------

        if [ $resol == 'lr' ]; then
            nim=1200  # need 0.65 GB/file for lr stacks; 
        else
            nim=400   # need 2.58 GB/file for hr stacks; with some luck this should work: largest split requires 0.87 TB
        fi

        for plist in list_paw[1-6]; do  
            nl=$(cat $plist | wc -l)
            ppaw=$(echo $plist | cut -d\_ -f2)       # NEW tmporary name for full paw
            split -a1 -n l/$(($nl/$nim+1)) $plist --additional-suffix='.lst' pswarp_${ppaw}_${tail}
            for slist in pswarp_${ppaw}_${tail}?.lst; do
                nl=$(cat $slist | wc -l)    
                paw=$(echo $slist | cut -d\_ -f2-3 | cut -d\. -f1)   
                outname=substack_${paw}
                ppn=9
#                ec "DEBUG:  For paw $paw, $nl images ==> $outname with subsky $subsky"

                # ---------------------- Local run by sublist ----------------------
                
                qfile="pswarp_$paw.sh"; touch $qfile; chmod 755 $qfile
                sed -e 's|@NODE@|'$node'|'  -e 's|@IDENT@|'$PWD/pswarp'|'  -e 's|@DRY@|'$dry'|'  \
                    -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'$slist'|'  -e 's|@WRK@|'$WRK'|' \
                    -e 's|@PAW@|'$paw'|'  -e 's|@PASS@|'${pass:1:1}'|'  -e 's|@HEADFILE@|'$headfile'|'  \
                    -e 's/@SUBSKY@/'$subsky'/'  -e 's|@PPN@|'$ppn'|'  -e 's|@HEADSDIR@|'$headsdir'|'  \
                    $bindir/pswarp.sh > $qfile
                if [ ${pass:1:1} -eq 2 ]; then
                    sed -i 's/@CLTAG@/'$cltag'/' $qfile
                fi
            
                ec "# Built $qfile, uses $slist with $nl images to build $outname.fits"
                echo "qsub $qfile; sleep 1" >> pswarp.submit
            done
        done 
        rm list_paw?
    done

    ec "#-----------------------------------------------------------------------------"
    njobs=$(cat pswarp.submit | wc -l)
    nfils=$(wc -l pswarp_paw*.lst | grep total | tr -s \  | cut -d\  -f2)
    
    ec "# ==> written to file 'pswarp.submit' with $njobs entries for $nfils files"
    ec "# ==> torque params: $(grep ppn= $qfile | cut -d\  -f3)"
    ec "#-----------------------------------------------------------------------------"
    if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi

    ec "# Submit qsub files ... ";  source pswarp.submit
    ec " >>>>   Wait $njobs pswarp jobs ... first check in 1 min  <<<<<"

    btime=$(date "+%s.%N");  sleep 60           # begin time
    while :; do           #  begin qsub wait loop for pswarp
        ndone=$(ls $WRK/images/pswarp_paw?_s???.out 2> /dev/null | wc -l)
        [ $njobs -eq $ndone ] && break          # jobs finished
        sleep 15
    done  
    ec "#-----------------------------------------------------------------------------"
    ec "# pswarp finished; walltime $(wtime)"
    
    # check exit status
    grep EXIT\ STATUS pswarp_paw?_s???.out > estats
    nbad=$(grep -v STATUS:\ 0  estats | wc -l)  # files w/ status != 0
    if [ $nbad -gt 0 ]; then
        ec "PROBLEM: pswarp_paw?_s???.sh exit status not 0: "
        grep -v STATUS:\ 0 estats 
        askuser
    else
        ec "CHECK: pswarp_paw?_s???.sh exit status ok"; rm estats
    fi

    # check num substacks found
    nn=$(ls substack_paw?_s???.fits | wc -l)
    if [ $nn -lt $njobs ]; then
        ec "PROBLEM:  found only $nn substacks for $njobs expected ..."
        askuser 
    fi

    # check sizes of sustacks
    ns=$(\ls -l substack_paw?_s???.fits | \
        tr -s ' ' | cut -d ' ' -f5,5 | sort -u | wc -l)
    if [ $ns -gt 1 ]; then 
        ec "PROBLEM: substacks not all of same size .... "
        ls -l substack_paw?_s???.fits 
        askuser
    fi

    # check for WARNINGS in logfiles
    grep WARNING pswarp_paw?_s???.log | grep -v FITS\ header > pswarp.warn
    if [ $(cat pswarp.warn | wc -l) -gt 1 ]; then 
        ec "#### ATTN: found warnings in pswarp logfiles"
    fi
    
    chmod 644 substack_*.* pswarp_paw*.log pswarp*.out

    mv -f pswarp*.sh pswarp.warn pswarp_paw?_s???.???  list_s?? $prod_dir
    mv substack_paw?_s???.fits  substack_paw?_s???_weight.fits  $prod_dir
    cp $confdir/swarp238.conf $prod_dir
    rm -f pswarp.submit substack*.head  # name built in script

    ec "#-----------------------------------------------------------------------------"
fi

#----------------------------------------------------------------------------------------#
#          Merge p2 substacks
#----------------------------------------------------------------------------------------#

if [ $doHiRes != 'T' ]; then
    ec "# Merge low-res (0.6 arcsec/pix) ${pass} substacks into final stacks"
    resol=lr
else
    ec "# Merge high res (0.30 arcsec/pix) ${pass} substacks into final stacks"
    resol=hr
fi

headfile=cosmos_${resol}.head   
#########    ATTN: need option for 48k stacks    #########

#stout=UVISTA_${pass}-${FILTER}_full_${resol}_${runID}          # name of full stack
#if [ -e $WRK/images/$stout.fits ]; then 

stacks_done=$(ls UVISTA_${pass}-${FILTER}_full_${resol}_${runID}.fits)
nstacks_done=$(cat stacks_done 2> /dev/null | wc -l)

if [ $nstacks_done -ge 1 ]; then
    ec "#CHECK: some final stacks already built:"
    cat stacks_done.txt
    ec "# Nothing to do? ..."
    askuser 
else 
    # delete remaining files, if any
    rm -f pmerge_*.??? pmerge_*.sh pmerge.submit

    nsubstacks=$(ls -1 $prod_dir/substack_paw?_s???.fits 2> /dev/null | wc -l)
    if [ $nsubstacks -eq 0 ]; then
        ec "ERROR: no substacks found - quitting"; exit 2
    fi

    ec "## - R17:  pmerge.sh: Merge $nsubstacks substacks into final stacks ..."
    ec "#-----------------------------------------------------------------------------"

    # make links to substacks
    ln -sf $prod_dir/substack_paw?_s???.fits .
    ln -sf $prod_dir/substack_paw?_s???_weight.fits .
    
    # check num of substacks vs num expected:
    ls -1 substack_paw?_s???.fits > pmerge_full.lst 2> /dev/null # if season stack are available
    nfnd=$(cat pmerge_full.lst | wc -l)                   # number found
    nexp=$(ls $prod_dir/pswarp_paw?_s???.sh | wc -l)     # number expected
    
    if [ $nfnd -ne $nexp ]; then
        ec "ERROR: found $nfnd substacks for $nexp expected from pswarp*.sh files .... "
        askuser
        exit 2
    fi
    
    #----------------------------------------------------------------------------------------#
    #          Merge all substacks into full stack
    #----------------------------------------------------------------------------------------#
    if [ $doFull == 'T' ]; then 
        stout=UVISTA_${pass}-${FILTER}_full_${resol}_${runID} 
        ec "# Merge all $nfnd substacks into $stout ..."
        ec "#-----------------------------------------------------------------------------"
        
        qfile="pmerge_full.sh"; touch $qfile; chmod 755 $qfile
        sed -e 's|@NODE@|'$node'|'   -e 's|@IDENT@|'$PWD/pmerge_full'|'  -e 's|@DRY@|'$dry'|'  \
            -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'pmerge_full.lst'|'  -e 's|@WRK@|'$WRK'|'  \
            -e 's|@STOUT@|'$stout'|'   -e 's|@TAG@|full|'              -e 's|@PASS@|'$pass'|'  \
            $bindir/pmerge.sh  > ./$qfile
        
        ec "# . Built $qfile and pmerge_full.lst with $nfnd entries"
        ec "#-----------------------------------------------------------------------------"
        echo "qsub $qfile; sleep 1" >> pmerge.submit
    fi
        
    #----------------------------------------------------------------------------------------#
    #          Merge substacks by season into seasons stacks
    #----------------------------------------------------------------------------------------#

    if [ $doSesn == "T" ]; then
        ec "## Build season stacks from substacks:"
        ec "#-----------------------------------------------------------------------------"
        rm -rf pmerge_s??.lst
        seasons=$(ls substack_paw?_s???.fits | cut -c16-17 | sort -u)
#        echo "## DEBUG: Found these seasons: $seasons"
        for s in $seasons; do 
            # build list of substacks with given season
            root=pmerge_s${s}
            list=$root.lst 
            ls -1 substack_paw?_s${s}?.fits > $list
            nfnd=$(cat $list | wc -l)                # number files found
            
            stout=UVISTA_${pass}-${FILTER}_s${s}_${resol}_${runID}
            
            if [ $nfnd -eq 1 ]; then
                ec "#  Found only 1 substack for season $s ... Nothing to merge "
                ec "# ==> just copy that substack to $stout"
                if [ $dry == 'F' ]; then 
                    r=$(cat $list)
                    cp $r ${stout}.fits
                    cp ${r%.fits}_weight.fits ${stout}_weight.fits
                fi
                rm $list
            else
                ec "# Merge $nfnd substacks for season $s into $stout ..."
                ec "# . Built $list with $nfnd entries"
                
                qfile=$root.sh; touch $qfile; chmod 755 $qfile
                sed -e 's|@NODE@|'$node'|'     -e 's|@IDENT@|'$PWD/$root'|'  -e 's|@DRY@|'$dry'|'  \
                    -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'$list'|'  -e 's|@WRK@|'$WRK'|'  \
                    -e 's|@STOUT@|'$stout'|'   -e 's|@TAG@|'s$s'|'     -e 's|@PASS@|'$pass'|' \
                    $bindir/pmerge.sh  > ./$qfile
                
                ec "# . Built $qfile and $list with $nfnd entries"
                ec "#-----------------------------------------------------------------------------"
                echo "qsub $qfile; sleep 1" >> pmerge.submit
            fi
        done
    fi

    #----------------------------------------------------------------------------------------#
    #          Merge substacks by paw into paw stacks
    #----------------------------------------------------------------------------------------#

    if [ $doPaws == "T" ]; then
        ec "## Build paw stacks from substacks:"
        ec "#-----------------------------------------------------------------------------"
        rm -rf pmerge_p?.lst
        for p in $(ls substack_paw?_s???.fits | cut -c13 | sort -u); do 
            # Build list of substacks with given paw
            root=pmerge_p${p}
            list=$root.lst 
            ls -1 substack_paw${p}_s???.fits > $list 
            nfnd=$(cat $list | wc -l)                # number files found
            #ec "# Built $list with $nfnd entries"

            stout=UVISTA_${pass}-${FILTER}_paw${p}_${resol}_${runID}
            
            if [ $nfnd -eq 1 ]; then
                ec "#  Found only 1 substack for paw $p ... Nothing to merge "
                ec "#  ==> just copy that substack to $stout"
                if [ $dry == 'F' ]; then 
                    r=$(cat $list)
                    cp $r ${stout}.fits
                    cp ${r%.fits}_weight.fits ${stout}_weight.fits
                fi
                rm $list
            else
                ec "#  Merge $nfnd substacks for paw $p into $stout ..."
                
                qfile=$root.sh; touch $qfile; chmod 755 $qfile
                sed -e 's|@NODE@|'$node'|'  -e 's|@IDENT@|'$PWD/$root'|' -e 's|@DRY@|'$dry'|'  \
                    -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'$list'|'    -e 's|@WRK@|'$WRK'|'  \
                    -e 's|@STOUT@|'$stout'|'   -e 's|@TAG@|'p$p'|'      -e 's|@PASS@|'$pass'|' \
                    $bindir/pmerge.sh  > ./$qfile
                
                ec "#  . Built $qfile and $list with $nfnd entries"
                ec "#-----------------------------------------------------------------------------"
                echo "qsub $qfile; sleep 1" >> pmerge.submit
            fi
        done
    fi

    #----------------------------------------------------------------------------------------#
    #          submit jobs
    #----------------------------------------------------------------------------------------#
 
    nq=$(cat pmerge.submit | wc -l)
    ec "#-----------------------------------------------------------------------------"
    ec "# written file 'pmerge.submit' with $nq entries "
    ec "#-----------------------------------------------------------------------------"
    if [ $dry == 'T' ]; then 
        echo "   >> EXITING DRY MODE << "
        for f in substa*.fits; do                
            if [ -h $f ]; then rm $f; fi    # delete links where they were built
        done
        exit 3
    fi

    ec "# submit qsub files ... "; source pmerge.submit
    ec " >>>>   wait for pmerge jobs to finish ...   <<<<<"
    
    btime=$(date "+%s.%N")
    sleep 60              # before starting wait loop
    while :; do           #  begin qsub wait loop for pmerge
        njobs=$(qstat -au moneti | grep merge_${FILTER} | wc -l)
        [ $njobs -eq 0 ] && break          # jobs finished
        sleep 60
    done  
    ec "# pmerge jobs finished - now check exit status"
    chmod 644 pmerge*.out
    nout=$(ls pmerge*.out 2> /dev/null | wc -l)
    if [ $nout -eq 0 ]; then
        ec "# PROBLEM: no pmerge .out files found ... nothing done?  quitting"
        exit 5
    fi

    nbad=$(grep EXIT pmerge_*.out | grep -v STATUS:\ 0 | wc -l)
    if [ $nbad -ne 0 ]; then
        ec "PROBLEM: pmerge exit status not 0 ... check pmerge.out"
        grep EXIT pmerge_*.out | grep -v STATUS:\ 0 
        askuser
    fi
    
    nbad=$(grep EXIT pmerge_*.out | grep -v STATUS:\ 0 | wc -l)
    if [ $nbad -ne 0 ]; then
        ec "PROBLEM: pmerge exit status not 0 ... check pmerge.out"
        grep EXIT pmerge_*.out | grep -v STATUS:\ 0 
        askuser
    fi
    
    ec "CHECK: pmerge.sh exit status ok ... continue"
    ec "# $stout and associated products built:"
    ls -lh UVISTA_${pass}-${FILTER}_*${resol}_${runID}*.fits
    ec "# ..... GOOD JOB! "
    for f in substa*.fits; do    ## remove links to substacks
        if [ -h $f ]; then rm $f; fi
    done
    mv pmerge_*.*  $prod_dir

    ec "#-----------------------------------------------------------------------------"
    ec "# Finished merging; moved scripts, lists and logs to $(echo $prod_dir | cut -d\/ -f6,7)"
    ec "#-----------------------------------------------------------------------------"
fi

ec "#-----------------------------------------------------------------------------#"
ec "#                   Non senza fatiga si giunge al fine                        #"
ec "#                                                                             #"
ec "#                    END OF THE DR6 STACKING PIPELINE                         #"
ec "#-----------------------------------------------------------------------------#"

exit 0
#------------------------------------------------------------------------------------

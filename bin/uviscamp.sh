#!/bin/bash 
#-----------------------------------------------------------------------------
# File: uviscamp.sh 
#-----------------------------------------------------------------------------
# Purpose:   Pipeline to run scamp and swarp; short version of uvis.sh
# Requires: 
# - work directory with data, given by $WRK env. var.
# - python3, python scripts from terapix pipe adapted to python 3,
#            in ~/softs/uvis-pipe/python etc.
# - wrapper scripts in ~/softs/uvis-pipe/bin
# Author:    A. Moneti
#-----------------------------------------------------------------------------
# Versions:
# v2.00: initial version, from DR5 processing                      (13.jan.22)
#-----------------------------------------------------------------------------
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
    echo "    uviscamp.sh option (dry or auto)"
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
DR5=$(echo $WRK  | cut -d/ -f1-4)      # Base directory

REL="DR5-"$FILTER                      # used in names of some products

badfiles=$WRK/DiscardedFiles.list      # built and appended to during processing
fileinfo=$WRK/FileInfo.dat             # lists assoc files (bpm, sky, flat) and other info for each image

pipelog=${WRK}/uviscamp.log ; if [ ! -e $pipelog ]; then touch $pipelog; fi
Trash=zRejected         ; if [ ! -d $Trash ]; then mkdir $Trash; fi


#-----------------------------------------------------------------------------
# Other options
#-----------------------------------------------------------------------------

do_hires=F    # to do or not hi-res stack
imdir=${WRK}/images
	
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

#imdir=${WRK}/images

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

erract() { # what to do in case of error
    echo ""
    ec "!!! PROBLEM "; tail $logfile
    exit 5
}

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
ec "   #       This is uviscamp.sh                       #"
ec "   #                                                 #"
ec "   #=================================================#"

cd $WRK

#-----------------------------------------------------------------------------------------------
# First a dummy step to start sequence .... do nothing:
#-----------------------------------------------------------------------------------------------

if [ $# -ge 1 ] && [ $1 = 'xxx' ]; then
    echo " DUMMY step .... nothing to do"

#-----------------------------------------------------------------------------------------------
#    echo "### QUIT HERE FOR NOW ###" ; exit 0       
#-----------------------------------------------------------------------------------------------
elif [ $1 = 'p2' ]; then      # P2: scamp, swarp, build stack and its mask, build obj masks
#-----------------------------------------------------------------------------------------------

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
        cd origs ; ls -1 v20*_0????.fits > ../list_images; cd -
    fi

    if [ ! -s list_ldacs  ]; then 
        ec "# WARNING: list_ldacs not found ... build it"
        cd ldacs ; ls -1 v20*_0????.ldac > ../list_ldacs; cd -
    fi

    if [ ! -s list_weights  ]; then 
        ec "# WARNING: list_weights not found ... build it"
        cd weights ; ls -1 v20*_0????_weight.fits > ../list_weights; cd -
    fi

    if [ ! -s list_heads  ]; then 
        ec "# WARNING: list_heads not found ... build it"
        cd heads ; ls -1 v20*_0????.ahead > ../list_heads; cd -
    fi

    nldacs=$(cat list_ldacs   | wc -l)
    nimages=$(cat list_images | wc -l)
    nwghts=$(cat list_weights | wc -l)
    nheads=$(cat list_heads | wc -l)
    
    if [ $nimages -eq $nldacs ]; then 
        ec "CHECK: found $nimages images, $nwghts weights, $nldacs ldac files ... " 
        ec "CHECK: ... seems ok to continue with first pass."
    else
        ec "!!! PROBLEM: Number of images, ldacs, weights not the same ..."
        echo "  $nimages,  $nldacs  $nwghts"
        askuser
    fi  
    
    ec "##----------------------------------------------------------------------------"
    ec "#"
    ec "##          ======  BEGIN SPECIAL PASS  ======"
    ec "#"
    ec "##----------------------------------------------------------------------------"

    # Some product names
#    stout=UVISTA_${FILTER}_p1              # name of pass1 stack w/o .fits extension (low res)
#    stout_flag=${stout%.fits}_obFlag.fits  # and the object flag

    #----------------------------------------------------------------------------------------#
    #       scamp
    #----------------------------------------------------------------------------------------#
    # check whether scamp has already been run ... 

    if [ -d scamp_vOct21 ] ; then
        ec "CHECK: scamp logfile already exists and found $nheads head files ..." 
        ec "CHECK:  ==> scamp already done skip to swarp "
        ec "#-----------------------------------------------------------------------------"
    else
        rcurr=R7
        nl=$(cat list_ldacs | wc -l)  # total num of files to process
        if [ $nl -lt 100 ]; then # set walltime - in hrs!!
            wtime=4    # useful in testing
        else 
            wtime=12
        fi

		# split randomly ... cross-matching has little importance; what counts is matching
		# against the Gaia catalogue ...
		#
		# build list of all ldacs first then split it:
		rm -f pscamp_??.lst             # delete residuals ones if any
        nl=$(cat list_ldacs | wc -l)    # total num of files to process
#		nf=1000                         # number of frames per group
		nf=10                         # number of frames per group
		ncats=$(($nl/$nf))              # approx number of catals per group
		split -n l/$ncats list_ldacs --additional-suffix='.lst' pscamp_

		#### Build PSFsel.dat  ####
		#for f in $(cat list_images); do grep ${f%.fits}  /n08data/UltraVista/DR5/N/images/PSFsel.dat ; done > PSFsel.dat 

		# Now find files with median FWHM and set them as photref= T
		nfiles=$(grep -v Name PSFsel.dat | wc -l)
		index=$(($nfiles / 2))
		median=$(grep -v Name PSFsel.dat | sort -nk2,3 | head -$index | tail -1 | tr -s \  | cut -d\  -f2)
		photlist=$(grep \ $median\  PSFsel.dat | cut -d\  -f1)  # list of files with median fwhm
		for f in $photlist; do ln -sf $confdir/photom_ref.ahead ${f}.ahead; done

		ec "## - R7: pscamp.sh: scamp, pass-1, split sequentially into $ncats groups "
        ec "#-----------------------------------------------------------------------------"

		IMDIR=$WRK/images
        rm -rf $IMDIR/pscamp.submit  
        for plist in pscamp_??.lst; do
            ptag=$( echo $plist | cut -c8-9)  # tag to build output file names

            rm -f $IMDIR/pscamp_$ptag.out $IMDIR/pscamp_$ptag.log $IMDIR/pscamp_$ptag.sh   
            nn=$(cat $plist | wc -l)

            qfile=$WRK/images/pscamp_$ptag.sh; touch $qfile; chmod 755 $qfile
            sed -e 's|@NODE@|'$node'|'     -e 's|@IDENT@|'$PWD/pscamp_$ptag'|'  -e 's|@DRY@|'$dry'|'  \
                -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'$plist'|'    -e 's|@WRK@|'${WRK}'/images|'  \
                -e 's|@WTIME@|'$wtime'|'   -e 's|@PTAG@|'$ptag'|'  $bindir/pscamp.sh > $qfile
        
            if [ $nn -lt 100 ]; then    # short (test) run - decrease resources
                sed -i -e 's|ppn=22|ppn=8|' -e 's|time=48|time=06|' $qfile
            fi

            ec "# Built $qfile for $plist with $nn entries"
            echo  "qsub $qfile ; sleep 1" >> $IMDIR/pscamp.submit
        done
		njobs=$(cat ./pscamp.submit | wc -l)

        ec "# ==> Built pscamp.submit with $(cat $IMDIR/pscamp.submit | wc -l) entries"
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

        ec "# - Build / replace links to needed config files:"
        ln -sf $confdir/scamp_dr5.conf .
		ln -sf $confdir/vista_gaia.ahead .
        ln -sf $confdir/GAIA-EDR3_1000+0211_r61.cat . 

        #-----------------------------------------------------------------------------
        # submit jobs and wait for them to finish
        #-----------------------------------------------------------------------------

        ec "# - Submitting $ncats pscamp_?? jobs ..."

        source $IMDIR/pscamp.submit
        ec " >>>>   wait for pscamp to finish ...   <<<<<"
        
        nsec=30  # wait loop check interval
        btime=$(date "+%s"); sleep 20   # before starting wait loop
        while :; do              #  begin qsub wait loop for pscamp
            ndone=$(ls $IMDIR/pscamp_??.out 2> /dev/null | wc -l)
            [ $ndone -eq $njobs ] && break               # jobs finished
            sleep $nsec
        done  
        chmod 644 pscamp_??.out

        ec "# $njobs pscamp_p?? jobs finished, walltime $(wtime) - now check exit status"
        ngood=$(grep STATUS:\ 0 pscamp_??.out | wc -l)
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

        # check warnings 
        nwarn=$(cat $WRK/pscamp_??.warn 2> /dev/null | wc -l)
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
            nun=$(sort -u -k2 $fsfile | wc -l)    # number of values
            if [ $nun -le $((2*$nfs/3)) ]; then
                ec "#### ATTN: $fsfile has $nun values of about $(($nimages * 16)) expected"
            fi
            
            nbad=$(\grep 0.0000000 $fsfile |  wc -l)
            if [ $nbad != 0 ]; then echo "#### ATTN: found $nbad chips with FLUXSCALE = 0.00"; fi
            nbad=$(\grep INF $fsfile |  wc -l)
            if [ $nbad != 0 ]; then echo "#### ATTN: found $nbad chips with FLUXSCALE = INF"; fi
            res=$(grep -v -e INF -e 0.00000000 $fsfile | tr -s ' ' | cut -d' ' -f2 | awk -f $uvis/scripts/std.awk )
            ec "# mean flux scale: $res"
        fi

        #-----------------------------------------------------------------------------
		# Combine pscamp_??.dat files into single global one:
        #-----------------------------------------------------------------------------
		head -1 pscamp_aa.dat > pscamp.dat
		grep -v File pscamp_??.dat | cut -d\: -f2 >> pscamp.dat
		grep v20 pscamp.dat | awk '{if ($2 < 4) print $1}'       > low_contrast.dat
		grep v20 pscamp.dat | awk '{if ($8 > 15) print $1}'      > large_shifts.dat
		grep v20 pscamp.dat | awk '{if ($3*$3 > 0.25) print $1}' > large_ZPcorr.dat
		echo ">> Found $(cat low_contrast.dat | wc -l) files with scamp contrast < 4"
		echo ">> Found $(cat large_shifts.dat | wc -l) files with total shift > 15 arcsec"
		echo ">> Found $(cat large_ZPcorr.dat | wc -l) files ZP-corr > 0.50 mag"
		cat low_contrast.dat large_shifts.dat large_ZPcorr.dat | sort -u > scamp_issue.dat
		echo ">>  ... for a total of $(cat scamp_issue.dat| wc -l) files with possible problem"
		echo ">> ... see scamp_issue.dat"

        ec "#-----------------------------------------------------------------------------"
        ec "CHECK: pscamp.sh successful, $nheads head files built ... clean-up and continue"
        if [ ! -d heads ] ; then mkdir heads ; else rm -f heads/* ; fi
        mv v20*.head heads   #####; ln -s scamp/v*.head .
        if [ ! -d scamp ] ; then mkdir scamp ; else rm -f scamp/* ; fi
        mv [fipr]*_??_$FILTER.png pscamp_??.* fluxscale.dat  scamp
		mv pscamp.dat low_contrast.dat large_shifts.dat large_ZPcorr.dat scamp_issue.dat scamp
        #rm GAIA*.cat scamp_dr5.conf vista_gaia.ahead
        rm -f v20*.ldac    # normally not there

        ec "#-----------------------------------------------------------------------------"
        if [ $int == "T" ]; then ec "# >>> Interactive mode:" ; askuser; fi
    fi


#	ec "#-----------------------------------------------------------------------------"
	ec "# Image selection:  "
	ec "#-----------------------------------------------------------------------------"

    #----------------------------------------------------------------------------------------#
    #       Image selection:
    #----------------------------------------------------------------------------------------#
	# rebuild list_images from list_cleaned, then remove:
	# - files rejected (grade C) in DR1
	# - files with large / elongated PSF
	# - files with large shifts from scamp
	# - files with large ZP-corr from scamp
	# - files with low contrast from scamp
    #----------------------------------------------------------------------------------------#

    # build list_cleaned in cleaned/
	cd cleaned ; ls v20* | sed 's/_clean//' > ../list_cleaned ; cd ..
	cp list_cleaned list_images ; nnIni=$(cat list_images | wc -l)

	# PSF-based image selection
	for f in $(grep v20 badPSF.dat | cut -d\  -f1); do grep -v $f list_images > x ; mv x list_images; done
    # remove Grade C file in DR1
	if [ ! -e DR1_gradeC.txt ]; then 
		grep \,C\, $confdir/DR1_file_info.txt | grep $FILTER | sed 's/, /; /g' | cut -d\, -f1,6,8,12-20 | tr \, \  > DR1_gradeC.txt
	fi
	for f in $(cut -d\  -f1 DR1_gradeC.txt); do grep -v $f list_images > x ; mv x list_images; done    
    # remove files with large shifts
	for f in $(cut -d\  -f1 scamp/large_shifts.dat); do grep -v $f list_images > x ; mv x list_images; done     
    # remove files with large ZP-corr
	for f in $(cut -d\  -f1 scamp/large_ZPcorr.dat); do grep -v $f list_images > x ; mv x list_images; done     
    # remove files with low contrast
	for f in $(cut -d\  -f1 scamp/low_contrast.dat); do grep -v $f list_images > x ; mv x list_images; done     
	
	nimages=$(cat list_images | wc -l)
	nnRem=$(($nnIni - $nimages))
	ec ">> Image selection: removed $nnRem files from list_images; $nimages files left to swarp"
	
#===================================================================================================#   

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
	# check that number of images in list_paw? == num in list_images
	npawims=$(cat list_paw? 2> /dev/null | wc -l)

    if [ $npawims == $nimages ]; then
        nn=$(wc list_paw? 2> /dev/null | grep total | tr -s ' ' | cut -d' ' -f2 )
        ec "# Found $npaws paws with a total of $npawims images"
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
    stout=TEST_${REL}_p2_lr   # output name of low-res pass 2 stack w/o .fits extension
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
			ec "CHECK: Found $stout.fits - Nothing to do!!! "
		fi
        ec "CHECK: ==> skip to next step "
        ec "#-----------------------------------------------------------------------------"
    else 
        rcurr=R16
        ec "## - R16:  pswarp.sh: swarp pass2 for $npaws paws, $nn images ... by sub-paws "
        ec "#-----------------------------------------------------------------------------"
        nout=$(ls -1 pswarp2_paw?_??.out 2> /dev/null | wc -l)
        nlog=$(ls -1 pswarp2_paw?_??.log 2> /dev/null | wc -l)
        if [ $nout -ge 1 ] || [ $nlog -ge 1 ]; then
            ec "#### ATTN: found $nout pswarp2_paw?_??.out and $nlog pswarp2_paw?_??.log files ... delete them and continue??"
            askuser
        fi
		
		rm -f qall estats pswarp2_paw?_??.lst  pswarp2_paw?_??.sh     # just in case
		rm -f pswarp2_paw?_??.out  pswarp2_paw?_??.log  

        # if no p2 stack, then do one at low res, 
        if [[ $stout =~ "lr" ]]; then
            ec "# First build low-res (0.6 arcsec/pix) p2 stack ..."
            headfile=cosmos_lr.head   # = std1G.head;  improved firstpass.head
        else
            ec "# Found low-res p2 stack ... build stack high-res (0.3 arcsec/pix) one"
            headfile=cosmos_hr.head   # = std4G.trim.head;  improved cosmos.head
        fi

        subsky=N                             # for pass2 DO NOT subtract sky

        ec "#-----------------------------------------------------------------------------"
        ec "# output:    $stout"
        ec "# head-file: $headfile"
        ec "# subsky:    $subsky"
        ec "#-----------------------------------------------------------------------------"

        nim=450  # approx num of images in each sublist; require 11GB of mem each
#        nim=900  # approx num of images in each sublist; require 11GB of mem each
#        nim=2900  # approx num of images in each sublist; require 11GB of mem each
        for list in list_paw[0-9]; do  
            nl=$(cat $list | wc -l)
            ppaw=$(echo $list | cut -d\_ -f2)       # NEW tmporary name for full paw
            split -n l/$(($nl/$nim+1)) $list --additional-suffix='.lst' pswarp2_${ppaw}_
            for slist in pswarp2_${ppaw}_??.lst; do
                nl=$(cat $slist | wc -l)    
                paw=$(echo $slist | cut -d\_ -f2-3 | cut -d\. -f1)   
                outname=substack_${paw}
				if [ $nl -lt 250 ]; then ppn=10; else ppn=20; fi
                #ec "DEBUG:  For paw $paw, $nl images ==> $outname with subsky $subsky"
            
                # ---------------------- Local run by sublist ----------------------
                
                qfile="pswarp2_$paw.sh"; touch $qfile; chmod 755 $qfile
                sed -e 's|@NODE@|'$node'|'  -e 's|@IDENT@|'$PWD/pswarp2'|'  -e 's|@DRY@|'$dry'|'  \
                    -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'$slist'|'  -e 's|@WRK@|'$WRK'|' \
                    -e 's|@PAW@|'$paw'|'  -e 's|@PASS@|'$pass'|'  -e 's|@HEADFILE@|'$headfile'|'  \
                    -e 's/@SUBSKY@/'$subsky'/'  -e 's|@PPN@|'$ppn'|'  $bindir/pswarp.sh > $qfile
            
                ec "# Built $qfile with $nl images for paw $paw ==> $outname"
                echo "qsub $qfile; sleep 1" >> qall
            done
        done 
        ec "#-----------------------------------------------------------------------------"
        njobs=$(cat qall | wc -l)
        ec "# ==> written to file 'qall' with $njobs entries "
		ec "# ==> torque params: $(grep ppn= $qfile | cut -d\  -f3)"
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then echo "   >> EXITING TEST MODE << "; exit 3; fi

        ec "# Submit qsub files ... ";  source qall
        ec " >>>>   Wait $njobs pswarp jobs ... first check in 1 min  <<<<<"

        ostr="ddd"              # a dummy string for comparisons within the loop
        btime=$(date "+%s.%N");  sleep 60           # begin time
        while :; do           #  begin qsub wait loop for pswarp
            ndone=$(ls $WRK/images/pswarp2_paw?_??.out 2> /dev/null | wc -l)
            [ $njobs -eq $ndone ] && break          # jobs finished

            # check substack completion progress
            str=$(ls -lthr substack_paw?_??.fits 2> /dev/null | tr -s ' ' | cut -d' ' -f4-9 | tail -1)
            if [[ $str != $ostr ]]; then 
                nrun=$(ls -d /scratch??/psw*_$FILTER  /n??data/psw*_$FILTER 2> /dev/null | wc -l )
                echo "$(date "+[%d.%h %T"]) $nrun jobs running; last substack:  $str " 
                ostr=$str
            fi

            # check for errors in logfiles
            grep -B4 Error pswarp2_paw?_??.log 2> /dev/null | strings > pswarp.errs
            nerr=$(grep Error pswarp.errs | wc -l) #  ; echo $nerr
            if [ $nerr -ge 1 ]; then
                ec "#####  ERRORS found in pswarp logfiles  #####" 
                cat pswarp.errs
            else
                rm pswarp.errs
            fi
            sleep 60
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
            ec "#### ATTN: found warnings in pswarp logfiles"
            askuser
        fi
		
		# build general logfile
		grep \ File\  pswarp2_paw*.log > pswarp2_${resol}.log
        chmod 644 subst*.* pswarp2_paw*.log pswarp2*.out

        mv -f pswarp2*.sh pswarp2*.warn pswarp2_paw?_??.???  $prod_dir
		cp $confdir/swarp238.conf $prod_dir
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

        ec "## - R17:  pmerge.sh: Merge $nsubstacks substacks into $stout ..."
        ec "#-----------------------------------------------------------------------------"

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

		# build general logfile
		grep Merge pmerge.out > pmerge_${resol}.log
        
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

	if [[ $do_hires != 'T' ]]; then
		ec "#### ATTN: skipping hi-res stacks"
	fi

    ec "# Low-res stack done, BRAVO! ####"
		
    if [[ $resol == 'lr' ]] && [[ $do_hires == 'T' ]]; then
        mycd $WRK
        ec "# now begin high-res one ####"
        $0 p4 auto
	else
		ec "#-----------------------------------------------------------------------------#"
		ec "#                   Non senza fatiga si giunge al fine                        #"
		ec "#                                                                             #"
		ec "#                        END OF THE DR4 PIPELINE                              #"
		ec "#-----------------------------------------------------------------------------#"
		exit 0
    fi

#-----------------------------------------------------------------------------------------------
else
#-----------------------------------------------------------------------------------------------
   echo "!! ERROR: $1 invalid argument ... valid arguments are:"
   help
fi 

exit 0
#@@ ------------------------------------------------------------------------------------

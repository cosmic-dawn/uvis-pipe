#!/bin/bash 
#-----------------------------------------------------------------------------
# DR-4 processing of UltraVista data -  scamp/swarp/merge (ssm)
#
# This script can be used to run scamp, swarp, and merge, - usually intended
# for pass2. It was originally a shortened version of the pipeline, but it has
# then undergone various modifications: various fixes and other improvements
# (including to the pscamp, pswarp, and pmerge scripts that it writes), and
# some more significant changes like to build and merge stacks by season.
#
# AMo - 25.Oct.18
#-----------------------------------------------------------------------------
set -u  # exit if a variable is not if 

if [ $# -eq 0 ]; then
	echo "  SYNTAX: ./pssm.sh scamp | swarp | merge {test}"
	echo "  ==> quitting"
	exit 8
fi

# continue

module() { eval $(/usr/bin/modulecmd bash $*); }
module purge ; module load intelpython/2

#-----------------------------------------------------------------------------
# Some variables
#-----------------------------------------------------------------------------
dry='F'
if [[ "${@: -1}" == 'dry'  ]]; then dry='T'; fi
if [[ "${@: -1}" == 'test' ]]; then dry='T'; fi
if [[ "${@: -1}" == 'auto' ]]; then auto='T'; else auto='F'; fi  # auto mode not implemented

#-----------------------------------------------------------------------------

uvis=/home/moneti/softs/uvis-pipe

bindir=$uvis/bin
pydir=$uvis/python
confdir=$uvis/config

# NB: -h file: true if file is a symbolic link
if [ -h $0 ]; then pn=$(readlink -f $0); else pn=$(ls $0); fi    # get the command line
sdate=$(date "+%h%d@%H%M") 

#-----------------------------------------------------------------------------
# Functions
#-----------------------------------------------------------------------------

wtime() {   # wall time: convert sec to H:M:S
    echo $(date "+%s.%N") $btime | awk '{print strftime("%H:%M:%S", $1-$2, 1)}'; 
}
ec() {    # echo with date
    if [ $dry == 'T' ]; then echo "[DRY MODE] $1";
    else echo "$(date "+[%d.%h %T]") $1 " | tee -a $pipelog 
    fi
} 
ecn() {     # idem for -n
    if [ $dry == 'T' ]; then echo -n "[DRY MODE] $1"
    else echo -n "$(date "+[%d.%h %T]") $1 " | tee -a $pipelog
    fi 
}

mycd() { 
    if [ -d $1 ]; then \cd $1; ec " --> $PWD"; 
    else echo "!! ERROR: $1 does not exit ... quitting"; exit 5; fi
}
imdir=${WRK}/images

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
#testmsg() {   # message for test/dry runs
#    ec "#############################################################################"
#    ec "#                                                                           #"
#    ec "##  Begin pssm $pn in dry mode  ##"
#    ec "#                                                                           #"
#    ec "#############################################################################"
#}

mkplists() {  # build paw lists
	if [ $(pwd) != $WRK/images ]; then mycd $WRK/images; fi
	list=$1
#    ec " >> Build paw lists from $list ..."
    
    file=$(mktemp)
    paws=" paw1 paw2 paw3 paw4 paw5 paw6 COSMOS"
    for i in $(cat $list); do grep $i ../FileInfo.dat >> $file; done
    for p in $paws; do grep $p $file | cut -d \   -f1 > list_${p}; done
    rm $file

    # if present, convert to paw0
    if [ -e list_COSMOS ]; then mv list_COSMOS list_paw0; fi

    for f in list_paw?; do if [ ! -s $f ]; then rm $f; fi; done
#    wc -l $list ; wc -l list_paw?   ## DEBUG
}

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
        N | NB118 ) FILTER=N ;;
        Y       ) FILTER=$FILTER ;;
        J       ) FILTER=$FILTER ;;
        H       ) FILTER=$FILTER ;;
        K | Ks  ) FILTER=K      ;;
        P       ) FILTER=$FILTER ;; # a bogus N filter for test purposes
        Q       ) FILTER=$FILTER ;; # a bogus Y filter for test purposes
        R       ) FILTER=$FILTER ;; # a bogus J filter for test purposes
        S       ) FILTER=$FILTER ;; # a bogus H filter for test purposes
        T       ) FILTER=$FILTER ;; # a bogus K filter for test purposes
        *          ) ec "# ERROR: invalid filter $FILTER"; exit 3 ;;
    esac
fi

# current node
node=$(pwd | cut -c 2-4)

badfiles=$WRK/DiscardedFiles.list      # built and appended to during processing
fileinfo=$WRK/FileInfo.dat             # lists assoc files (bpm, sky, flat) and other info for each image
Trash=Rejected                         # in images
pass=2                                 # all this is only for pass 2
runID="RC1"                             # for different version of the products; appended to $outdir
pipelog="${WRK}/pssm_$runID.log" 

if [ ! -e $pipelog ]; then touch $pipelog; fi

mycd $WRK/images

#-----------------------------------------------------------------------------------------------
if [ $1 == 'scamp' ]; then    # scamp
#-----------------------------------------------------------------------------------------------

    mycd $WRK/images
    ncurr=2 ; pcurr="p$ncurr" ; pprev=$(($ncurr-1)) ; pnext="p$(($ncurr+1))"
    
	rm -rf v20*_00???.head v20*_00???.ahead  v20*_00???.ldac [a,d,f,p]*.png

	# get ldacs for valid images only
	for f in $(cat list_images); do r=${f%.fits}; ln -s ldacs/${r}_noSAT.ldac ${r}.ldac; done
	for d in swarp_lr/pswarp_paw2_s??_a?.lst; do 
		for f in $(tail -5 $d); do
			ln -sf ldacs/${f%.fits}_noSAT.ldac ${f%.fits}.ldac 
		done
	done 

	# now get to work ....
    rename _noSAT.l .l v*noSAT*

	ls -1 v20*.ldac > list_ldacs
	nldacs=$(cat list_ldacs   | wc -l)

	photref=T     # use phot. ref. fields?
	bySeason=F    # split by season - tested, but not used for now

    ec "##----------------------------------------------------------------------------"
    ec "#"
    ec "##          ======  BEGIN RUN OF SCAMP  ======"
    ec "#"
    ec "##----------------------------------------------------------------------------"
    
    if [ -e pscamp.out ] ; then
        ec "CHECK: scamp logfile already exists ... cleanup and restart" 
        ec "#-----------------------------------------------------------------------------"
		exit 3
    else
		echo "  #####   scamp option not working - use with swarp - quitting  #####"
		echo "  ###################################################################"

        if [ -s list_some ]; then cp list_some pscamp.lst; else cp list_ldacs pscamp.lst; fi
		nl=$(cat pscamp.lst | wc -l)

        ec "## - R7:  pscamp.sh: run scamp on pscamp.lst with $nl entries ... "
        ec "#-----------------------------------------------------------------------------"
        rm -f  tlist photlist? pscamp.out pscamp*.log pscamp.sh   # do not delete pscamp.lst, just build above

		if [ $bySeason == 'T' ]; then
			ec "# build SEASON keyword for instrument selection into .ahead files"
			for f in v*.ldac; do 
				yr=$(echo $f | cut -c2-5); mo=$(echo $f | cut -c6-7)
				for n in $(seq 2009 2017); do
					if [ $yr -eq $n ]; then
						if [ $mo -gt 8 ]; then 
							echo "SEASON  = 'S$(($yr - 2008))      '" > ${f%.ldac}.ahead
						else
							echo "SEASON  = 'S$(($yr - 2009))      '" > ${f%.ldac}.ahead
						fi
					fi
				done
				echo "END" >> ${f%.ldac}.ahead 
			done
		else
			ec "# Do not use SEASON kwd for instrument selection"
		fi

		if [ $photref == "T" ]; then 
			plist=photlist${FILTER}
			if [ ! -e $plist ]; then    # build photlist with files that have FWHM near median
				nfiles=$(grep -v Name PSFsel.dat | wc -l)
				index=$(($nfiles / 2)) #; echo $index
				median=$(grep -v Name PSFsel.dat | sort -nk2,3 | head -$index | tail -1 | tr -s \  | cut -d\  -f2)
#				echo $median  #DEBUG

				grep \ $median\  PSFsel.dat | cut -d\  -f1 > medlist  # list of files with median fwhm
				for f in $(cat pscamp.lst); do grep ${f%.ldac} medlist; done > $plist
				ec "## Found $(cat $plist | wc -l) files with median value of FWHM ($median)"
				for f in $(cat $plist); do ln -sf $confdir/photom_ref.ahead ${f}.ahead; done
#				rm medlist; exit
			else
				ec "## found $plist with $(cat $plist | wc -l) entries"
			fi

			ec "# Write PHOTFLAG kwd to ahead file for photref images"
		    # .... and make them photom refs.
			for f in $(cat $plist); do
				if [ -e $f ]; then 
					ah=${f%.ldac}.ahead
					if [ -e $ah ]; then 
						sed -i "1s/^/PHOTFLAG= T\n/" $ah
					else
						echo "PHOTFLAG= T" > $ah
						echo "END" >> $ah
					fi
				fi
			done
			ec "# ==> Found $(ls v20*.ahead | wc -l) photref files"
		else
			ec "# Do not use PHOTFLAG kwd to specify photometric ref. images;"
			ec "# ==> rm v20*.ahead files to make sure they won't be used"
			rm -fr v20*_00???.ahead   # normally already deleted at star of this step
		fi

		#for v6
		export PATH="/home/moneti/bin:/softs/cfitsio/3.430/bin:/opt/intel/intelpython2/bin:/usr/torque/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/usr/local/bin:/home/moneti/.local/bin:"
		#for v7
		#export PATH="/softs/cfitsio/3.430/bin:/opt/intel/intelpython2/bin:/usr/torque/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/usr/local/bin:/home/moneti/.local/bin:"

		# set scampdir for products
        ec "# using scamp  ==> $(scamp -v)"
		ver=$(scamp -v | cut -d\  -f3 | tr -d \.)
		scampdir=scamp_v${ver}_n${nl}  #_$sdate
 		scampdir=scamp_noAirmass       # no airmass kwd
		ec "# outputs to $scampdir"
        ec "#-----------------------------------------------------------------------------"

        qfile="pscamp_xxx.sh"; touch $qfile; chmod 755 $qfile
        sed -e 's|@NODE@|'$node'|'     -e 's|@IDENT@|'$PWD/pscamp'|'  -e 's|@DRY@|'$dry'|'  \
            -e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'pscamp.lst'|'    -e 's|@WRK@|'$WRK'|'  \
            -e 's|@WTIME@|'99'|' -e 's|@PATH@|'$PATH'|' -e 's|@TAG@|xxx|' $bindir/pscamp.sh > ./$qfile
        
        if [ $nl -lt 100 ]; then    # short (test) run - decrease resources
            sed -i -e 's|ppn=22|ppn=8|' -e 's|time=48|time=06|' $qfile
        fi

        ec "# Built $qfile and pscamp.lst with $nl entries"
        ec "#-----------------------------------------------------------------------------"
        if [ $dry == 'T' ]; then 
			ec ""
			ec "   >> BEGIN dry-run of pscamp.sh:  << "
			ec ""
			$PWD/pscamp.sh list_ldacs dry
			ec "   >> Dry-run of $0 finished .... << "
		#	rm v20*.ldac
			exit 0
		fi
        
        ec "# submitting $qfile ... "; qsub $qfile      
        ec " >>>>   wait for pscamp to finish ...   <<<<<"
        
        btime=$(date "+%s"); sleep 10   # before starting wait loop
        while :; do           #  begin qsub wait loop for pscamp
            njobs=$(qstat -au moneti | grep pscamp_${FILTER} | wc -l)
            [ $njobs -eq 0 ] && break          # jobs finished
            sleep 60
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
        nwarn=$(cat pscamp.warn | wc -l)
        if [ $nwarn -ge 1 ]; then 
            ec "# WARNING: $nwarn warnings found in logfile for $nl files"; head pscamp.warn
		else
			ec "# ... NO warnings found"
        fi   
		
        # check constrast
        ec "#-----------------------------------------------------------------------------"
        ec "#       Scamp constrast results"
        ec "#-----------------------------------------------------------------------------"
		ecn "# Mean X-Y contrast: "
		awk '{print $10}' pscamp_tb3.log | awk -f $confdir/awk/std.awk 
		nlow=$(cat pscamp_low.log 2> /dev/null | wc -l) ; nl=$(cat pscamp_tb3.log | wc -l)
		ec "# Found $nlow of $nl files with low contrast ($(echo "100 * $nlow / $nl" | bc)%). "


        # check fluxscales
        ec "#-----------------------------------------------------------------------------"
        ec "#       Scamp flux-scale results"
        ec "#-----------------------------------------------------------------------------"
    	fsfile=fluxscale.dat    
    	if [ ! -e $fsfile ]; then 
    	    ec ">> Rebuild $fsfile"
    	    grep FLXSCALE v20*.head | cut -d\/ -f1 | sed 's/.head:FLXSCALE=//' > $fsfile
    	fi
 
    	nfs=$(cat $fsfile | wc -l)   
    	nun=$(sort -u -k2 $fsfile | wc -l)    # number of unique values
    	if [ $nun -le $((2*$nfs/3)) ]; then
    	    ec "# ATTN $fsfile has $nun unique values of about $(($nl * 16)) expected"
    	fi

    	nbad=$(\grep 0.0000000 $fsfile |  wc -l)
    	if [ $nbad != 0 ]; then 
			echo "# ATTN: found $nbad chips with FLUXSCALE = 0.00"
		else 
			ec "# found NO chips with FLUXSCALE = 0.00"
		fi

    	nbad=$(\grep INF $fsfile |  wc -l)
    	if [ $nbad != 0 ]; then 
			echo "# ATTN: found $nbad chips with FLUXSCALE = INF"
		else 
			ec "# found NO chips with FLUXSCALE = INF"
		fi

    	res=$(grep -v -e INF -e 0.00000000 $fsfile | tr -s ' ' | cut -d' ' -f2 | awk -f $confdir/awk/std.awk )
		nfs=$(cat $fsfile | wc -l)
		ec "# found $nfs valid fluxscale measurements; " #$(echo "scale=2; $nfs / $nl" | bc) per ldac catal"
    	ec "# mean flux scale: $res"
        ec "#-----------------------------------------------------------------------------"

		# all OK ... continue; cleanup and move products to $scampdir
        ec "CHECK: pscamp.sh successful, $nheads head file built." 
		ec "# move products to $scampdir."
		if [ -e $scampdir ]; then 
			echo "# ATTN: $scampdir already exists - erase or rename it to keep it"
			echo "#       or leave it to overwrite its contents"
			askuser
		fi
		mkdir $scampdir
        mv v20*.head $scampdir    #; ln -sf $scampdir/v*.head .
		mv *.png pscamp* fluxscale.dat v20*.ahead   $scampdir
        # don't need these anymore
        if [[ $FILTER == "T" ]] || [[ $FILTER == "R" ]]; then rm -rf list_ldacs  v20*.ldac v20*.ahead; fi

        ec "#-----------------------------------------------------------------------------"
		exit 0
    fi

#-----------------------------------------------------------------------------------------------
elif [[ $1 == 'swarp' ]] || [[ $auto == 'T' ]] ; then      # swarp 
#-----------------------------------------------------------------------------------------------

    ec "##----------------------------------------------------------------------------"
    ec "#"
    ec "##          ======  BEGIN SWARPING  ======"
    ec "#"
    ec "##----------------------------------------------------------------------------"


    #-----------------------------------------------------------------------------------------------
    # preps for swarp and merge
    #-----------------------------------------------------------------------------------------------

#	outdir="swarp_lr"_$runID     # default directory for the products
	outdir="swarp_48k"_$runID    # to build 48k stacks
	headfile=cosmos48k.head    # standard 48k head file
	ec "##  Build high-res substacks with $headfile; products into $outdir  ..."

##    # if products already there, then swarp with high res (cosmos) headfile
##	if [ -d $outdir ]; then
##		nss=$(ls -1 $outdir/substack_paw?_s??_??.fits 2> /dev/null | wc -l)
##	else
##		nss=0
##	fi
##	
##	if [ $nss -eq 0 ]; then    # products not yet built
##		resol=lr
##		headfile=std1G.head        # Bo's external head file for low-res
##		ec "##  Build low-res substacks using $headfile; products into $outdir ..."
##	else
##		ec "# ATTN: Found $nss low-res substacks in $outdir"
##		resol=hr
##		outdir="swarp_hr"_$runID
##		headfile=cosmos.head    # standard 48k head file
##		ec "##  Build high-res substacks with $headfile; products into $outdir  ..."
##	fi
#	ec "#-----------------------------------------------------------------------------"

    #----------------------------------------------------------------------------------------#
    #       swarp
    #----------------------------------------------------------------------------------------#

    # check if swarp alreay done:
	prod_dir=swarp_seasons_lr   #

	nsubstks=$(ls -1 $prod_dir/substack_paw?_s??_??.fits 2> /dev/null | wc -l)   
	nscripts=$(ls -1 $prod_dir/pswarp*paw?_s??_??.sh     2> /dev/null | wc -l)
	nswlists=$(ls -1 $prod_dir/pswarp*paw?_s??_??.lst    2> /dev/null | wc -l)

    if [ $nsubstks -eq $nscripts ] && [ $nscripts -eq $nswlists ] && [ $nsubstks -ne 0 ]; then 
        ec "CHECK: Found $nsubstks substacks, $nscripts scripts, and $nswlists lists ...."
        ec "CHECK: ==> swarp done ??? check, clean-up and continune to merge ... quitting"
        ec "#-----------------------------------------------------------------------------"
		exit 0
    fi

	nout=$(ls -1 pswarp_paw?_s??_??.out 2> /dev/full | wc -l)
	nlog=$(ls -1 pswarp_paw?_s??_??.log 2> /dev/full | wc -l)
	if [ $nout -ge 1 ] || [ $nlog -ge 1 ]; then
		ec "ATTN: found $nout pswarp .out and $nlog pswarp .log files ... delete them and continue??"
		askuser
	fi
		
    rm -f pswarp.qall estats pswarp*.*     # just in case

	#----------------------------------------------------------------------------------------
	# Here we build substacks by season, then we can merge then by season to build the seasons
	# stacks, or all together to build the full stack, or seasons 1-5 to build a pseudo-DR3.
	# 
	#----------------------------------------------------------------------------------------
	
	if [ ! -e list_season ]; then
		$bindir/mkSeasonList.sh
	fi

	# build seasons list from list_season
	for id in $(seq -w 01 16); do
		grep "S$id" list_season | cut -d\  -f1 > list_s$id
		if [ ! -s list_s$id ]; then rm list_s$id; fi		
	done 
	
	# loop over lists by season
	ec "##  Begin loop over seasons ..."

	for imlist in list_s[0-3]?; do 
		tail=${imlist: -3}                  # suffix to use in script names (like s??)
		nimages=$(cat $imlist | wc -l)      # num images in list
		mkplists $imlist                    # split season list by paws
		npaws=$(ls list_paw? 2> /dev/null | wc -l)

		imroot=$(head -$(($nimages / 2))  $WRK/images/$imlist | tail -1 | cut -d\. -f1 | cut -d\/ -f2 )
		
		ec "#-----------------------------------------------------------------------------"
		ec "## Season $tail, list is $imlist with $nimages files, $npaws paws: Build qsub files"

		# -----------  Finished setting up -----------

        ec "## Will use files like:"
		ex=$(ls -l cleaned/${imroot}_clean.fits | tr -s ' ' | cut -d' ' -f9-13)
		ec "  - images .... $ex" 
		ex=$(ls -l weights/${imroot}_weight.fits | tr -s ' ' | cut -d' ' -f9-13)
		ec "  - weights ... $ex" 
		ex=$(ls -l heads/$imroot.head | tr -s ' ' | cut -d' ' -f9-13)
		ec "  - heads ..... $ex" 

		subsky=N                             # for pass2 DO NOT subtract sky
		ec "#-----------------------------------------------------------------------------"

		nim=500  # approx num of images in each sublist
        for list in list_paw?; do  
			nl=$(cat $list | wc -l)
            ppaw=$(echo $list | cut -d\_ -f2)       # NEW temporary name for full paw
            split -n l/$(($nl/$nim+1)) $list --additional-suffix='.lst' pswarp_${ppaw}_${tail}_
			for slist in pswarp_${ppaw}_${tail}_??.lst; do
				nl=$(cat $slist | wc -l)    
				paw=$(echo $slist | cut -d\_ -f2-4 | cut -d\. -f1)    # ; echo $paw
				outname=substack_${paw}
				#ec "DEBUG:  For $imlist, paw $paw, $nl images ==> $outname with subsky $subsky"
           
                # ---------------------- Local run by sublist ----------------------
				
				qfile="pswarp_${paw}.sh"; touch $qfile; chmod 755 $qfile
				sed -e 's|@NODE@|'$node'|'  -e 's|@IDENT@|'$PWD/pswarp'|'  -e 's|@DRY@|'$dry'|'  \
					-e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'$slist'|'  -e 's|@WRK@|'$WRK'|' \
					-e 's|@PAW@|'$paw'|'  -e 's|@PPN@|5|'  -e 's|@HEADFILE@|'$headfile'|'                     \
					-e 's/@SUBSKY@/'$subsky'/'  -e 's|@PASS@|'$pass'|'  $bindir/pswarp.sh > $qfile
            
				ec " . Built $qfile with $nl images for paw $paw ==> $outname"
				echo "qsub $qfile; sleep 1" >> pswarp.qall
			done
        done 
	done
    ec "#-----------------------------------------------------------------------------"
	nq=$(cat pswarp.qall | wc -l)
    ec "# ==> written to file 'pswarp.qall' with $nq entries "
    ec "#-----------------------------------------------------------------------------"
	rm list_paw?

    if [ $dry == 'T' ]; then echo "   >> EXITING DRY MODE << "; exit 3; fi

    ec "# Submit qsub files ... ";  source pswarp.qall # >> $pipelog
    ec " >>>>   Wait $nq pswarp jobs ... first check in 1 min  <<<<<"
	
    if [ $nimages -lt 500 ]; then nsec=10; else nsec=300; fi    # useful in testing
	ostr="ddd"                     # a dummy string for comparisons within the loop
    btime=$(date "+%s.%N");  sleep 60           # begin time
    while :; do           #  begin qsub wait loop for pswarp
        njobs=$(qstat -au moneti | grep sw_${FILTER} | wc -l)
        [ $njobs -eq 0 ] && break          # jobs finished
		
		# check every $nsec sec, and if a new substack is done then print this message
		str=$(ls -lthr substack_paw?_s??_??.fits 2> /dev/null | tr -s ' ' | cut -d' ' -f4-9 | tail -1)
		if [[ $str != $ostr ]]; then echo "$(date "+[%d.%h %T]") $njobs running or queued; last substack:  $str " ; fi
		ostr=$str
        sleep $nsec
    done  
    ec "#-----------------------------------------------------------------------------"
    ec "# pswarp finished; walltime $(wtime)"
    
   	# check exit status
    grep EXIT\ STATUS pswarp_paw?_s??_??.out >> estats
    nbad=$(grep -v STATUS:\ 0  estats | wc -l)  # files w/ status != 0
    if [ $nbad -gt 0 ]; then
        ec "# PROBLEM: pswarp2_paw?_s??_??.sh exit status not 0: "
		grep -v STATUS:\ 0 estats 
		askuser
    fi
    ec "# CHECK: pswarp scripts exit status ok"; rm estats

	# check num substacks found
 	nn=$(ls substack_paw?_s??_??.fits | wc -l)
	if [ $nn -lt $nq ]; then
		ec "# PROBLEM:  found only $nn substacks for $nq expected ..."
		askuser 
	fi
	chmod 644 pswarp_paw?_s??_??.out pswarp_paw?_s??_??.log
	chmod 644 substack_paw?_s??_??.fits substack_paw?_s??_??_weight.fits
	
	# check sizes of sustacks
    ns=$(\ls -l substack_paw?_s??_??.fits | \
		tr -s ' ' | cut -d ' ' -f5,5 | sort -u | wc -l)
    if [ $ns -gt 1 ]; then 
        ec "# PROBLEM: substacks not all of same size .... "
        ls -l substack_paw?_s??_??.fits 
        askuser
    fi
	
	# check for WARNINGS in logfiles: 
	# one WARNING of "fits header data read from substack....head" ok
	for f in pswarp_paw?_s??_??.log; do
		grep WARNING $f > ${f%.log}.warn
		nn=$(($(cat ${f%.log}.warn | wc -l) - 1))
		if [ $nn -ge 1 ]; then 
			ec "# ATTN: found $nn unexpected warnings in pswarp $f"
			askuser	
		else
			rm ${f%.log}.warn
		fi
	done

    # rm the links to the headfile
	rm substack_paw?_s??_??.head

    if [ ! -d $outdir ]; then mkdir $outdir; fi
    mv pswarp*.* substack*.*   $outdir
	rm list_paw? list_s??

    ec "#-----------------------------------------------------------------------------"
    ec "# Finished swarping; moved scripts, lists and products to $outdir"
    ec "#-----------------------------------------------------------------------------"

#	exit 0
echo $auto	
#-----------------------------------------------------------------------------------------------
elif [[ $1 == 'merge' ]] || [[ $auto == 'T' ]]; then      # 
#-----------------------------------------------------------------------------------------------
	
    ncurr=4 ; pcurr="p$ncurr" ; pprev=$(($ncurr-1)) ; pnext="p$(($ncurr+1))"
    mycd $WRK/images

    ec "##----------------------------------------------------------------------------"
    ec "#"
    ec "##          ======  BEGIN MERGING  ======"
    ec "#"
    ec "##----------------------------------------------------------------------------"

    # default directory for the swarp products (inputs)
	# TEMP: need to re-define it here in case starting at merge
	outdir="swarp_48k"_$runID     

	# Release name
	REL=DR5

	# stacks to build:
	doFull=1   # Full 
	doPdr3=0   # DR3-equivalent (fewer seasons)
	doSesn=1   # Season
	doPaws=1   # paws

    # if products already there, then swarp with high res (cosmos) headfile
	if [ -d $outdir ]; then
		nss=$(ls -1 $outdir/substack_paw?_s??_??.fits 2> /dev/null | wc -l)
	else
		nss=0
	fi


	if [ $nss -ne 0 ]; then    # products not yet built
		ec "#-----------------------------------------------------------------------------"
		ec "# ATTN: New run of merge ==> build low-res substacks in $outdir"
		resol=48k
	else
		ec "#-----------------------------------------------------------------------------"
		ec "# ATTN: Found $nss low-res substacks in $outdir ==> build high res stacks"
		resol=hr
#		outdir="swarp_hr"_$runID
		outdir="swarp_48k"_$runID
	fi

    ec "# Link substacks from $outdir to current dir"
	ln -sf $outdir/substa*.fits .        # link substacks to work dir
	ec "#-----------------------------------------------------------------------------"

    #----------------------------------------------------------------------------------------#
    #          Merge substacks into full and partial stacks
    #----------------------------------------------------------------------------------------#

	stout=UVISTA_${REL}-${FILTER}_full_${resol}_$runID     #; echo $stout
    if [ -e $stout.fits ]; then 
        ec "#CHECK: stack $stout already exists ... merge already done ... quitting "
		exit 0
    else
		rm -f pmerge_full.* pmerge_dr3.* pmerge.qall pmerge_*.lst

	    # check num of substacks vs num expected:
		ls -1 substack_paw?_s??_??.fits > pmerge_full.lst 2> /dev/null # if season stack are available
		nfnd=$(cat pmerge_full.lst | wc -l)         # number found
		nexp=$(cat $outdir/pswarp.qall | wc -l)     # number expected
		
		if [ $nfnd -eq 0 ]; then
			ec "ERROR: no substacks found to merge .... quitting"
			exit 2
		fi

		if [ $nfnd -ne $nexp ]; then
			ec "ERROR: found $nfnd substacks for $nexp expected from pswarp.qall .... "
			askuser
			exit 2
		fi
		
		# 1. full stack, all substacks:
		if [ $doFull -eq 1 ]; then 
		    ec "# Merge all $nfnd substacks into $stout ..."
		    ec "#-----------------------------------------------------------------------------"
		    
		    qfile="pmerge_full.sh"; touch $qfile; chmod 755 $qfile
		    sed -e 's|@NODE@|'$node'|'   -e 's|@IDENT@|'$PWD/pmerge_full'|'  -e 's|@DRY@|'$dry'|'  \
		    	-e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'pmerge_full.lst'|'  -e 's|@WRK@|'$WRK'|'  \
		    	-e 's|@STOUT@|'$stout'|'   -e 's|@SET@|'full'|'  $bindir/pmerge.sh  \
				-e 's|@PASS@|'$pass'|'     -e 's|@TAG@|full|'   > ./$qfile
		    
		    ec "# . Built $qfile with $nfnd entries"
		    ec "#-----------------------------------------------------------------------------"
		    echo "qsub $qfile; sleep 1" >> pmerge.qall
		fi
		    
    	#----------------------------------------------------------------------------------------#
    	#          Merge substacks by season into seasons stacks
    	#----------------------------------------------------------------------------------------#

		if [ $doSesn -eq 1 ]; then
			ec "## Build season stacks from substacks:"
		    ec "#-----------------------------------------------------------------------------"
			rm -rf pmerge_s??.lst
			for s in $(ls substack*_??.fits | cut -d\s -f4 | cut -d\_ -f1 | sort -u); do 
     			# build list of substacks with given season
				root=pmerge_s${s}
				list=$root.lst 
	    		ls -1 substack_paw?_s${s}_??.fits > $list
	    		nfnd=$(cat $list | wc -l)                # number files found
				#ec "# Built $list with $nfnd entries"
				
	    		stout=UVISTA_${REL}-${FILTER}_s${s}_${resol}_$runID
				
	    		if [ $nfnd -eq 0 ]; then
	    			ec "ERROR: no season ${s} substacks found to merge .... quitting"
	    			exit 2
	    		elif [ $nfnd -eq 1 ]; then
	    			ec "#  Found only 1 substack for season $s ... Nothing to merge "
	    			ec "# . just copy that substack to $stout"
	    			if [ $dry == 'F' ]; then 
	    				r=$(cat $list)
	    				cp $r ${stout}.fits
	    				cp ${r%.fits}_weight.fits ${stout}_weight.fits
	    			fi
	    			rm $list
				else
#	    			ec "#-----------------------------------------------------------------------------"
	    			ec "#  Merge $nfnd substacks for season $s into $stout ..."
					
	    			qfile=$root.sh; touch $qfile; chmod 755 $qfile
	    			sed -e 's|@NODE@|'$node'|'     -e 's|@IDENT@|'$PWD/$root'|'  -e 's|@DRY@|'$dry'|'  \
	    				-e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'$list'|'  -e 's|@WRK@|'$WRK'|'  \
	    				-e 's|@STOUT@|'$stout'|'   -e 's|@SET@|'${s}'|'  -e 's|@TAG@|'s$s'|' \
					-e 's|@PASS@|'$pass'|'  $bindir/pmerge.sh  > ./$qfile
	    			
	    			ec "# . Built $qfile with $nfnd entries"
#	    			ec "#-----------------------------------------------------------------------------"
	    			echo "qsub $qfile; sleep 1" >> pmerge.qall
	    		fi
			done
	    	ec "#-----------------------------------------------------------------------------"
		fi

	    #----------------------------------------------------------------------------------------#
	    #          Merge substacks by paw into paw stacks
	    #----------------------------------------------------------------------------------------#

		if [ $doPaws -eq 1 ]; then
			ec "## Build paw stacks from substacks:"
		    ec "#-----------------------------------------------------------------------------"
			rm -rf pmerge_p?.lst
			for p in $(ls substack*_??.fits | cut -d\w -f2 | cut -d\_ -f1 | sort -u); do 
				# Build list of substacks with given paw
				root=pmerge_p${p}
				list=$root.lst 
	    		ls -1 substack_paw${p}_s??_??.fits > $list 
	    		nfnd=$(cat $list | wc -l)                # number files found
				#ec "# Built $list with $nfnd entries"

	    		stout=UVISTA_${REL}-${FILTER}_paw${p}_${resol}_$runID
				
	    		if [ $nfnd -eq 0 ]; then
	    			ec "ERROR: no substacks for paw ${p} found to merge .... quitting"
	    			exit 2

	    		elif [ $nfnd -eq 1 ]; then
	    			ec "#  Found only 1 substack for paw $p ... Nothing to merge "
	    			ec "#  . just copy that substack to $stout"
	    			if [ $dry == 'F' ]; then 
	    				r=$(cat $list)
	    				cp $r ${stout}.fits
	    				cp ${r%.fits}_weight.fits ${stout}_weight.fits
	    			fi
	    			rm $list
	    		else
	    			ec "#  Merge $nfnd substacks for paw $p into $stout ..."
					
	    			qfile=$root.sh; touch $qfile; chmod 755 $qfile
	    			sed -e 's|@NODE@|'$node'|'   -e 's|@IDENT@|'$PWD/$root'|'  -e 's|@DRY@|'$dry'|'  \
	    				-e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'$list'|'  -e 's|@WRK@|'$WRK'|'  \
	    				-e 's|@STOUT@|'$stout'|'   -e 's|@SET@|'p${p}'|'  -e 's|@TAG@|'p$p'|' \
						-e 's|@PASS@|'$pass'|'  $bindir/pmerge.sh  > ./$qfile
	    			
	    			ec "#  . Built $qfile with $nfnd entries"
	    			echo "qsub $qfile; sleep 1" >> pmerge.qall
	    		fi
			done
		fi

        #----------------------------------------------------------------------------------------#
        #          Merge substacks by season into seasons stacks
        #----------------------------------------------------------------------------------------#

	    # 2. pseudo DR3 from seasons 1-5:		
		if [ $doPdr3 -eq 1 ]; then 
		    stout=UVISTA_${REL}-${FILTER}_dr3_${resol}_$runID
			ec "## Merge seasons 1-5 substacks into pseudo-DR3 stack: $stout ..."
		    ls -1 substack_paw?_s??_??.fits > pmerge_dr3.lst 2> /dev/null
		    nfnd=$(cat pmerge_dr3.lst | wc -l)    # number found
		    ec "## Merge all $nfnd substacks into pseudo DR3: $stout ..."
		    
		    qfile="pmerge_dr3.sh"; touch $qfile; chmod 755 $qfile
		    sed -e 's|@NODE@|'$node'|'     -e 's|@IDENT@|'$PWD/pmerge_dr3'|'  -e 's|@DRY@|'$dry'|'  \
		    	-e 's|@FILTER@|'$FILTER'|' -e 's|@LIST@|'pmerge_dr3.lst'|'  -e 's|@WRK@|'$WRK'|'  \
		    	-e 's|@STOUT@|'$stout'|'   -e 's|@SET@|'dr3'|'  -e 's|@TAG@|full|' \
				-e 's|@PASS@|'$pass'|'   $bindir/pmerge.sh  > ./$qfile
		    
		    ec "# Built $qfile with $nfnd entries"
		    ec "#-----------------------------------------------------------------------------"
		    echo "qsub $qfile" >> pmerge.qall
		fi
	fi                 # 

    #----------------------------------------------------------------------------------------#
    #          submit jobs
    #----------------------------------------------------------------------------------------#
 
	nq=$(cat pmerge.qall | wc -l)
	ec "#-----------------------------------------------------------------------------"
	ec "# written file 'pmerge.qall' with $nq entries "
	ec "#-----------------------------------------------------------------------------"
    if [ $dry == 'T' ]; then 
		echo "   >> EXITING DRY MODE << "
		for f in substa*.fits; do                
			if [ -h $f ]; then rm $f; fi    # delete links where they were built
		done
		exit 3
	fi

    ec "# submit qsub files ... "; source pmerge.qall
    ec " >>>>   wait for pmerge jobs to finish ...   <<<<<"
    
    btime=$(date "+%s.%N")
	sleep 60              # before starting wait loop
    while :; do           #  begin qsub wait loop for pmerge
        njobs=$(qstat -au moneti | grep merge_${FILTER} | wc -l)
        [ $njobs -eq 0 ] && break          # jobs finished
        sleep 60
    done  
    ec "# pmerge jobs finished - now check exit status"
	nout=$(ls pmerge*.out 2> /dev/null | wc -l)
	if [ $nout -eq 0 ]; then
		ec "# PROBLEM: no pmerge.out files found ... nothing done?  quitting"
		exit 5
    fi

    nbad=$(grep EXIT pmerge_*.out | grep -v STATUS:\ 0 | wc -l)
    if [ $nbad -ne 0 ]; then
        ec "PROBLEM: pmerge exit status not 0 ... check pmerge.out"
		grep EXIT pmerge_*.out | grep -v STATUS:\ 0 
        askuser
	fi
    
	ec "CHECK: pmerge.sh exit status ok ... continue"
    ec "# $stout and associated products built:"
    ls -lh UVISTA_${REL}-${FILTER}_*${resol}_${runID}*.fits
    ec "# ..... GOOD JOB! "
	for f in substa*.fits; do    ## remove links to substacks
        if [ -h $f ]; then rm $f; fi
    done
	mv pmerge_*.*  $outdir

    ec "#-----------------------------------------------------------------------------"
    ec "# Finished merging; moved scripts, lists and logs to $outdir"
    ec "#-----------------------------------------------------------------------------"
	ec ""
	ec "##------------------------  END OF DR4 PIPELINE ------------------------------"
    ec "#-----------------------------------------------------------------------------"

#-----------------------------------------------------------------------------------------------
else
   echo "!! ERROR: $1 invalid argument ... valid arguments are:"
   help
fi 

exit 0

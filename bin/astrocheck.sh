#!/bin/sh
#-----------------------------------------------------------------------------
# Run SEx and scamp to perform astrometry check on selected stacks
#-----------------------------------------------------------------------------
if [[ ${@: -1} =~ 'dry' ]]; then
	DRY=T
	echo; echo "               ###### DRY mode ######"; echo
	nfiles=$(($#-1))
else
	nfiles=$#
	DRY=F
fi

echo "## Found $nfiles to proces .... ok?"  ; sleep 3

datadir=/n08data/UltraVista/DR6
confdir=/n23data1/moneti/config
refcat=/home/moneti/softs/uvis-pipe/config/GAIA-EDR3_1000+0211_r61.cat
pydir=/home/moneti/softs/uvis-pipe/python
sex_pars="-c $confdir/scamp.sex  -WEIGHT_TYPE MAP_WEIGHT -MAG_ZEROPOINT 30.0"
sca_pars="-SOLVE_ASTROM N  -SOLVE_PHOTOM N  -MATCH Y "
sca_pars="$sca_pars  -ASTREF_CATALOG FILE  -ASTREFCAT_NAME $refcat"


# get number of 'names' in path
nn=$(echo $1 | tr \/ \  | \wc -w) ; np=$(($nn+1))

for f in ${@:1:$nfiles}; do 
	root=${f%.fits}
    # if root begins with '/', then remove path
    if [ ${f:0:1} == "/" ]; then
        name=$(echo $root | cut -d\/ -f$np)
    else
        name=$root
    fi

	filt=$FILTER 

	if [[ ${name:0:8} == "substack" ]]; then    	# for substacks
		base=$f
		labl=$(echo $name | cut -d \_ -f2-4)
#		echo $base ; echo $name; echo $labl #; exit
	elif [[ ${name:13:4} == "full" ]]; then         # for full stacks
		base=$root#                              ; echo "$base      $name"
		labl=$(echo $name | cut -d\_ -f2-3)
		echo "$name      $labl"
	elif [[ ${name:12:2} == "_s" ]]; then          # for season stacks
		base=$root
		labl=$(echo $name | cut -d\_ -f2-3)
	elif [[ ${name:12:4} == "_paw" ]]; then          # for season stacks
		base=$root
		labl=$(echo $name | cut -d\_ -f2-3)
	fi

    weit=${root}_weight.fits      # input weight file
	ldac=$name.ldac               # output ldac catal
	logfile=$name.log             # scamp logfile

	if [[ ${name:0:8} == "substack" ]]; then    	# for substacks
		tag=$(echo $root | cut -d\/ -f$nn)
		tag=$(echo $tag | cut -d\_ -f2)
		ldac=substack_${tag}_${labl}.ldac
		logfile=substack_${tag}_${labl}.log
	elif [[ ${name:0:4} == "UVIS" ]]; then         
		comm3="rename _1.png .png *_1.png ; rename _1.l .l $ldac $logfile "     
	fi

	comm1="sex $f  -CATALOG_NAME $ldac -WEIGHT_IMAGE $weit $sex_pars" ; 

	ckn="-CHECKPLOT_RES 1200,800 -CHECKPLOT_TYPE FGROUPS,ASTR_REFERROR2D,ASTR_REFERROR1D "
	ckn="$ckn -CHECKPLOT_NAME groups_$labl,eref2d_$labl,eref1d_$labl"
	comm2="scamp $ldac $sca_pars $ckn -VERBOSE_TYPE FULL"


	if [[ $DRY == T ]]; then
		echo "####  astrocheck $base - Filter $filt ==>  $ldac"
		echo "# 1. $comm1"; echo "# 2. $comm2" ; echo #"# 3. $comm3" ; echo
	else
		echo "#-----------------------------------------------------------------------------"
		echo "#   Begin astrocheck on $f ... " #; exit
		echo "#-----------------------------------------------------------------------------"
		rm -f *_${labl}*.png $logfile

		# do sex if ldac not already present
		if [ ! -s $ldac ]; then 
			echo $comm1 ; echo; $comm1    
		else
			echo "1. Found $ldac ... skips sex"
		fi

		# run scamp
		echo "2. $comm2 > $logfile 2>&1 " | tee $logfile
		$comm2 > x  2>&1 
		echo "#------------------------------------------------------------------" >> $logfile
		grep Command_Line scamp.xml >> $logfile
		echo "#------------------------------------------------------------------" >> $logfile
		strings x >> $logfile  ; rm x
		
		$pydir/scamp_xml2dat.py scamp.xml ; mv scamp.dat pscamp_${tag}_${labl}.dat

		echo "3. Now rename scamp png files ..."     #; ls -l *_1.png $ldac $logfile ; echo
		if [[ ${name:0:8} == "substack" ]]; then    	# for substacks
			rename _paw _${tag}_paw *${labl}_1.png   #; ls -l *_1.png $ldac $logfile ; echo
			rename _1.p .p [eg]*${labl}_1.png        ; ls -l *${labl}.png $ldac $logfile  ; echo
#		    rename _paw _${tag}_paw $ldac $logfile 
		elif [[ ${name:0:4} == "UVIS" ]]; then         
			$comm3
		fi
		
	fi
done

rm -f psphot*png disto*png *chi2*png *interr*png *.head # scamp.xml
exit 0

#-----------------------------------------------------------------------------
# Summaries
#-----------------------------------------------------------------------------

# do them all
astrocheck.sh  $WRK/images/swarp_p2m_lr/substack_paw*_s???.fits  $WRK/images/UVI*_lr_1.fits

# for the UVISTA stacks
grep UVIS pscamp__p1s*.dat | cut -d\_ -f6-9 | sed 's/_lr_1//' 

# for the substacks:
# 1. summary of restuls
grep sub  pscamp_p1s_paw* | cut -d\_ -f6-7
# 2. as above, sorted by season
grep sub  pscamp_p1s_paw* | cut -d\_ -f7 | sort -k1,1


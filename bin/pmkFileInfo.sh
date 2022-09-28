#!/bin/sh
set -u

export PATH=$PATH:/softs/dfits/bin

filter=$1
case $filter in
   Y     ) wdir=/n09data/UltraVista_ConvertedData/$filter ;;
   J     ) wdir=/n08data/UltraVista_ConvertedData/$filter ;;
   H     ) wdir=/n08data/UltraVista_ConvertedData/$filter ;;
   Ks    ) wdir=/n09data/UltraVista_ConvertedData/$filter ;;
   K     ) wdir=/n09data/UltraVista_ConvertedData/$filter ;;
   NB118 ) wdir=/n08data/UltraVista_ConvertedData/$filter ;;
   N     ) wdir=/n08data/UltraVista_ConvertedData/$filter ;;
esac

echo "#-----------------------------------------------------------------------------"
echo "## Build stack.imlist files"
echo "#-----------------------------------------------------------------------------"

for f in $wdir/stacks/v20*_st.fits; do
   root=${f%.fits} 
   if [ ! -e ${root}.imlist ]; then 
      dfits -x 1 $f | grep Card | cut -d\' -f2 | sed 's/\[1\]/s/g'  > ${root}.imlist
      echo ">> wrote ${root}.imlist"
   fi
done

echo "#-----------------------------------------------------------------------------"
echo "## Build FileInfo.dat table"
echo "#-----------------------------------------------------------------------------"
# NB: FILTER and TARG NAME (PAW) kwds in hd0, others in hd 1-16
echo "# Build info table (for each image, paw, flat, sky and mask)"
echo "# To be complete need to add bpm to the table, but I didn't figure out where from"

cd $wdir/images

dfits v20*.fits | fitsort -d OBJECT # | sed 's/\t/ /g' | tr -s \   #| tee -a ff1.txt
echo "ff1 done"

dfits -x 1 v20*.fits | grep 'HIERARCH ESO DET DIT '  | tr -s \  | cut -d\  -f6 | cut -d\. -f1 > ff2.txt
echo "ff2 done"

dfits -x 1 v20*.fits | fitsort -d FLATCOR SKYSUB | \
   sed -e 's/Done with //' -e 's/\[1\]/s/g' -e 's/fit /fits /g' | \
   cut -f2- | sed 's/\t/ /g' | tr -s \  > ff3.txt
echo "ff3 done"

for f in v20*_00???.fits; do grep $f ../stacks/v20*.imlist | \
   cut -d\: -f1 | cut -d\/ -f3 | sed 's/imlist/fits/' >> ff4.txt; done
echo "ff4 done"

paste -d \  ff1.txt ff2.txt ff3.txt ff4.txt > ../FileInfo.txt 
rm ff?.txt
## ATTN: missing bpm file from table

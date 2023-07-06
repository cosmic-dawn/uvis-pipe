#!/bin/sh
#-----------------------------------------------------------------------------
# 22.oct.12:  modified to work on new data for DR6; all filters combined;
#             ==> remove filter dependency
# 6.jun.23: modified to work on list of converted files and include bpm data
#-----------------------------------------------------------------------------
set -u

export PATH=$PATH:/softs/dfits/bin

echo "#-----------------------------------------------------------------------------"
echo "## Build FileInfo.dat table"
echo "#-----------------------------------------------------------------------------"
# Nfiles are /n08data/UltraVista/DR6/N/images/origs/v2010011[7-9]*.fits v2010012*.fits
# J files are /n08data/UltraVista/DR6/J/images/origs/v2010011*.fits   v20100120*.fits 


dfits -x1 $* | fitsort -d OBJECT FILTER IMRED_FF IMRED_MK STACK SKYSUB | \
    sed -e 's/Done with //' -e 's/\[1\]/s/' -e 's/_st/_st.fits/' -e 's/\t/  /g' -e 's/   /  /g' \
	> FileInfo_temp.dat

exit


# NB: FILTER and TARG NAME (PAW) kwds in hd0, others in hd 1-16
echo "# Build info table (for each image, paw, flat, sky and mask)"
echo "# To be complete need to add bpm to the table, but I didn't figure out where from"

cd $wdir/images

dfits v20*.fits | fitsort -d OBJECT | sed 's/\t/ /g' | tr -s \  > ff1.txt
echo "ff1 done"

dfits -x 1 v20*.fits | grep 'HIERARCH ESO DET DIT '  | tr -s \  | \
	cut -d\  -f6 | cut -d\. -f1 > ff2.txt
echo "ff2 done"

dfits -x 1 v20*.fits | fitsort -d FLATCOR SKYSUB | \
   sed -e 's/Done with //' -e 's/\[1\]/s/g' -e 's/fit /fits /g' | \
   cut -f2- | sed 's/\t/ /g' | tr -s \  > ff3.txt
echo "ff3 done"

for f in v20*.fits; do grep $f ../stacks/v20*.imlist | \
   cut -d\: -f1 | cut -d\/ -f3 | sed 's/imlist/fits/' >> ff4.txt; done
echo "ff4 done"

paste -d \  ff1.txt ff2.txt ff3.txt ff4.txt > $wdir/FileInfo.txt 
##rm ff?.txt
## ATTN: missing bpm file from table

echo "## FileInfo.txt done"
#-----------------------------------------------------------------------------

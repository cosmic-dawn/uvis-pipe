#!/bin/sh
#-----------------------------------------------------------------------------
# Build lists by paw: built from FileInfo.dat which contains infor for
# all files
#-----------------------------------------------------------------------------

if [ -z ${WRK+x} ]; then 
	echo "!! ERROR: must export WRK variable before starting" ; exit 2; 
else
	FILTER=$(echo $WRK | cut -d/ -f5)
fi

mycd() { 
    if [ -d $1 ]; then \cd $1; echo " --> $PWD"; 
    else echo "!! ERROR: $1 does not exit ... quitting"; exit 5; fi
}

#-----------------------------------------------------------------------------

cd $WRK/images

rm -f list_paw?   # remove old lists

paws=" paw1 paw2 paw3 paw4 paw5 paw6 COSMOS"
for p in $paws; do grep -v ^# ../FileInfo.dat | grep $p  | cut -d \   -f1 > list_${p}; done

# if present, convert to paw0
if [ -e list_COSMOS ]; then mv list_COSMOS list_paw0; fi

# remove lists if empty
for f in list_paw?; do if [ ! -s $f ]; then rm $f; fi; done

nims=$(cat list_images | wc -l)
pims=$(cat list_paw? | wc -l)

if [ $nims -ne $pims ]; then
	echo "## ERROR: num images in paws not same as num images:"
	wc -l list_images ; wc -l list_paw?
fi

#for f in $(cat list_paw?); do ln -sf ldacs/${f%.fits}.ldac .; done

exit 0

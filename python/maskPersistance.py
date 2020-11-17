#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
#------------------------------------------------------------------
# maskPersistance.py
#------------------------------------------------------------------
# apply Bo's persistance mask to weight files:
# - in practice multiply the _weight files by 1-mask; 
# - input list contains root names of COSMOS files; built, eg, with
#   %grep COSMOS ../FileInfo.dat | cut -d\. -f1 > list_cosmos
#------------------------------------------------------------------
# AMo nov.17
# AMo may.18: modified to overwrite input weight file

import os,sys
import numpy as np
import astropy.io.fits as fits

imlist = "list_cosmos"

# Get the list of images
if not os.path.isfile(imlist):
    print("List %s not found ..."%imlist)
    sys.exit(1)

wlist = open(imlist)
for g in wlist:
    root = g.strip()
    wgt = "weights/" + root + "_weight.fits"
    msk = "Masks/"   + root + "_mask.fits"
    print("Fixing %s with %s" %(wgt, msk))
    w = fits.open(wgt, mode='update')
    m = fits.open(msk)
    
    for i in range(1,17):
        w[i].data *= (1-m[i].data)

    w[0].header['history'] = 'Multiplied by persistance mask'
    w.close()
    m.close()

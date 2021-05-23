#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
#
#-----------------------------------------------------------------------------
# for each extension, where flag = 7, change to flag = 4
# AMo - 11.dec.19
#-----------------------------------------------------------------------------
import sys
import numpy as np
import astropy.io.fits as fits
#from saturation_sub import merge_ldac

Nfiles = len(sys.argv) 

for n in range(1, Nfiles):
    f = sys.argv[n]    # ldac file
    cat = fits.open(f, mode="update")

    for chip in range(1,17):
        ext = chip * 2   # extensions to work on
        flg = cat[ext].data.field("FLAGS")
        flg[flg == 7] = 4
    cat.close()
    print("Done "+ f)

sys.exit()

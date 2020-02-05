#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
# ------------------------------------------------
# check bpms and invert if necessary; originals are overwritten
# Mar.18, AMo: 
# Feb.20, AMo: simplified for DR5
# ------------------------------------------------

import sys
import numpy as np
import astropy.io.fits as pyfits

for n in range(1,len(sys.argv)):
    ima = sys.argv[n]
    pima = pyfits.open(ima, mode='update')
    hd = pima[0].header
    if (np.mean(pima[4].data) < 0.5):
        print("Inverting {:}".format(ima))
        for ext in range(1,17):
            pima[ext].data = 1 - pima[ext].data
        hd['history'] = " Inverted original"
    else:
        print(" ok {:}".format(ima))
    pima.close(output_verify='silentfix+ignore')


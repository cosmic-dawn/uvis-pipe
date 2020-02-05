#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
#-----------------------------------------------------------------------------
# Compute %Num valid (= 1) of each extension of file(s)
#-----------------------------------------------------------------------------

import sys 
import numpy as np
import astropy.io.fits as pyfits

for n in range(1, len(sys.argv)):
    ima = sys.argv[n]
    pima  = pyfits.open(ima)
    n_ext = len(pima)         # num extensions
    if n_ext == 1: n_ext = 2  # to handle SEF files

    nv = []  # 
    for e in range(1, n_ext):
        Npix = pima[e].header['NAXIS1'] * pima[e].header['NAXIS2']
        
        nv.append(100*len(np.where(pima[e].data == 1)[0])/Npix)
        
    pima.close()
    ave = np.array(nv).mean()
    print("{:17s} ".format(ima.split('.')[0]), ' '.join(["{:6.2f}".format(x) for x in nv]), ' | {:6.2f}'.format(ave))

#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
#
#------------------------------------------------
# remove PV keywords from file ... if found
#------------------------------------------------

import sys, os  #, re, math
import numpy as np
import astropy.io.fits as pyfits

ima = sys.argv[1]

if not os.path.isfile(ima):
    print(">> ERROR: file {:} not found".format(ima))
    sys.exit(3)

pima = pyfits.open(ima, mode='update')
n_ext = len(pima)         # ; print(n_ext)
if n_ext == 1: n_ext=2    # to handle SEF files

for e in range(1, n_ext):     # delete PV kwds
    hdr = pima[e].header
    to_remove = []
    nn = 0
    for key in hdr:
        if "PV" in key:
            to_remove.append(key)
    if len(to_remove) > 0:
        for key in to_remove:
            del hdr[key]
            nn += 1
            
pima.close(output_verify='silentfix+ignore')
if nn > 0:
    print(">> Deleted PV kwds from {:}".format(ima))

#!/usr/bin/env python

# ------------------------------------------------
# simple subtraction of 2 images: first image is copied to output, then second image is subtracted; so 
# output will have same headers as first image.
# ------------------------------------------------

import sys, re, os
import numpy as np
import astropy.io.fits as pyfits
from optparse import OptionParser

parser = OptionParser()

parser.add_option('-j', '--ima', dest='ima', help='image', type='string', default="")
#parser.add_option('-k', '--im2', dest='im2', help='Negative image', type='string', default="")
#parser.add_option('-o', '--out', dest='out', help='Resulting difference', type='string', default="sum.fits")

# Parse command line
try:
    opts, args = parser.parse_args(sys.argv[1:])
except:
    print "Error ... check usage with imsub.py -h "
    sys.exit(1)

# prepare
pima = pyfits.open(opts.ima)
nn = len(pima)     # num extensions

# do the subraction
if nn == 1:     # Single extension:
#    print " %0.3f"%( pima[0].data.max()*1.e3 )
    print " %0.3f"%( pima[0].data.mean()*1.e3 )
else:           # MEF starting at 1
    for e in range(1, nn):
        pima[e].data = (pim1[e].data + pim2[e].data)/2
        print " %0.4f  %0.4f "%(np.min(pima[e].data), np.max(pima[e].data))

pima.close()


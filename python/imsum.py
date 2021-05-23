#!/usr/bin/env python

# ------------------------------------------------
# simple subtraction of 2 images: first image is copied to output, then second image is subtracted; so 
# output will have same headers as first image.
# ------------------------------------------------

import sys, re, os
import numpy as np
#import math
import astropy.io.fits as pyfits
from optparse import OptionParser

parser = OptionParser()

parser.add_option('-j', '--im1', dest='im1', help='positive image', type='string', default="")
parser.add_option('-k', '--im2', dest='im2', help='Negative image', type='string', default="")
parser.add_option('-o', '--out', dest='out', help='Resulting difference', type='string', default="sum.fits")

# Parse command line
try:
    opts, args = parser.parse_args(sys.argv[1:])
except:
    print("Error ... check usage with imsub.py -h ")
    sys.exit(1)

# prepare
os.system('cp ' +opts.im1 + ' ' + opts.out)
pim2 = pyfits.open(opts.im2)
pout = pyfits.open(opts.out, mode="update")
nn = len(pout)     # num extensions

# do the subraction
if nn == 1:     # Single extension:
    pout[0].data += pim2[0].data
    pout[0].data /= 2.
else:           # MEF starting at 1
    for e in range(1, nn):
        pout[e].data = (pim1[e].data + pim2[e].data)/2
        print(" {:0.4f}  {:0.4f} ".format(np.min(pout[e].data), np.max(pout[e].data)))

pim2.close()
pout.close()


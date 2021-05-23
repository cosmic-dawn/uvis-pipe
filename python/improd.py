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

parser.add_option('-j', '--im1', dest='im1', help='image 1', type='string', default="")
parser.add_option('-k', '--im2', dest='im2', help='image 2', type='string', default="")
parser.add_option('-l', '--im3', dest='im3', help='product', type='string', default="prod.fits")

# Parse command line
try:
    opts, args = parser.parse_args(sys.argv[1:])
except:
    print "Error ... check usage with imsub.py -h "
    sys.exit(1)

# prepare
os.system('cp ' +opts.im1 + ' ' + opts.im3)
pim2 = pyfits.open(opts.im2)
pim3 = pyfits.open(opts.im3, mode="update")
nn = len(pim3)     # num extensions

# do the subraction
if nn == 1:     # Single extension:
    pim3[0].data *= pim2[0].data
else:           # MEF starting at 1
    for e in range(1, nn):
        pim3[e].data *= pim2[e].data
        #print " %0.4f  %0.4f "%(np.min(pim3[e].data), np.max(pim3[e].data))
print "# built %s"%(opts.im3)

pim2.close()
pim3.close()

#!/usr/bin/env python

#----------------------------------------------------------------------
# Slight crop to remove ragged edges and surrounding area of 0s
#----------------------------------------------------------------------

import sys, re, os
import numpy as np
import astropy.io.fits as fits
from optparse import OptionParser

parser = OptionParser()

parser.add_option('-i', '--ima', dest='ima', help='input image', type='string', default="")
parser.add_option('-o', '--out', dest='out', help='output images', type='string', default="small.fits")

# Parse command line
try:
    opts, args = parser.parse_args(sys.argv[1:])
except:
    print("Error ... check usage with imshrink.py -h ")
    sys.exit(1)

# prepare
pima = fits.open(opts.ima)
hdu = fits.PrimaryHDU()

#hdu.data = pima[0].data[100:14850,100:18000]  # to shring UltraVISTA lo-res images
hdu.data = pima[0].data[3700:12600,4000:13000]  # to shring IRAC/COSMOS images

keys  =['CTYPE1','CTYPE2','CRVAL1','CRVAL2','CD1_1','CD1_2','CD2_1','CD2_2']
nkeys = len(keys)
for k in range(nkeys):
    key = keys[k]  
    hdu.header[key] = pima[0].header[key]

# not sure about this; but probably not important
hdu.header['CRPIX2'] = pima[0].header['CRPIX2'] - 7300
hdu.header['CRPIX1'] = pima[0].header['CRPIX1'] - 6400

hdu.writeto(opts.out, overwrite=True)
print("{} done ".format(opts.out))

pima.close()


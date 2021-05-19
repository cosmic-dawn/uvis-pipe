#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
#
#-----------------------------------------------------------------------------
# Destriping ... destripe.py
# Jun.18, AMo: using numpy
# Aug.18, AMo: change method to median
#-----------------------------------------------------------------------------
# inputs: list of files, input and output suffix
# output: destriped images
# procedure: for each MEF extension
# - build a masked array using the corresponding mask, marr
# - construct the medians of the columns (axis 0) and subtract it, 
# - then idem for the rows
# NB: destripes vertical before horizontal, which is N-S first for swarped SEF
# images, but E-W first for the pre-swarped MEF images. Results are not identical
# due to the median filtering, but comparable.
#-----------------------------------------------------------------------------

import math,sys,re,os
import numpy as np
import numpy.ma as ma
import astropy.io.fits as pyfits
from optparse import OptionParser

#-----------------------------------------------------------------------------

parser = OptionParser()

parser.add_option('-l', '--list', dest='list', help='List of images', type='string', default="")
parser.add_option('-i', '--isuf', dest='isuf', help='input suffix',   type='string', default="")
parser.add_option('-o', '--osuf', dest='osuf', help='output suffix',  type='string', default="_des")

opts, args = parser.parse_args(sys.argv[1:])
#-----------------------------------------------------------------------------

file = open(opts.list, 'r')
lines = file.readlines()
file.close()

for line in lines:
    xx = line.split()[0]
    root = xx.split(opts.isuf + ".fits")[0]
    root = xx.split(".fits")[0]

    im = root + opts.isuf + ".fits"
    mask = root + "_mask.fits"
    out  = root + opts.osuf + ".fits"
    print " >> Begin destriping %s with %s ==> %s"%(im, mask, out)
    
    os.system('cp ' + im + " " + out )  

    pout=pyfits.open(out, mode="update")
    pmsk=pyfits.open(mask)
    
    nn = len(pout)     # num extensions
    if nn == 1:
        rr = [0]
    else:
        rr = range(1,17)

    for i in rr:
        data=pout[i].data; hdr=pout[i].header
        mask=pmsk[i].data.astype(">i4")

        # destripe along Y
        marr = ma.array(data, mask=(1-mask))
        mmx  = ma.median(marr, axis=0).astype(">i4")
        data -= mmx

        #destripe along X
        marr = ma.array(data, mask=(1-mask))
        mmy  = ma.median(marr, axis=1)
        mmy  = mmy.reshape(data.shape[0],1).astype(">i4")
        data -= mmy

    pout.close()
    pmsk.close()
    print "    ... output is %s"%out


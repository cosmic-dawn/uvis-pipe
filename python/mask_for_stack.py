#!/usr/bin/env python
# -----------------------------------------------------------------
# Build mask for the stack
# ex. python $pydir/mask_for_stack.py -I UVIS_p1.fits -W UVIS_p1_weight.fits --threshold 0.7 --extendedobj
# -----------------------------------------------------------------

import sys, re, os
import numpy as np
from time import ctime
from subsky_sub import *
from ASCII_cat import *
from optparse import OptionParser

parser = OptionParser()

# Input Stack
parser.add_option('-I', '--image',  dest='image',  help='Reference stack', type=str, default="")
parser.add_option('-W', '--weight', dest='weight', help='Reference weight stack', type=str, default="")

# configuration PATHS
parser.add_option('--conf-path', dest='cpath', help='path for configuration files', type=str, default="")
parser.add_option('--script-path', dest='spath', help='path for script files', type=str, default="")

# Masking depth
parser.add_option('--threshold', dest='thresh', help='detection threshold in building masks', type=float, default="1.")
parser.add_option('--extendedobj', dest='extendedobj', help='Extended Objetcs ?', action='store_true', default=False)

# thresholds
parser.add_option('-m', '--min', dest='min', help='minimum ok',  type='float', default="-50.")
parser.add_option('-n', '--max', dest='max', help='maximum ok',  type='float', default="500000.")

try:
    opts,args = parser.parse_args(sys.argv[1:])
except:
    print "Error ... check usage with mask_for_stack.py -h "
    sys.exit(1)

#-----------------------------------------------------------------------------

print "## Run SExtractor to remove the objects and create a -OBJECT check image..."

root  = opts.image.split('.fits')[0]
stack = opts.image.split('/')[-1]
check = root + '_ob_check.fits'   # for -OBJECTS check image
flag  = root + '_obFlag.fits'     # for final object flag image

sexconf = opts.cpath + "/sex_for_mask.conf"
cparam_sex = {'CATALOG_TYPE': 'NONE', 'DEBLEND_MINCONT': 1, 'CLEAN': 'N',
              'DETECT_THRESH': opts.thresh, 'ANALYSIS_THRESH': opts.thresh, 
              'FILTER_NAME': opts.cpath + "/gauss_3.0_7x7.conv", 'WEIGHT_IMAGE': opts.weight, 
              'CHECKIMAGE_TYPE': '-OBJECTS', 'CHECKIMAGE_NAME': flag, 
              'PARAMETERS_NAME': opts.cpath + '/ss.param',
              'MEMORY_PIXSTACK': 5000000, 'VERBOSE_TYPE': 'QUIET'}

if opts.extendedobj:
    cparam_sex['BACK_SIZE'] = 512
    cparam_sex['BACK_FILTERSIZE'] = 5
sextract(opts.image, sexconf, cparam_sex)

#-----------------------------------------------------------------------------

print "## then build a flag file (python):"
print " - copy the check image to the flag file, and set non-zero values to 1"

out = pyfits.open(flag, mode='update')
data = out[0].data
data[data.nonzero()] = 1                          # set non-zero values to 1

print " - flag (to 0) pixels > %i and pixels < %i"%(opts.max, opts.min)

ima = pyfits.open(opts.image)                     # open stack image
data[ima[0].data < opts.min] = 0                  # flag low values in
data[ima[0].data > opts.max] = 0                  # flag high values
data = data.astype('UInt8')                       # convert to integer ... no effect, no error

# valid fraction:
d2 = data.reshape(data.size)
dz = d2.nonzero()
vf = 1.*len(dz[0])/len(d2)
print " - fraction of valid pixels: %0.2f"%vf

print "## Write history kwds of output flagfile"
out[0].header['history'] = "Original file: %s"%(stack)
out[0].header['history'] = "Threshold used to build mask: %0.2f"%(opts.thresh)
out[0].header['history'] = "Percent of valid pixels: %0.2f"%(100.*vf)

out.close(output_verify='silentfix+ignore')
ima.close()

print "## Done ... outflag is: %s"% flag

#-----------------------------------------------------------------------------

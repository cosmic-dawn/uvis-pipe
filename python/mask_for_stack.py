#!/opt/intel/intelpython2-2019.4-088/intelpython2/bin/python
#-----------------------------------------------------------------
# Build mask for the stack
# this version adapted for DR5 and beyond - AMo, Jan 2023
#
# SYNTAX $pydir/mask_for_stack.py file.fits (thresh)
# - assumes file_weight.fits is available
# - thresh is the fraction of the local RMS noise (determined by
#   sextractor) above which pixels belong to objects
# - produces file_obFlag-th{n.nn}.fits
#   The output object flag is 0 on ojbect, 1 elsewhere (sky)
#-----------------------------------------------------------------
# AMo - updated for DR6 2.Feb.23
#       minor updates 6.apr.23
#-----------------------------------------------------------------

import sys, re, os
import numpy as np
from time import ctime
from ASCII_cat import *
import astropy.io.fits as pyfits

#-----------------------------------------------------------------------------
# Sextractor wrapper
#-----------------------------------------------------------------------------
def sextract(image, sexconf, cparam):
    cstr = ""
    for k in cparam:
	cstr += " -" + str(k) + " " + str(cparam[k]) + " "

    print "SEXtractor command line:"
    print "  sex %s -c %s %s" % (image, sexconf, cstr)
    os.system("sex %s -c %s %s" % (image, sexconf, cstr));
    print ""

#-----------------------------------------------------------------------------

uvis = "/home/moneti/softs/uvis-pipe/"
cpath = uvis+"config"

image = sys.argv[1]
weight = image.split('.')[0] + "_weight.fits"

# Detection threshold for SExtractor
if len(sys.argv) == 3 :
    thresh = sys.argv[2]
else:
    thresh = 0.7

# if true, increases BACK_SIZE and BACK_FILTERSIZE to larger than value.
extendedobj = True

#-----------------------------------------------------------------------------

print "# Begin mask_for_stack.py for "+image
print "#-----------------------------------------------------------------------------"
print "## 1. Run SExtractor to remove the objects and create a -OBJECT check image..."
print "## detect thresh = "+thresh+"; extendedobj =",extendedobj
print ""

root  = image.split('.fits')[0]
stack = image.split('/')[-1]
#check = root + '_ob_check.fits'             # for -OBJECTS check image
flag  = root + '_obFlag-th'+thresh+'.fits'    # for final object flag image

sexconf = cpath + "/sex_for_mask.conf"
cparam_sex = {'CATALOG_TYPE': 'NONE', 'DEBLEND_MINCONT': 1, 'CLEAN': 'N',
              'DETECT_THRESH': thresh, 'ANALYSIS_THRESH': thresh, 
              'FILTER_NAME': cpath + "/gauss_3.0_7x7.conv", 'WEIGHT_IMAGE': weight, 
              'CHECKIMAGE_TYPE': '-OBJECTS', 'CHECKIMAGE_NAME': flag, 
              'PARAMETERS_NAME': cpath + '/ss.param',
              'MEMORY_PIXSTACK': 5000000, 'VERBOSE_TYPE': 'QUIET'}

if extendedobj:
    cparam_sex['BACK_SIZE'] = 512
    cparam_sex['BACK_FILTERSIZE'] = 5

sextract(image, sexconf, cparam_sex)

## sys.exit()
#-----------------------------------------------------------------------------

#print "## 2. build a flag file:"
print "## 2. set non-zero values (sky) in the -OBJECT to 1 to convert it to object flag"
# this check images is 0 where sextractor found a source and unchanged elsewhere, ie. on sky

out = pyfits.open(flag, mode='update')
ddd = out[0].data

flat = ddd.flatten()
#nz = flat.nonzero()
print '# some info on the -OBJECTS image:'
print '- mean   ', np.mean(flat)
print '- stdev  ', np.std(flat)

# Now convert to mask (or flag)
ddd[ddd.nonzero()] = 1                          # set non-zero values to 1 (sky)
ddd = ddd.astype('UInt8')                   # convert to integer ... no effect, no error

# final fraction of valid (sky) pixels:
d2 = ddd.reshape(ddd.size)
dz = d2.nonzero()
vf = 100.*len(dz[0])/len(d2)  # valid fraction
print " - Percent valid pixels: %0.2f"%(vf)

print "# Write kwds to output flagfile"
out[0].header['inp_file'] = (stack,         "Original file")
out[0].header['sky_mean'] = (np.mean(flat), "Input sky mean level")
out[0].header['sky_nois'] = (np.std(flat) , "Input sky rms noise")
out[0].header['msk_thrs'] = (thresh,        "Threshold used to build mask")
out[0].header['valid_fr'] = (vf,            "Percent valid pixels")

out.close(output_verify='silentfix+ignore')

print "## Done ... outflag is: %s"% flag

#-----------------------------------------------------------------------------

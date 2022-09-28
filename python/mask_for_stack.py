#!/opt/intel/intelpython2-2019.4-088/intelpython2/bin/python
# -----------------------------------------------------------------
# Build mask for the stack
# ex. python $pydir/mask_for_stack.py -I UVIS_p1.fits -W UVIS_p1_weight.fits --threshold 0.7 --extendedobj
# The output object flag (_obFlag) is 0 on an ojbect, 1 elsewhere (sky)
# -----------------------------------------------------------------

import sys, re, os
import numpy as np
from time import ctime
from ASCII_cat import *
import astropy.io.fits as pyfits
from optparse import OptionParser

#-----------------------------------------------------------------------------
# Sextractor wrapper
#-----------------------------------------------------------------------------
def sextract(image, sexconf, cparam):
    cstr = ""
    for k in cparam:
	cstr += " -" + str(k) + " " + str(cparam[k]) + " "

    print "  sex %s -c %s %s" % (image, sexconf, cstr)
    os.system("sex %s -c %s %s" % (image, sexconf, cstr));

#-----------------------------------------------------------------------------

parser = OptionParser()

# Input Stack
parser.add_option('-I', '--image',  dest='image',  help='Reference stack', type=str, default="")
parser.add_option('-W', '--weight', dest='weight', help='Reference weight stack', type=str, default="")

# configuration PATHS
parser.add_option('--conf-path',   dest='cpath', help='path for configuration files', type=str, default="")
parser.add_option('--script-path', dest='spath', help='path for script files', type=str, default="")

# Masking depth
parser.add_option('--threshold',   dest='thresh', help='detection threshold in building masks', type=float, default="1.")
parser.add_option('--extendedobj', dest='extendedobj', help='Extended Objetcs ?', action='store_true', default=False)

# thresholds
parser.add_option('-m', '--min', dest='min', help='minimum ok',  type='float', default="-50.")
parser.add_option('-n', '--max', dest='max', help='maximum ok',  type='float', default="50.")

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

sys.exit()
#-----------------------------------------------------------------------------

print "## then build a flag file (python):"
print " - in the -OBJECT check image, set non-zero values(sky) to 1"
# this check images is 0 where sextractor found a source and unchanged elsewhere, ie. on sky

out = pyfits.open(flag, mode='update')
data = out[0].data
flat = data.flatten()
nz = flat.nonzero()
print '# some info on the -OBJECTS image:'
print '- mean   ', np.mean(flat)
print '- median ', np.median(flat)
print '- stdev  ', np.std(flat)
print '- mean(nonzero):   ', np.mean(nz)
print '- median(nonzero): ', np.median(nz)
print '- stdev(nonzero):  ', np.std(nz)

# Histogram

#fig, ax = plt.subplots(2,1, figsize=(20, 20))
#hist,bins,pat = ax[0].hist(nz, range=(-100,50), bins=150, log=True)
#ax[1].imshow(core, vmin=-3, vmax=3, origin='lower', cmap='gist_heat')



# Now convert to mask (or flag)
data[data.nonzero()] = 1                          # set non-zero values to 1 (sky)

print " - Set to 0 other pixels with abnormal values:"  #%(opts.thresh)

ima = pyfits.open(opts.image)                     # open stack image
data[ima[0].data < opts.min] = 0                  # flag low values in
data[ima[0].data > opts.max] = 0                  # flag high values
data = data.astype('UInt8')                       # convert to integer ... no effect, no error

# final fraction of valid (sky) pixels:
d2 = data.reshape(data.size)
dz = d2.nonzero()
vf = 100.*len(dz[0])/len(d2)  # valid fraction
print " - Percent valid pixels: %0.2f"%(vf)

print "## Write history kwds of output flagfile"
out[0].header['history'] = "Original file: %s"%(stack)
out[0].header['history'] = "Threshold used to build mask: %0.2f"%(opts.thresh)
out[0].header['history'] = "Percent valid pixels: %0.2f"%(vf)

out.close(output_verify='silentfix+ignore')
ima.close()




print "## Done ... outflag is: %s"% flag

#-----------------------------------------------------------------------------

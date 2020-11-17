#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
#
#-----------------------------------------------------------------------------
# cp_astro_kwds.py
#-----------------------------------------------------------------------------
# Copy astrometry kwds from image to anoter file
# - input is image name (root.fits)
# - other is suffix like root_suff.fits
# - mask?  if so convert to byte
#-----------------------------------------------------------------------------
import os,sys
import numpy as np
import astropy.io.fits as pyfits
from optparse import OptionParser

parser = OptionParser()
parser.add_option('-i','--image', dest='image', help='image name', type='string', default="")
parser.add_option('-s','--suff',  dest='suff' , help='suffix',     type='string', default="_mask")

try:
    opts,args = parser.parse_args(sys.argv[1:])
except:
    print("Error ... check usage with cp_astro_kwds.py -h ")
    sys.exit(1)
    
root = opts.image.strip().split('.fits')[0]
print(" >> Fix astro kwds of {:}".format(root+opts.suff+".fits"))

image = pyfits.open(root+".fits")
targt = pyfits.open(root+opts.suff+".fits", mode="update")

keys  =['CTYPE1','CTYPE2','CRVAL1','CRVAL2','CRPIX1','CRPIX2','CD1_1','CD1_2','CD2_1','CD2_2']
nkeys = len(keys)

for i in range(1,17):
    if opts.suff == '_mask':    # convert mask data to byte if mask
        targt[i].data = targt[i].data.astype('UInt8')

    im = image[i].header
    tg = targt[i].header
    for k in range(nkeys):
        key = keys[k]  
        tg[key] = im[key]

image.close()
targt.close(output_verify='silentfix+ignore')


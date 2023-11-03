#!/opt/intel/intelpython3-2020/intelpython3/bin/python
#-----------------------------------------------------------------------------
# Insert a hi-res (36420x30120) cosmos file into a blank 48k (4096x4096) file
# SYNTAX:
#   Insert.py hi-res_file outfile.fts
# where outfile.fits is the name of the output file
#
# AMo - 16.aug.23
#-----------------------------------------------------------------------------

# general imports
import sys, re, os
import numpy as np
from astropy.io import fits

hrfile = sys.argv[1]
outnam = sys.argv[2]
ref48k = '/n25data1/moneti/Blank_48k.fits'

# copy blank reference 48k file to given name
com = "cp {:} {:} ; chmod 644 {:}".format(ref48k, outnam, outnam)  ; print(">>", com)
os.system(com)

px = fits.open(outnam, mode='update')
pl = fits.open(hrfile)

# read naxis and crpix kwds

#nxx = px[0].header['NAXIS1']  ;  nxy = px[0].header['NAXIS2']     # size 48k
cxx = px[0].header['CRPIX1']  ;  cxy = px[0].header['CRPIX2']     # centre 48k

nlx = pl[0].header['NAXIS1']  ;  nly = pl[0].header['NAXIS2']     # size large
clx = pl[0].header['CRPIX1']  ;  cly = pl[0].header['CRPIX2']     # centre large

offsx = np.int(cxx - clx) ; offsy = np.int(cxy - cly)     #;print( offsx, offsy)

print(">> insert")
px[0].data[offsy:offsy+nly, offsx:offsx+nlx] = pl[0].data

print(">> copy keywords")
kw_list = ['MJD-OBS', 'EXPTIME', 'GAIN', 'SATURATE', 'ORIGIN', 'DATE', 'OBJECT']

for kwd in kw_list:
    px[0].header[kwd] = pl[0].header[kwd]

pl.close()
px.close()

print(">> DONE {:}".format(outnam))
#-----------------------------------------------------------------------------

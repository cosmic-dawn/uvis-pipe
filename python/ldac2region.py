#!/opt/intel/intelpython2-2019.4-088/intelpython2/bin/python
#
#-----------------------------------------------------------------------------
# build a region file suitable for ds9 from a sextractor catalog (ldac)
# - MEFs are merged into a single table.
# - uses python 2
# AMo - 11.dec.19
# - Oct.20: modified to take list directly from cmd line
# - ToDo:  convert to python3, different color for flagged stars
#-----------------------------------------------------------------------------

import sys, os
import numpy as np
import astropy.io.fits as pyfits
from saturation_sub import *

##-----------------------------------------------------------------------------

Force = False
Nfiles = len(sys.argv) 

if sys.argv[-1][:3] == 'for': 
    print("     ####  FORCE mode  ####")
    Force   = True
    Nfiles = Nfiles -1

if Nfiles > 1:
    print(">> found {:} files to process".format(len(sys.argv)-1))

fmt_ok='ICRS;circle (%f,%f,0.0006) #color=green, width=2'
fmt_ko='ICRSafter removing ;circle (%f,%f,0.0009) #color=red, width=4'

for n in range(1, Nfiles):
    nam = sys.argv[n]   # ; print nam
    tmp1 = nam.split('.')[0]+".r1"
    tmp2 = nam.split('.')[0]+".r2"
    out  = nam.split('.')[0]+".reg"

    # check if already done ....
    if (os.path.isfile(out) | os.path.isfile('regs/'+out))  & (Force != True):
        print("ATTN: {:} already done ... continue".format(out))
        continue

    cat = pyfits.open(nam)
    try:
        hdu = merge_ldac(cat)
    except:
        print("PROBLEM: can't merge ldac on {:}".format(nam))
        continue

    try:
        ra = hdu[1].data.field('XWIN_WORLD')
    except:
        ra = hdu[1].data.field('X_WORLD')
    
    try:
        de = hdu[1].data.field('YWIN_WORLD')
    except:
        de = hdu[1].data.field('Y_WORLD')

    flag = hdu[1].data.field('FLAGS')
    
    loc = np.where(flag < 4)
    np.savetxt(tmp1, np.transpose((ra[loc],de[loc])), delimiter=" ", fmt=fmt_ok)
    loc = np.where(flag >= 4)
    np.savetxt(tmp2, np.transpose((ra[loc],de[loc])), delimiter=" ", fmt=fmt_ko)
    os.system("cat {:} {:} > {:} ".format(tmp1, tmp2, out))
    
#    if len(sys.argv)-1 > 1:
    print(">> Built {:} with {:} entries".format(out,len(de)))
    os.system("rm {:} {:}".format(tmp1, tmp2))
#-----------------------------------------------------------------------------

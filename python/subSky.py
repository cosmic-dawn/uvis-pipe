#!/usr/bin/env python

#-----------------------------------------------------------------------------
'''
New sky subtraction: subtracts the sky previously built, then compute (with 
SExtractor) and subtract the large-scale background variation, and finally
destripe along Y, first, and X

Inputs
Outputs
'''
#-----------------------------------------------------------------------------

import sys, re, os
import math
import numpy as np
import numpy.ma as ma
import astropy.io.fits as pyfits
from subsky_sub import cp_skykeys
from optparse import OptionParser
#from time import ctime
#import datetime

parser = OptionParser()

# Input 
parser.add_option('-l', '--list', dest='flist', help='List of images', type='string', default="")
#parser.add_option('-N', '--n-exten', dest='n_ext', help='Number of extentions (def=look in the first image)', type='int', default="0")

# Suffixes
parser.add_option('--inmask-suffix',    dest='inmask_suf',    help='input weight suffix',  type='string', default="_mask.fits")
parser.add_option('--outname-suffix',   dest='outname_suf'  , help='output name suffix',   type='string', default="_clean")

# Conf directory path
parser.add_option('--config-path', dest='cpath', help='path for configuration files', type='string', default="")
parser.add_option('--script-path', dest='spath', help='path for script files', type='string', default="")

# Other
parser.add_option('-v', '--verbose', dest='verbose', help='Verbose ...', action='store_true', default=False)
#parser.add_option('-T', '--n-thread', dest='nproc', help='Number of threads', type='int', default="1")
parser.add_option('-D', '--dry',   dest='dry', help='Dry mode; list what is to be done', action='store_true', default=False)


print "#---------------------------------------------------------------------"
print "#  Begin run of subSky.py"

# Parse command line
try:
    opts, args = parser.parse_args(sys.argv[1:])
except:
    print "Error ... check usage with subsky.py -h "
    sys.exit(1)

print "#---------------------------------------------------------------------"

# Read the list of images 

try:
    file = open(opts.flist, 'r')
except:
    print "Error in opening list " + opts.flist + ' ... quitting \n'
    sys.exit(0)

n_ext=16
exts = range(1, n_ext+1)

#-----------------------------------------------------------------------------
bsize = 64     # ==> back_size
bfilt = 3      # ==> backfilter_size
print "# SExtractor params: bs=%i, bf=%i"%(bsize,bfilt)


lines = file.readlines()
file.close()

print "# list of images contains %i files"%len(lines)
print "#-------------------------------------------------------------"

for line in lines:
    xx = line.split()[0]
    print "# Begin working on %s ... "%xx
    root = xx.split('.fits')[0]
    ima = xx
    sky = root + '_sky.fits'          # sky to subtract
    msk = root + '_mask.fits'         # input mask used in cleaning
    sub = root + '_sub.fits'          # output sky-subtracted image
    cln = root + '_bgcln.fits'        # output destriped images
    des = root + '_clean.fits'        # output final cleaned image

    os.system('cp ' + ima +' '+ sub)
    os.system('cp ' + ima +' '+ des)

    psky = pyfits.open(sky)
    pmsk = pyfits.open(msk)
    psub = pyfits.open(sub, mode='update')     
    
    #-----------------------------------------------------------------------------
    # 1. subtract the sky and a constant sky offset    
    #-----------------------------------------------------------------------------
    for ext in exts:
        psub[ext].data -= psky[ext].data
        mask = pmsk[ext].data.astype("f4")
        data = psub[ext].data.astype("f4")
        marr = ma.array(data, mask=(1-mask))
        psub[ext].data -= marr.mean()
        
    psky.close()
    psub.close()  
    print " - sky-subtracted image is %s "%(sub)
    # copy sky kwds from _sky images to _sub
    print " - copy the _sky keywords in the _sub image"
    cp_skykeys(sky, sub)

    #-----------------------------------------------------------------------------
    # 3. rm large-scale background variations (SExtractor)
    #-----------------------------------------------------------------------------

    chkims = " -CHECKIMAGE_TYPE -BACKGROUND  -CHECKIMAGE_NAME "+cln
    bksize = " -BACK_SIZE %i  -BACK_FILTERSIZE %i "%(bsize, bfilt)
    verb = " -CATALOG_TYPE NONE  -INTERP_TYPE NONE  -VERBOSE_TYPE QUIET"
    args = " -c bgsub.conf " +chkims+bksize+ "  -WEIGHT_IMAGE "+msk
    pars = " -PARAMETERS_NAME bgsub.param  -FILTER_NAME gauss_3.0_7x7.conv  -WRITE_XML Y -XML_NAME bgsub.xml "
    command = "sex "+ sub + args + pars + verb
    print command
    os.system(command)

    print " - cleaned image is %s; copy sky kwds "%(cln)
    cp_skykeys(sky, cln)

#    #-----------------------------------------------------------------------------
#    # add the subtracted background value in the sub image
#    #-----------------------------------------------------------------------------
#    pcln = pyfits.open(cln, mode='update')  # write kwds to this
#
#    for ext in exts:
#        pout[ext].header["BACKLVL"] = (back[ext-1], "Mean value of bkg subtracted")
#    pout[0].header['history'] = " Residual bgd removed with back_size %i, back_filtersize %i"%(bsize,bfilt)
#
#    pout.close()
#    print "   bgd values:" + ",".join([" %0.1f"%x for x in back])

    #-----------------------------------------------------------------------------
    # 2. destripe
    #-----------------------------------------------------------------------------
    os.system('cp ' + cln +' '+ des)
    pdes = pyfits.open(des, mode='update')     

    back=[]
    for ext in exts:
        mask = pmsk[ext].data.astype("f4")
        data = pdes[ext].data
        # along Y
        marr = ma.array(data, mask=(1-mask))
        mmx  = ma.median(marr, axis=0).astype("f4")
        data -= mmx
        back.append(mmx.mean())

        # along X
        marr = ma.array(data, mask=(1-mask))
        mmy  = ma.median(marr, axis=1)
        mmy  = mmy.reshape(data.shape[0],1).astype("f4")
        data -= mmy
        #print " >> DEBUG: ext %-2i, mean mmx,mmy = %0.3f, %0.3f"%(ext, mmx.mean(), mmy.mean())

    pdes.close()  
    pmsk.close()
    print " - destriped image is %s"%(des)
    cp_skykeys(sky, des)
    os.remove(cln)
    os.remove(sub)


    print "#-----------------------------------------------"

#-----------------------------------------------------------------------------


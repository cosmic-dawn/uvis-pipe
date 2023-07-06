#!/opt/intel/intelpython2-2019.4-088/intelpython2/bin/python
#-----------------------------------------------------------------------------
'''
New sky subtraction: subtracts the sky previously built, then compute (with 
SExtractor) and subtract the large-scale background variation, and finally
destripe along Y, first, and X.  Main input is a list of files to process

Inputs:
- input file      root.fits
- its sky file    root_alt.fits for the alternate sky)
                  NB: sky file has sky shape but not average level (i.e. average = 0.0)
                  NB: sky file is NaN where no sky could be computed
- its mask file   root_mask.fits
- its weight      root_weight.fits
Outputs
- clean file      root_clean.fits

For DR6: 
- initial sub file is deleted
- cln points to the final cleaned image, removed intermediate bgdcln file
  to save disk space
- cln file is moved immediately to the filter's images/ dir also to save space
'''
#-----------------------------------------------------------------------------

import sys, re, os
import math
import numpy as np
import numpy.ma as ma
import astropy.io.fits as pyfits
import time
from subsky_sub import cp_skykeys
from optparse import OptionParser

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
#parser.add_option('-D', '--dry',   dest='dry', help='Dry mode; list what is to be done', action='store_true', default=False)


print "#---------------------------------------------------------------------"
print "###  Begin run of subSky.py"

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
    print "### Begin working on %s ... "%xx
    tini = time.time()
    root = xx.split('.fits')[0]
    ima = xx
    sky = root + '_alt.fits'          # sky to subtract
    cnt = root + '_cnt.fits'          # its counts
    msk = root + '_mask.fits'         # input mask used in cleaning
    sub = root + '_sub.fits'          # output sky-subtracted image (SExtractor check image)
    cln = root + '_cln.fits'          # clean image

    print("- 1. subtract {:} from {:} ".format(sky,xx))
    com = 'cp ' + ima +' '+ sub + '; chmod 644 ' + sub 
    os.system(com)
    cp_skykeys(sky, sub)

    psky = pyfits.open(sky)
    pcnt = pyfits.open(cnt)
    pmsk = pyfits.open(msk)
    psub = pyfits.open(sub, mode='update')     

    #-----------------------------------------------------------------------------
    ## 1. subtract the sky and a constant sky offset: ==> sub = _sub.fits
    #-----------------------------------------------------------------------------
    for ext in exts:
        odata  = psub[ext].data    ;  sdata  = psky[ext].data
        olevel = np.median(odata)  # ;  slevel = np.nanmedian(sdata)
        odata -= (sdata + olevel)   # remove sky shape AND mean sky level

    # add some history to primary header:
    psub[0].header['history'] = "# Subtracted local sky %s "%sky
    filt = psub[0].header['FILTER']
    if (filt == 'Ks'): filt = 'K'
    if (filt == 'NB118'): filt = 'N'

    psky.close()
#   print "  - output sky-subtracted image is %s "%(sub)

    #-----------------------------------------------------------------------------
    # 2. rm large-scale background variations (SExtractor): ==> cln = _bgcln.fits
    #-----------------------------------------------------------------------------
    # input files: sub,mask; output is cln
    
    print("- 2. compute and remove large scale background")  # (SExtractor)" ==> {:}".format(cln))
    chkims = " -CHECKIMAGE_TYPE -BACKGROUND  -CHECKIMAGE_NAME "+cln
    bksize = " -BACK_SIZE %i  -BACK_FILTERSIZE %i "%(bsize, bfilt)
    verb = " -CATALOG_TYPE NONE  -INTERP_TYPE NONE  -VERBOSE_TYPE QUIET"
    args = " -c bgsub.conf " +chkims+bksize+ "  -WEIGHT_IMAGE "+msk
    pars = " -PARAMETERS_NAME bgsub.param  -FILTER_NAME gauss_3.0_7x7.conv  -WRITE_XML N"
    command = "sex "+ sub + args + pars + verb   #;         print command
    os.system(command)
    
    # add some history to primary header:
    pcln = pyfits.open(cln, mode="update")
    pcln[0].header = psub[0].header    # copy header of input image
    pcln[0].header['history'] = "# Removed large-scale background variations "
    pcln.close()
#    print "  - output image with large-scale bgd removed is %s"%(cln)      
        
    #-----------------------------------------------------------------------------
    # 3. destripe along Y, then X: in place on cln = _clean file
    #-----------------------------------------------------------------------------
    pcln = pyfits.open(cln, mode='update')     
    print("- 3. Destripe along Y, then X ")  #==> in place on {:}".format(cln))

    back=[]
    for ext in exts:
        mask = pmsk[ext].data.astype("f4")
        data = pcln[ext].data
        count = pcnt[ext].data
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

        # make NaNs the masked pixels
        data[count == 0] = np.nan
#        data[mask == 0] = np.nan
    
    # add some history to primary header:
    pcln[0].header['history'] = "# Destriped along Y, then X "
    
    pcln.close()  
    pmsk.close()
    print "#---------------------------------------------------------------------"
    print("##  DONE {:}; exec time: {:0.2f} min".format(cln, (time.time() - tini)/60))
    print "#---------------------------------------------------------------------"

#    os.system('mv '+ cln + ' /n08data/UltraVista/DR6/'+filt+'/images/' )
#    print "  - %s moved to -/DR6/%s/images "%(cln,filt)
#    os.system('rm '+ sub )
#    print "  - deleted %s  "%(sub, cln)

print "###  Finished run of subSky.py"
print "#---------------------------------------------------------------------"

sys.exit(0)


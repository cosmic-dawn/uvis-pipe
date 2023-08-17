#!/opt/intel/intelpython2-2019.4-088/intelpython2/bin/python
#-----------------------------------------------------------------------------
'''
New sky subtraction: subtracts the sky previously built, then compute (with 
SExtractor) and subtract the large-scale background variation, and finally
destripe along Y, first, and X.  Main input is a list of files to process

For DR6: 
- initial sub file is deleted
- cln points to the final cleaned image, removed intermediate bgdcln file
  to save disk space
- cln file is moved immediately to the filter's images/ dir also to save space

13.aug.23: 
-  Updated to use the CASU files as inputs in place of the "withSky" files;
  i.e. add the CASU sky here in order to save disk space (no need to keep
  the withSky files)
  NB. the CASU skies are not just the shape, but include a sky level.  
  NB. the CASU files have had the sky shape removed, but contain a sky level
      that is given in the SKYLEVEL keywork (one value per chip)

Inputs:
- input file      root.fits          # the CASU image file:
  The associated CASU sky filename and bpm filename are written in
  . kwd SKYSUB (in each extension, always the same up the the chip number)
  . kwd IMRED_MK (in the prinary extension)
- its sky file    root_alt.fits      # the alternate sky built by the pipeline
  . NB: this file contains the sky shape only; the median (?) level is removed
  . NB: the pixels where no sky could be computed (counts = 0) are set to NaN
- its weight      root_weight.fits   # the weight file
Outputs
- clean file      root_cln.fits      # to distinguish it from the _clean files 
                                     # produced with the "traditional" sky
'''
#-----------------------------------------------------------------------------

import sys, re, os
import math
import numpy as np
import numpy.ma as ma
import astropy.io.fits as fits
import time
from subsky_sub import cp_skykeys

#-----------------------------------------------------------------------------
# Read the list of images 
#-----------------------------------------------------------------------------

flist = sys.argv[1]

try:
    file = open(flist, 'r')
except:
    print "Error in opening list " + flist + ' ... quitting \n'
    sys.exit(0)

lines = file.readlines()
file.close()

print "#---------------------------------------------------------------------"
print "# ##  Begin run of subSky.py; input list contains %i entries"%len(lines)
print "#-------------------------------------------------------------"

#-----------------------------------------------------------------------------
# Parameters:
#-----------------------------------------------------------------------------
calDir = os.environ['WRK'] + '/calib/'
imdir  = os.environ['WRK'] + '/images/'

oriDir = imdir + "origs/"
altDir = imdir + "mkAlt/"
mskDir = imdir + "Masks/"

# might as well wet these once and for all
n_ext=16
exts = range(1, n_ext+1)

bsize = 256     # ==> back_size
bfilt = 3       # ==> backfilter_size

verbose = False
if verbose == True: print oriDir, calDir

#-----------------------------------------------------------------------------
# Begin work on individual files:
#-----------------------------------------------------------------------------
for line in lines:
    tini = time.time()                # start time 

    ima = line.split()[0]             # name of the CASU image file, in origs/
    print "### Begin working on %s ... "%ima
    root = ima.split('.fits')[0]

    #-----------------------------------------------------------------------------
    # Set up the inputs and ancillary data
    #-----------------------------------------------------------------------------

    sky = root + '_alt.fits'          # name of file with the "new" sky to subtract 
    cnt = root + '_cnt.fits'          # name of gile with its counts 
    msk = root + '_mask.fits'         # input object mask used to select sky pixels
    sub = root + '_sub.fits'          # name for output sky-subtracted image (SExtractor check image - temporary)
    cln = root + '_cln.fits'          # name for clean (_cln) image

    ##### May not want to di this all the time ######
    # check if _cln file exists, if so nothing to do
    qq = os.path.isfile(imdir + 'cleaned/' + cln)
    if qq == True : 
        print("## ATTN: found cleaned/{:} ... skip it and continue".format(cln))
        continue        
                       
    #-----------------------------------------------------------------------------
    # Set up the inputs
    #-----------------------------------------------------------------------------

    print("- 1. subtract {:} from {:} ".format(sky,ima))
    com = 'cp '+oriDir + ima +' '+ sub + '; chmod 644 ' + sub 
    if verbose == True: print "# DEBUG:", com
    os.system(com)
    cp_skykeys(altDir + sky, sub)

    pmsk = fits.open(mskDir + msk)      
    psub = fits.open(sub, mode='update')     

    # get the name of the CASU sky to add back in
    casu_sky = psub[4].header["SKYSUB"]
    if verbose == True: print "# DEBUG: SKYSUB kwd:", casu_sky
    casu_sky = casu_sky[10:].split('[')[0]+'s'        # name of casu sky to add
    if verbose == True: print "# DEBUG: CASU sky file:", casu_sky
    print "-    CASU sky file:", casu_sky
    pcsky = fits.open(calDir + casu_sky)

    ppsky = fits.open(altDir + sky)             # new (pipeline) sky
    ppcnt = fits.open(altDir + cnt)             # its counts file

    #-----------------------------------------------------------------------------
    ## 1. add back casu sky, subtract the new sky, remove constant sky offsets
    #-----------------------------------------------------------------------------
    for ext in exts:
        idata  = psub[ext].data 
        ilevel = psub[ext].header["SKYLEVEL"]
        cdata  = pcsky[ext].data   
        # sometimes one, sometimes the other, sometines both ... va savoir!
        try: 
            clevel = pcsky[ext].header["SKYLEVEL"]
        except:
            clevel = pcsky[ext].header["MEDSKLEV"]

        ndata = ppsky[ext].data   # nominally its mean is 0.0

        # now perform subtraction
        idata = (idata - ilevel) + (cdata - clevel) - ndata

        # check residual background level:
        xxx = idata * pmsk[ext].data
        sel = np.where((idata != 0) & np.isfinite(idata))
        resBgd = np.mean(xxx[sel])        # residual background
        stdBgd = np.std(xxx[sel])         # its st.dev.

        print "  >> ext %2i: res. bgd, st.dev:  %6.2f, %6.2f"%( ext, resBgd, stdBgd)
        idata[idata < -5. * stdBgd] = 0.       # remove large negative values
        psub[ext].data = idata.astype("float32")

    # add some history to primary header:
    psub[0].header['history'] = "# Subtracted local sky %s "%sky

    ppsky.close()
    psub.close()

    if verbose == True: print "#DEBUG  - output sky-subtracted image is %s "%(sub)

    #-----------------------------------------------------------------------------
    # 2. rm large-scale background variations (SExtractor): ==> cln = _cln.fits
    #    input files: _sub, _mask; output is _cln
    #-----------------------------------------------------------------------------
    print("- 2. compute and remove large scale background")  # (SExtractor)" ==> {:}".format(cln))
    print "## INFO:  SExtractor params: bs=%i, bf=%i"%(bsize,bfilt)

    chkims = " -CHECKIMAGE_TYPE -BACKGROUND  -CHECKIMAGE_NAME "+cln
    bksize = " -BACK_SIZE %i  -BACK_FILTERSIZE %i "%(bsize, bfilt)

    verb = " -CATALOG_TYPE NONE  -INTERP_TYPE NONE  -VERBOSE_TYPE QUIET"
    args = " -c bgsub.conf " +chkims+bksize+ "  -WEIGHT_IMAGE "+mskDir+msk
    pars = " -PARAMETERS_NAME bgsub.param  -FILTER_NAME gauss_3.0_7x7.conv  -WRITE_XML N"

    com = "sex "+ sub + args + pars + verb
    if verbose == True: print com  
    os.system(com)
    
    # add some history to primary header:
    pcln = fits.open(cln, mode="update")
    pcln[0].header = psub[0].header    # copy header of input image
    pcln[0].header['history'] = "# Removed large-scale background variations "
    if verbose == True: print "  #DEBUG  - output image with large-scale bgd removed is %s"%(cln)      
    
    # clean up
    os.system("rm "+sub)
    #-----------------------------------------------------------------------------
    # 3. destripe along Y, then X: in place on cln = _clean file
    #-----------------------------------------------------------------------------
    print("- 3. Destripe along Y, then X ")  #==> in place on {:}".format(cln))

    for ext in exts:
        mask  = pmsk[ext].data.astype("f4")
        data  = pcln[ext].data
        # ATTN: SExtractor left "bad" pixels at -1E+30 ... add them to the mask
        mask[data >  1E+5] = 0.
        count = ppcnt[ext].data 

        # along Y
        marr = ma.array(data, mask=(1-mask))
        mmx  = ma.median(marr, axis=0).astype("f4")
        data -= mmx
    
        # along X
        marr = ma.array(data, mask=(1-mask))
        mmy  = ma.median(marr, axis=1)
        mmy  = mmy.reshape(data.shape[0],1).astype("f4")
        data -= mmy
        print "  >> ext %2i, mean mmx, mmy:  %7.3f, %7.3f"%(ext, mmx.mean(), mmy.mean())

        # make NaNs the masked pixels
        data[count == 0] = np.nan
        data[data == -1E+30] = np.nan
        pcln[ext].data = data

    # add some history to primary header:
    pcln[0].header['history'] = "# Destriped along Y, then X "
    
    pcln.close()  
    pmsk.close()
    print "#---------------------------------------------------------------------"
    print("##  DONE {:}; exec time: {:0.2f} min".format(cln, (time.time() - tini)/60))
    print "#---------------------------------------------------------------------"

print "###  Finished run of subSky.py"
print "#---------------------------------------------------------------------"

sys.exit(0)

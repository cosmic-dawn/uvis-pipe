#!/opt/intel/intelpython2-2019.4-088/intelpython2/bin/python
#-----------------------------------------------------------------------------
# Double pass sky subtraction - build sky image
#
# Method:  for each file:
# - apply the object mask (masked regions set to NaN)
# - determine median sky level, then either
#   . subtract the median sky (method subtract) or
#   . normalise the sky to that of the source file, and subtract the median
#     level of the source file
#   NB. goal is to produce the sky shape w/o its median value
# - place that value into the data cube with one place for each sky
# - stack median filter the cube to produce the sky frame
# 
#-----------------------------------------------------------------------------
# Jan.18, AMo: 
#   fixed to handle images with insufficient nearby frames to build sky
# Aug.18: AMo:
#   adapated from subSky.py, removing section that does the subtraction
# Apr.23, AMo:  
#   removed "traditional" sky building method (using swarp/missfits), 
#   replaced by a pure python method decribed above.
# Aug.23, AMo:
#   added check of unequal jitter_I kwd
#-----------------------------------------------------------------------------

import math
from optparse import OptionParser
import sys, re, os
import astropy.io.fits as pyfits
import numpy as np
from subsky_sub import *
import time
import datetime

parser = OptionParser()

# Input 
parser.add_option('-l', '--list', dest='flist', help='List of images from which to subtract sky', type='string', default="")
parser.add_option('-S', '--sublist', dest='sublist', help='List of sky images to use', type='string', default="")
parser.add_option('-N', '--n-exten', dest='n_ext', help='Number of extentions (def=look in the first image)', type='int', default="0")

# Suffixes
parser.add_option('--inmask-suffix',    dest='inmask_suf',    help='input weight suffix',  type='string', default="_mask.fits")
parser.add_option('--outweight-suffix', dest='outweight_suf', help='output weight suffix', type='string', default="_mask.fits")
#parser.add_option('--outname-suffix',   dest='outname_suf'  , help='output name suffix',   type='string', default="_sub")

# Skysub method
parser.add_option('-n', '--n-images', dest='numim', help='Number of images to build the sky (def: 20)',  type='int', default="20")
parser.add_option('-s', '--n-skies',  dest='nskies', help='Min num of images to build the sky (def: 4)', type='int', default="4")
parser.add_option('-t', '--time', dest='dtime', help='maximum time between source and sky image in mn (def: 30)', type='float', default="30.")
parser.add_option('-d', '--dist', dest='dist',  help='maximum dist between source and sky image in arcmin (def: 1000)', type='float', default="1000.")
parser.add_option('--pass2', dest='spass', help='Double pass skysub ?', action='store_true', default=False)

# not used by must leave in foc ompatibilty sith routines in subsky_sub.py
parser.add_option('--n-cubes', dest='numcube', help='Number of cubes to build the sky (def: 5) ', type='int', default="0")
parser.add_option('--nimcube', dest='nimcube', help='Number of images from each cube to use in the sky (def: 0=all) ', type='int', default="0")

# Conf directory path
parser.add_option('--config-path', dest='cpath', help='path for configuration files', type='string', default="")
parser.add_option('--script-path', dest='spath', help='path for script files', type='string', default="")

# Other
parser.add_option('-v', '--verbose', dest='verbose', help='Verbose ...', action='store_true', default=False)
parser.add_option('-T', '--n-thread', dest='nproc', help='Number of threads', type='int', default="1")
parser.add_option('-D', '--dry',   dest='dry', help='Dry mode; list what is to be done', action='store_true', default=False)
parser.add_option('-B', '--debug', dest='debug', help='Debuging mode ..', action='store_true', default=False)

# Log
parser.add_option('--npix',  dest='npix',  help='Compute hit count (def: no)', action='store_true', default=False)
parser.add_option('--log',   dest='flog',  help='Log filename (def: subsky.log)', type='string', default="subsky.log")


print "#---------------------------------------------------------------------"
#method = "subtract"
method = "rescale"
doRMS = False
doVAR = False

print "#####  Begin run of mkAltSky.py  ##### "

fitsext = ".fits"
headext = ".head"

# Parse command line
try:
    options, args = parser.parse_args(sys.argv[1:])
except:
    print "Error ... check usage with subsky.py -h "
    sys.exit(1)

# Get options and put into scalar variables
list = dir(parser.values)
for el in list:
    if el[0] == "_":
        continue
    exec (el + " = options." + el)

options.dtime2 = options.dtime / 60 / 24  # convert to minutes
options.dist2  = options.dist  / 60       # convert to min of arc

if options.dry:
    print "====================   Dry mode: check what to do   ===================== "    

# If no sublist ... use list instead
if options.sublist == "":
    options.sublist = options.flist

# Read the list of images + check for weights
imlist = []
masklist = []
try:
    file = open(options.flist, 'r')
except:
    print "Error in opening list " + options.flist + ' ... quitting \n'
    sys.exit(0)

lines = file.readlines()
file.close()
for line in lines:
    im = line.split()[0]
    mask = im.split('.fits')[0] + inmask_suf
    imlist.append(im)
    masklist.append(im.split('.fits')[0] + inmask_suf)

if len(imlist) == 0:
    print 'ERROR: input list empty or contains no valid images ...'
    sys.exit(1)

# Get the list of subimages
sublist = []
try:
    file = open(options.sublist, 'r')
except:
    print "Error in opening sublist " + options.sublist + ' ... quitting \n'
    sys.exit(0)

lines = file.readlines()
file.close()
for line in lines:
    im = line.split()[0]
    sublist.append(im)

n_ext=16
exts = range(1, n_ext+1)

print " - INFO: input source list contains %i files"%len(imlist)
print " - INFO: input sky list contains %i files"%len(sublist)
print " - INFO: method is .....", method
print " - INFO: doRMS .........", doRMS
print " - INFO: doVAR .........", doVAR

print "#-----------------------------------------------------------------------------"
print "#### 1. Check images for number of available skies ..."
print "#-----------------------------------------------------------------------------"

newimlist=[]   

# Read some keywords
keys = ['FILTER', 'MJDATE', 'RA_DEG', 'DEC_DEG', 'OBJECT', 'EXPTIME', 'SATURATE', 'FILENAME', 'SKYLEVEL', 'JITTER_I']
data_sublist = read_header(sublist, keys)
data_imlist  = read_header(imlist, keys)
bertin_par   = bertin_param()

# loop on the images of the list
for (im, ind) in zip(imlist, range(len(imlist))):

    imroot = im.split(fitsext)[0]
    skylist = get_skylist_dr6(im, ind, sublist, data_imlist, data_sublist, options)
    # check that sky list contains at least nskymin images
    if len(skylist) < options.nskies:
        print " CHECK: %s: skip - only %i images available for sky, %i required. "%(im, len(skylist), options.nskies)
        continue

    print(">> Source file {:}: found {:-2n} files to build sky: ".format(im, len(skylist)))
    newimlist.append(im)

if (len(newimlist) == 0):
    print " ##"
    print " ##  AAArgh ....  No images available with suffient skies ... quitting"
    print " ##"
    sys.exit(1)

if options.dry:
    print ""
    print "Subsky module arameters: "
    print options
    print ""
    print " =====================   Finished dry mode check exiting   ===================== "                      


print "#-----------------------------------------------------------------------------"
print "#### 2. Loop on images to build individual skies ...   "
print "#-----------------------------------------------------------------------------"

data_imlist = read_header(newimlist, keys)    # data of list with enough skies only
for (im, ind) in zip(newimlist, range(len(newimlist))):

    tini = time.time()
    pima = pyfits.open(im) 

    imroot = im.split(fitsext)[0]
    imhead = imroot + headext
    skylist = get_skylist_dr6(im, ind, sublist, data_imlist, data_sublist, options)
    skylist = sorted(skylist)

    print(" -- Begin working on {:} ... found {:} images to build sky:".format(im, len(skylist)))
    altsky = im.split('.')[0] + '_alt.fits'   # name of alternative sky
    mask   = im.split('.')[0] + '_mask.fits'  # name of input mask
    count  = im.split('.')[0] + '_cnt.fits'   # name of counts map

    print " -- initialize altsky map ", altsky
    cmd="cp %s %s ; chmod 644 %s "%(im,altsky,altsky)  ; os.system(cmd)
    palt = pyfits.open(altsky, mode="update")

    if doRMS == True:
        rms  = im.split('.')[0] + '_rms.fits'   # name of std dev map
        print " -- initialize rms map    ", rms
        cmd="cp %s %s ; chmod 644 %s "%(im,rms,rms)    ; os.system(cmd)
        prms = pyfits.open(rms, mode="update")
    if doVAR == True:
        var  = im.split('.')[0] + '_var.fits'   # name of variance map
        print " -- initialize rms map    ", var
        cmd="cp %s %s ; chmod 644 %s "%(im,var,var)    ; os.system(cmd)
        pvar = pyfits.open(var, mode="update")

    print " -- initialize counts map ", count
    cmd="cp %s %s ; chmod 644 %s "%(mask,count,count)  ; os.system(cmd)
    pcnt = pyfits.open(count , mode="update")

    print " -- open mask file        ", mask
    pmsk = pyfits.open(mask)

    print("#---------  Begin loop on the extensions ---------")
    # ---------  Loop on the extensions ---------
    for ext in exts:
        sext = str(ext)

        # median sky level of source file
        sky0 = pima[ext].data                       # the source file
        sky0[pmsk[ext].data == 0] = np.nan
        sky0 = np.nanmedian(sky0)                   # determine the median
        print(" - ext {:-2n}: source sky level: {:0.0f} ".format(ext, sky0))
        if options.dry:
            print " >>>> Dry mode: swarp params to build 1st sky ext <<<<< "
            print args 
            print " =====================   Finished dry mode check exiting   ===================== "   
            sys.exit(0)

#        print("## Build alt-sky for {:}; sky level: {:0.0f} ".format(im, sky0))
        cube = np.zeros((2048,2048,len(skylist)) )
        xx0 = 624; yy0 = 1028
        for n in range(len(skylist)):
            rr = skylist[n].split('.')[0]
            ss = pyfits.open(skylist[n])             # sky file
            mm = pyfits.open(rr + '_mask.fits')      # sky mask file

            data = ss[ext].data                      # the sky data
            data[mm[ext].data == 0] = np.nan         # apply the mask: masked regions set to NaN
            medi = np.nanmedian(data.flatten())      # internal estimate of sky level

            # sky data cube 
            if (method == 'subtract'):
                cube[:,:,n] = data - medi      # simple sky subtraction
                print("  . sky # {:-2n}: {:}, level {:0.0f} ".format(n+1,skylist[n], medi))
            else:
                gain = sky0/medi                # gain factor
                cube[:,:,n] = gain*data - sky0  # ..to normalise sky level to that of source file
                print("  . sky # {:-2n}: {:}, level {:0.0f} ==> gain {:0.2f}".format( n+1,skylist[n], medi, gain))
            # counts map: coadd the mask frames
            if n == 0:
                counts = mm[ext].data
            else:
                counts += mm[ext].data
            mm.close(); ss.close()

        ## Build median  ... ATTN: some pixels could be NaN everywhere (gives python warning)
        medi = np.float32(np.nanmedian(cube, axis=2))     # stack median of cube
        if doRMS == True:
            srms = np.nanstd(cube, axis=2)        # stack rms of cube
        if doVAR == True:
            svar = np.nanvar(cube, axis=2)        # stack var of cube
        mmedi = np.nanmedian(medi)            # median value of medi (should be near zero)
        nloc = len(medi[np.isnan(medi)])      # Number of NaNs in sky
        nzz = len(counts[counts == 0])        # number of zeros in counts

        print(" ==> on ext {:-2n}: {:-2n} skies: median: {:0.2f}; masked {:0n} or {:0.2f}%".format(ext, len(skylist), mmedi, nloc, nloc/2048/20.48))

        palt[ext].data = medi                     # fill in sky map
        ## if all NaNs, then count=0 (at least in this version of python)
        pcnt[ext].data = counts                   # fill count map
        if doRMS == True:
            prms[ext].data = srms.astype("float32")   # fill rms map, maybe
        if doVAR == True:
            pvar[ext].data = svar.astype("float32")   # fill var map, maybe

    
    print '#------  Finished loop over extensions; close files  ------------------'
    palt.close() ; print(" >> Built alternate sky image: {:}".format(altsky))
    pcnt.close() ; print(" >> Built alt sky counts map:  {:}".format(count))
    if doRMS == True:
        prms.close() ; print(" >> Built std deviation map:   {:}".format(rms))
    if doVAR == True:
        pvar.close() ; print(" >> Built variance map:        {:}".format(var))
    pmsk.close() 

    # Finish alt sky and count map
    print "# Add kwd with names of images used for building sky"
    now = datetime.datetime.now()
    hist = '%s, on %s'%(now.strftime("%Y-%m-%d %H:%M"), os.getenv('PWD'))

    with pyfits.open(imroot + '_alt.fits', mode='update') as psub:
        hd1 = psub[0].header
        for (imm, index) in zip(skylist, range(len(skylist))):
            hd1['SKYIM' + str(index)] = imm
        hd1['history'] = '# mkSky finished on %s on Candide node %s'%(now.strftime("%Y-%m-%d %H:%M"),os.uname()[1])
        hd1['history'] = '# List of sky files used: %s '%flist

    print("#-----------------------------------------------------------------------------")
    print("##  DONE {:} with {:-2n} skies;  exec time: {:0.2f} min".format(altsky, len(skylist), (time.time() - tini)/60))
    print("#-----------------------------------------------------------------------------")

sys.exit()
#-----------------------------------------------------------------------------

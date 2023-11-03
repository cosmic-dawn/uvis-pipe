#!/opt/intel/intelpython2-2019.4-088/intelpython2/bin/python
#-----------------------------------------------------------------------------
# Double pass sky subtraction - build sky image (traditional method using 
# swarp to stack-median filter sky frame)
#
# Jan.18, AMo: 
#   fixed to handle images with insufficient nearby frames to build sky
# Aug.18, AMo:
#   adapated from subSky.py, removing section that does the subtraction
# Apr.23, Amo:
#   introduced timing info
# Sep.23, AMo:
#   added check of unequal jitter_I kwd
#-----------------------------------------------------------------------------

import math
from optparse import OptionParser
import sys, re, os
import astropy.io.fits as pyfits
import numpy as np
from subsky_sub import *
from time import ctime
import datetime
import time

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
parser.add_option('-d', '--dist', dest='dist',  help='maximum dist between source and sky image in arcmin (def: 10)', type='float', default="10.")
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
print "# Begin run of mkSky.py"

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

#print "#---------------------------------------------------------------------"

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

print "  - INFO: list of source images contains %i files"%len(imlist)
print "  - INFO: list of  sky   images contains %i files \n"%len(sublist)
#print " >> work on extensions: ", exts

# -------- SKY SUBTRACTION ------

print "# Check images for number of available skies ..."
print "#---------------------------------------------------------------------"

newimlist=[]   

# Read some keywords
keys = ['FILTER', 'MJDATE', 'RA_DEG', 'DEC_DEG', 'OBJECT', 'EXPTIME', 'SATURATE', 'FILENAME', 'SKYLEVEL', 'JITTER_I']
data_sublist = read_header(sublist, keys)
data_imlist = read_header(imlist, keys)
bertin_par = bertin_param()

# loop on the images of the list
for (im, ind) in zip(imlist, range(len(imlist))):

    imroot = im.split(fitsext)[0]
    skylist = get_skylist_dr6(im, ind, sublist, data_imlist, data_sublist, options)

    # check that sky list contains at least nskymin images
    if len(skylist) < options.nskies:
        print " CHECK: %s: skip - only %i images available for sky, %i required. "%(im, len(skylist), options.nskies)
        continue

    print ">> CHECK: %s: found %2i images to build sky"%(im, len(skylist))
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


print "# -------------------------------------------------------------"
print "# Begin actual work on images that have enough nearby skies"
print "# -------------------------------------------------------------"

data_imlist = read_header(newimlist, keys)    # data of list with enough skies only
for (im, ind) in zip(newimlist, range(len(newimlist))):

    tini = time.time()
    print " >> Begin working on %s ... "%im
    imroot = im.split(fitsext)[0]
    imhead = imroot + headext
    skylist = get_skylist_dr6(im, ind, sublist, data_imlist, data_sublist, options)

    print " >> Found %i images to build sky:"%(len(skylist))
    print " >> Copy %s header to %s "%(im, imhead)
    cp_head(im, imhead)

    print " >> and link it to (pseudo) head files of skies .. links are"
    for skyim in skylist:
        os.symlink(imhead, skyim.split(fitsext)[0] + headext) 
        print "   " + skyim.split(fitsext)[0]+headext + " -> " + os.readlink( skyim.split(fitsext)[0] + headext)

    # Loop on the extensions
    for ext in exts:
        sext = str(ext)
        print "#------------------- Begin working on extension %2i -------------------"%(ext)

        # External header: .exp file used by missfits when recombining extensions  
        expout = im.split('.fits')[0] + '_sky.' + str(ext) + '.exp'
        file = open(expout, 'w')
        file.write("EXPTIME =             %8.4f  / Integration time (seconds)\n" % (data_imlist['EXPTIME'][ind]))
        file.write("SATURATE=             %8.4f  / Saturation value (ADU)\n" % (data_imlist['SATURATE'][ind]))
        file.close()

        ext_head = imroot + '_sky.' + sext + headext
        print " >> Copy image header to %s and link it to the head of the sky files"%ext_head
        exkeys = ["XTENSION", "PCOUNT", "GCOUNT"]
        copy_header_MEF(imroot + fitsext, imroot + '_sky.' + sext + headext, ext, exkeys)

        print " >> Build the sky image (swarp) ==> %s_sky.%i.fits"%(imroot,ext) 
        imout = imroot + '_sky.' + sext + fitsext
        args = ' -RESAMPLE Y  -RESAMPLING_TYPE NEAREST  -COMBINE_TYPE MEDIAN  -SUBTRACT_BACK Y  -BACK_SIZE 4096  -COPY_KEYWORDS OBJECT,FILTER  -WEIGHT_SUFFIX '+inmask_suf+'  -IMAGEOUT_NAME '+imout+'  -c swarp.conf  -VERBOSE_TYPE QUIET  -WRITE_XML N '
        other = '-WEIGHTOUT_NAME sky.weight.fits  -WEIGHT_TYPE MAP_WEIGHT' # default param, should not be necessary for method median
        # out weights not used - leave default name (coadd.weight.fits) and delete later

#        if (ext == 1):  # write xml for first ext only, others are all the same
#            args += '-WRITE_XML Y -XML_NAME '+imroot+'_sky.1.xml '
#        else:
#        args += '-WRITE_XML N'

        if options.dry:
            print " >>>> Dry mode: swarp params to build 1st sky ext <<<<< "
            print args    # ex swarppar
            print " =====================   Finished dry mode check exiting   ===================== "   
            sys.exit(0)
        else:
            simlist = ','.join([x+"[%s]"%ext for x in skylist])
            if (ext == 1): 
                print  " % swarp " +simlist+" "+ args
            os.system("swarp " +simlist+" "+ args)
        print " >> Built sky image: %s "%imout

    print '#------------------  Finished loop over extensions  ------------------'
    print '#---------------  Merge extensions into new MEF file  ----------------'

    print "# Join the extentions for %s "%(imroot + '_sky.fits')
    os.system('missfits -c ' + cpath + '/missfits.conf ' + imroot + '_sky  -WRITE_XML N   -OUTFILE_TYPE MULTI -HEADER_SUFFIX none -SAVE_TYPE REPLACE -SPLIT_SUFFIX .%01d.fits')

    print "# Clean up (rm v*.*.exp, coadd.*, head, temp files)"
    os.remove('coadd.weight.fits')
    os.remove(imhead)
    for imm in skylist:
        os.remove(imm.split(fitsext)[0] + headext)
    for e in exts:
        os.remove(imroot+'_sky.'+str(e)+'.exp')
        os.remove(imroot+'_sky.'+str(e)+'.head')

    print "# Add kwd with names of images used for building sky"
    now = datetime.datetime.now()
    hist = '%s, on %s'%(now.strftime("%Y-%m-%d %H:%M"), os.getenv('PWD'))

    with pyfits.open(imroot + '_sky.fits', mode='update') as psub:
        hd1 = psub[0].header
        for (imm, index) in zip(skylist, range(len(skylist))):
            hd1['SKYIM' + str(index)] = imm
        hd1['history'] = ' mkSky finished on %s '%(now.strftime("%Y-%m-%d %H:%M"))
        hd1['history'] = ' On node %s '%os.uname()[1]
        hd1['history'] = ' Files used: %s '%flist


    print("#-----------------------------------------------------------------------------")
    print("##  DONE {:}_sky.fits with {:-2n} skies;  exec time: {:0.2f} min".format(imroot, len(skylist), (time.time() - tini)/60))
    print("#-----------------------------------------------------------------------------")
    print "#---------------  Finished with %s  ----------------"%im 

#-----------------------------------------------------------------------------


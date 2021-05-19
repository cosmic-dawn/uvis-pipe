#---------------------------------------------------------------------
# mkMasks.py
#---------------------------------------------------------------------
''' 
purpose:   
  build masks for sky subtraction
procedure:
  build a mask for each image that identifies the location of sources
  as found in the pass1 stack and of other features found on the image itself (eg.
  satellite trails).  this mask will be used in
  building the skies to subtract
'''
#-----------------------------------------------------------------------------

import os, sys
import argparse
from subsky_sub import *

def get_parser():
    # NB. some seemeingly unnecessary params are neded for compatibility with code in subsky_sub.py

    parser = argparse.ArgumentParser(description='Produce masks for sky subtraction')
    
    parser.add_argument('-l', '--list', dest='flist', help='List of images', type=str, default="")
    parser.add_argument('--inweight-suffix', dest='inweight_suf', help='input weight suffix (def=_zeroes.fits)', type=str, default="_weight.fits")
    parser.add_argument('--outweight-suffix', dest='outweight_suf', help='output weight suffix (def=_mask.fits)', type=str, default="_mask.fits")
    parser.add_argument('-S', '--stack',  dest='stack',  help='Reference stack', type=str, default="")
    parser.add_argument('-W', '--weight', dest='weight', help='Reference stack weight', type=str, default="")
    parser.add_argument('-M', '--mask', dest='mask_file', help='bad pixel mask (def = zeroes.fits)', type=str, default="zeroes.fits")
    parser.add_argument('--threshold', dest='thresh', help='detection threshold in building masks (def = 1)', type=float, default="1.5")
    parser.add_argument('--double_mask', dest='double_mask', help='Mask from stack AND single image', action='store_true', default=True)
    parser.add_argument('-T', '--n-thread', dest='nproc', help='Number of threads', type=int, default="1")
    parser.add_argument('--conf-path', dest='cpath', help='path for configuration files', type=str, default="")
    parser.add_argument('--pass2', dest='spass', help='Double pass skysub?', action='store_true', default=True)

    parser.add_argument('-v', '--verbose_level', dest='verbose_level', help='ERROR,WARNING,INFO,DEBUG', type=str, default='ERROR')
    parser.add_argument('--clean', dest='clean', help='Delete intermediate products', action='store_true', default=False)
    parser.add_argument('--extendedobj', dest='extendedobj', help='Extended Objetcs?', action='store_true', default=False)
    parser.add_argument('--guide-on', dest='guide', help='guide defects detection?', action='store_true', default=False)

    return parser

def main(args):

    print "#-----------------------------------------------------------------------------"
    print "## starting mkMasks.py "
    print "#-----------------------------------------------------------------------------"

    # Build image list
    flist = open(args.flist, 'r')
    lines = flist.readlines()
    flist.close()

    n_ext=16
    # Loop on the images
    for line in lines:
        im = line.split()[0]
        print "## Building mask file for %s"%im

        print "## first build mask from input image"
        # sex to produce image of sources; ww to convert it to a mask
        bpar = bertin_param()
        print "\n >> 1. create_mask_pass1" #(%s, %s, %s)"%(im, bpar, args)
        create_mask_pass1(im, bpar, args)

        #print "## DEBUG:  "
        print "## copy source images and apply to it the external header file"
        #wim = im.split(".fits")[0] + args.inweight_suf
        print "missfits " + im + " -c missfits.conf -SAVE_TYPE NEW -NEW_SUFFIX .temp -VERBOSE_TYPE QUIET "
        os.system( "missfits " + im + " -c missfits.conf -SAVE_TYPE NEW -NEW_SUFFIX .temp -VERBOSE_TYPE QUIET ")
        
        # Project the mask
        maskin = args.stack.split('.fits')[0] + '_obFlag.fits'  # here images is the input stack
        maskout = im.split('.fits')[0] + '.flag.miss.fits'
        #arg_thread = [maskin, maskout, im, n_ext, args]
        
        print "## run project_combine to project the stack mask to the image (swarp)"
        print "## and merge with mask from image"
        project_combine(maskin, maskout, im, n_ext, args)
        
    print "#-----------------------------------------------------------------------------"
    print "##  mkMasks.py Finished"
    print "#-----------------------------------------------------------------------------"

if __name__ == '__main__':

    parser = get_parser()
    args = parser.parse_args()
    # Run the main code
    main(args)

#-----------------------------------------------------------------------------

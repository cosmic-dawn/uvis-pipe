#!/usr/bin/env python
'''
File:     addSky.py
Purpose:  add the casu sky to the images in the list.  The list is actually a table of the format described
          below, which, for each file, gives the names of the support files.  As the original images already
          include some kind of mean sky level, the median level of the sky image, as computed for the unmasked
          pixels, is subtrated.  
          Also, multiply the result image by the mask in order to put the masked pixesl to 0

   infotab is an ascii table of 7 columns containing:
   1. filename (.fits)
   2. its paw (OBJECT kwd)
   3. its filter
   4. its flatfield
   5. its bpm
   6. its stack
   7. its sky
   and which is built at p1 of the pipeline
'''

import argparse
import sys, re, os, glob
import astropy.io.fits as pyfits
#import numpy
import numpy.ma as MA
from logger_lib import setup_logger

def get_parser():
    """
    Build the argument parser
    :return: argparse parser
    """

    parser = argparse.ArgumentParser(description='Add sky back to CASU images')

    parser.add_argument('-o', '--osuff', dest='osuff', help='Suffix for sky added files (default = s.fits)', type=str, default='s.fits')
    parser.add_argument('-t', '--infotab', dest='infotab',  help='name of info table (def: fileinfo.txt)', type=str, default='fileinfo.txt')

    # verbose options
    parser.add_argument('-v', '--verbose_level', dest='verbose_level', help='Verbose level (ERROR,WARNING,INFO,DEBUG)', type=str, default='INFO')
    parser.add_argument('--log', dest='flog', help='Log filename', type=str, default="addSky.log")
    return parser

def main(args):
    """
    Main fonction. Get the arguments from the args dictionary
    :param args: dictionary of arguments (see parser for list and description)
    :return:
    """

    # setup the logger
    logging = setup_logger(args.flog, loglevel=args.verbose_level, file_loglevel="INFO", name='whatsthis')

    # List of images
    logging.debug("Check the info table ... ")
    if not os.path.isfile(args.infotab):
        logging.error("Info table not found: %s" % args.infotab)
        sys.exit(1)
        
    # Extract lists of skies and bpms from fileinfo table
    lines = os.popen("cat " + args.infotab).readlines()
    for line in lines:
        if line.strip()[0] == "#":
            continue
        ima = line.strip().split()[0]
        bpm = line.strip().split()[4]
        sky = line.strip().split()[6]
#        print ima,bpm,sky
        out = ima.replace(".fits", args.osuff)
        logging.info("on %s with %s and %s"%(ima, bpm, sky))

        pyima = pyfits.open(ima)
        pybpm = pyfits.open(bpm)
        pysky = pyfits.open(sky)

        n_ext = len(pyima)
        for iext in range(n_ext - 1):
            # Get the median of the sky (with bpm masking)
            masked_sky = MA.array(pysky[iext + 1].data, mask=(1 - pybpm[iext + 1].data))
            med2 = MA.median(masked_sky)
    
            pyima[iext + 1].data = pyima[iext + 1].data.astype('float32') + pysky[iext + 1].data - med2
            # finally multiply by the mask to set masked pixels to nought
            pyima[iext + 1].data = pyima[iext + 1].data * pybpm[iext + 1].data
        pyima.writeto(out, overwrite=True)
    
        pyima.close()
        pysky.close()
        pybpm.close()


# Command line running
if __name__ == '__main__':

    # Get the argument parser and parse the command line
    parser = get_parser()
    args = parser.parse_args()

    # Run the main code
    main(args)

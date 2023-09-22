#!/usr/bin/env python
'''
File:     addSky.py
Purpose:  add the casu sky to the images in the list.  The list is actually a table of the format described
          below, which, for each file, gives the names of the support files.  As the original images already
          include some kind of mean sky level, the median level of the sky image, as computed for the unmasked
          pixels, is subtrated.  
          Also, multiply the result image by the bad pixel mask in order to put the masked pixels to 0

   infotab is an ascii table of 7 columns containing:
   1. filename (.fits)
   2. its paw (OBJECT kwd)
   3. its filter
   4. its flatfield
   5. its bpm
   6. its stack
   7. its sky
   and which is built at p1 of the pipeline

Major update / AMo - sep 23: after fixing SKYSUB kwd, the FileInfo files are no longer correct.  Rather
than fixing them, it's deemed safer to modifiy this code to read the bpm and sky files from the image
keywords. Nevertheless, the list of images is still read from the FileInfo table in order to not have
to modify other pipeline scripts.
- bpm read from MIRED_MK in ext 0
- sky read form SUBSKY in ext 4 (same for all exts.)
- mean sky level read from SKYLEVEL or MEDSKLEV kwds (as in subAltSky.py), not recomputed
'''

import argparse
import sys, re, os, glob
import astropy.io.fits as pyfits
import numpy as np
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
#_#        bpm = line.strip().split()[4]
#_#        sky = line.strip().split()[6]
        out = ima.replace(".fits", args.osuff)

        pyima = pyfits.open(ima)
        bpm = pyima[0].header["IMRED_MK"]
        sky = pyima[4].header["SKYSUB"]   # same for all extensions
        sky = sky[10:].split('[')[0]+'s'
        logging.info("on %s with %s and %s"%(ima, bpm, sky))
        # open the files
        pybpm = pyfits.open(bpm)
        pysky = pyfits.open(sky)

        n_ext = len(pyima)
        for iext in range(n_ext - 1):
#_#            # Get the median of the sky (with bpm masking) ... removed; not worth it; takes long time
#_#            masked_sky = MA.array(pysky[iext + 1].data, mask=(1 - pybpm[iext + 1].data))
#_#            med2 = MA.median(masked_sky) ; print(iext+1, med2)
#            print(iext+1, np.mean(pysky[iext+1].data))
#            print(iext+1, np.median(pysky[iext+1].data))
            # Get the median sky level from casu kwd
            # sometimes one, sometimes the other, sometines both ... va savoir!
            try: 
                med2 = pysky[iext+1].header["SKYLEVEL"]
            except:
                med2 = pysky[iext+1].header["MEDSKLEV"]
    
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


# -------------------------------------------------------
# Remove PV kwds from image headers if there (applies to VISTA flatfields)
# input : l, list     : list of flats
#       : v, verbose_level : ERROR,WARNING,INFO,DEBUG
#       : -, log      : logfile
# --------------------------------------------------------

import os, sys
import argparse
import astropy.io.fits as pyfits
from logger_lib import setup_logger


def get_parser():
    parser = argparse.ArgumentParser(description='Remove PV kwds from the headers (inplace).')

    parser.add_argument('-l', '--list', dest='inlist', help='Input list of images', type=str, default="")
    # verbose options
    parser.add_argument('-v', '--verbosity', dest='verbose_level', help='Verbosity (ERROR,WARNING,INFO,DEBUG)', type=str, default='INFO')
    parser.add_argument('--log', dest='flog', help='Log filename', type=str, default="clean_flats.log")
    return parser


def main(args):

    # Setup the logger
    logging = setup_logger(args.flog, loglevel=args.verbose_level, file_loglevel="ERROR", name="clean_flats.py")

    try:
        file_list = open(args.inlist)
    except:
        logging.error("Invalid list : %s" % args.inlist)
        sys.exit(1)

    for input_file in file_list:
        entry = input_file.strip().split()[0]
        if entry == "":
            continue
        logging.info("Cleaning header of %s." % entry)

        if not os.path.exists(entry):
            logging.error("File %s does not exist, aborting." % entry)
            sys.exit(1)

        hdulist = pyfits.open(entry, mode='update')
        nn=0
        for hdu in hdulist:
            to_remove = []
            for key in hdu.header:
                if "PV" in key:
                    to_remove.append(key)
            for key in to_remove:
                del hdu.header[key]
                logging.debug("Deleting %s from %s" % (key, entry))
                nn += 1
        hdulist.close(output_verify='silentfix+ignore')
        logging.info("Header of %s cleaned; %d PV kwds removed" %(entry, nn))


# Command line running
if __name__ == '__main__':

    # Get the argument parser and parse the command line
    parser = get_parser()
    args = parser.parse_args()

    # Run the main code
    main(args)

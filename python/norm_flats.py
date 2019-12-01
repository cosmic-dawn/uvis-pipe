# Uses python 2
# -------------------------------------------------------
# Normalise the flat fields
# input : l, list     : list of flats
#       : o, osuff    : output suffix
#       : v, verbose_level : ERROR,WARNING,INFO,DEBUG
#       : -, log      : logfile
# --------------------------------------------------------


import os, sys
import numpy
import astropy.io.fits as pyfits
#import pylab
from logger_lib import setup_logger
import argparse


def get_parser():

    parser = argparse.ArgumentParser(description='Create condor submission file to run qfits first pass')
    parser.add_argument('-l', '--list', dest='imlist', help='list of flats', type=str, default="")
    parser.add_argument('-o', '--osuff', dest='osuff', help='Output suffix (def = _norm.fits)', type=str, default="_norm.fits")
    parser.add_argument('-s', '--stat', dest='stat', help="Normalisation statistic MEAN,MEDIAN,MODE (def=MEDIAN)", type=str, default='MEDIAN')

    # verbose options
    parser.add_argument('-v', '--verbose_level', dest='verbose_level', help='Verbose level (ERROR,WARNING,INFO,DEBUG)', type=str, default='ERROR')
    parser.add_argument('--log', dest='flog', help='Log filename', type=str, default="norm_flats.log")
    return parser


def normalize_val(data, stat):

    stat_val = 0
    if stat == 'MEDIAN':
        stat_val = numpy.median(data.flatten())
    elif stat == 'MEAN':
        stat_val = numpy.mean(data.flatten())
    elif stat == 'MEDIANCLIP':
        flat = data.flatten()

        # clip it
        median = numpy.median(flat)
        disp = numpy.std(flat)
        min = median - disp
        max = median + disp

        flat3 = flat[numpy.where(flat > min)]
        flat2 = flat3[numpy.where(flat3 < max)]

        stat_val = numpy.median(flat2)
    elif stat == 'MODE':
        flat = data.flatten()

        # clip it
        median = numpy.median(flat)
        disp = numpy.std(flat)
        min = median - disp
        max = median + disp
        # print "minmax = "+str(min)+" "+str(max)

        flat3 = flat[numpy.where(flat > min)]
        flat2 = flat3[numpy.where(flat3 < max)]

#       # Do not build histogram

        stat_val = bins[imax]
    else:
        print "Should never get there ... bad stat"
        sys.exit(1)

    # print stat_val
    return stat_val

def main(args):

    logging = setup_logger(args.flog, loglevel=args.verbose_level, file_loglevel="ERROR",  name="norm_flat.py")

    # Normalization computation ?
    if not args.stat.upper() in ['MODE', 'MEDIAN', 'MEAN']:
        logging.error("Incoorect statistic for normalization ...('MODE', 'MEDIAN', 'MEAN')")
        sys.exit(1)
    
    # Get the list of images
    flist = []
    if not os.path.isfile(args.imlist):
        logging.error("Impossible to find list ... %s" % args.imlist)
        sys.exit(1)
    
    wlist0 = os.popen("cat " + args.imlist).readlines()
    for w in wlist0:
        if not os.path.isfile(w.strip().split()[0]):
            continue
        flist.append(w.strip().split()[0])
    
    # Do the job
    logging.info("Working on :")
    for w in flist:
        logging.info("  -- %s " % w)
    
        pyim = pyfits.open(w)
        outfile = w.split(".fits")[0] + args.osuff
    
        next = len(pyim)
    
        if next == 1:
            normalize_val(pyim[0].data, args.stat)
        else:
    
            for iext0 in range(next - 1):
                iext = iext0 + 1
                val = normalize_val(pyim[iext].data, args.stat)
                print " - ext %0i : %0.2f"%( iext0 + 1, val)
                pyim[iext].data /= val
        pyim.writeto(outfile, output_verify="warn")
        pyim.close()

# Command line running
if __name__ == '__main__':

    # Get the argument parser and parse the command line
    parser = get_parser()
    args = parser.parse_args()

    # Run the main code
    main(args)

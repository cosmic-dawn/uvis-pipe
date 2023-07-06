#!/usr/bin/env python

# -------------------------------------------------------
# Convert a list of VISTA images to wircam standards
# input : l,list     : list of image 
#       : i,image    : image
# --------------------------------------------------------

import os, sys
import re, glob
import argparse
import numpy
import astropy.io.fits as pyfits
# our libs
import convertim_lib
from logger_lib import setup_logger


def get_survey_keyname(name):
    if name.lower() == "ultravista":
        return "Ult"
    if name.lower() == "viking":
        return "VIK"
    if name.lower() == "video":
        return "VID"
    return -1


def get_parser():

    parser = argparse.ArgumentParser(description="Convert a list of VISTA images to wircam standards")
    
    parser.add_argument('-l', '--list', dest='flist', help='List of images (def = all)', type=str, default='')
    parser.add_argument('-i', '--image', dest='image', help='image file', type=str, default='')
    parser.add_argument('-o', '--osuff', dest='osuff', help='Output images suffix', type=str, default='_WIRCam.fits')
    parser.add_argument('--over', dest='over', help='Overwrite image (old image in _orig.fits)', action='store_true', default=False)


    # Keywords from stacks
    parser.add_argument('-s', '--stack_dir', dest='stack_dir', help='Stacks directory', type=str, default='./')
    parser.add_argument('--stack_addzp', dest='stack_addzp', help='Config files of ZP to copy from stacks', type=str, default='')
    
#    # Keywords from QC files
#    parser.add_argument('-q', '--QCfiles', dest='QCfiles', help='List of QC files (fits)', type=str, default='')
#    parser.add_argument('--QC_addzp', dest='QC_addzp', help='Config files of ZP to copy from QCfiles (olfKey newKey)', type=str, default='')
    
    parser.add_argument('--survey', dest='survey', help='Survey name (def = all)', type=str, default='Ultravista')

    # verbose options
    parser.add_argument('-v', '--verbose_level', dest='verbose_level', help='Verbose level (ERROR,WARNING,INFO,DEBUG)', type=str, default='ERROR')
    parser.add_argument('--log', dest='flog', help='Log filename', type=str, default="convert_to_WIRCAM.log")

    return parser

def main(args):
    """
    Main fonction. Get the arguments from the args dictionnary
    :param args: dictionnary of arguments (see parser for list and description)
    :return:
    """

    # Setup the logger
    logging = setup_logger(args.flog, loglevel=args.verbose_level, file_loglevel="ERROR", name="convert_to_WIRCAM")

    # Get the list of images 
    imlist = []
    if args.flist != "":
        if not os.path.isfile(args.flist):
            logging.error("Impossible to find list of images : %s" % args.flist)
            sys.exit(1)
        lines = os.popen("cat " + args.flist)
        for line in lines:
            file = line.strip().split()[0]
            if os.path.isfile(file):
                imlist.append(file)
    else:
        if args.image != "" and os.path.isfile(args.image):
            imlist.append(args.image)
    
    if len(imlist) == 0:
        logging.error("No images to work on !!")
        sys.exit(1)
    
    logf = open(args.flog, 'w')
    logf.write("# 1 IMAGE \n")
    logf.write("# 2 STATUS \n")
    
#    #########################################
#    # Keys from QC files (NOT FINISHED YET) #
#    #########################################
#    
#    # Check list of QC files
#    list_QCfiles = []
#    if args.QCfiles != "":
#        list_QCfiles0 = args.QCfiles.split(",")
#        for f in list_QCfiles0:
#            if not os.path.isfile(f):
#                logging.error("QC file not found ... %s" % f)
#                sys.exit(1)
#            list_QCfiles.append(f)
#    
#    # Read the list of keys
#    data_QCkeys = {}
#    if args.QC_addzp != "":
#        if not os.path.isfile(args.QC_addzp):
#            logging.error("Impossible to open %s " % args.QC_addzp)
#            sys.exit(1)
#        lines = os.popen("cat " + args.QC_addzp)
#    
#        toks = line.strip().split()
#        ikey = ""
#        okey = ""
#        if toks[0] == "HIERARCH":
#            ikey = " ".join(toks[:-1])
#        else:
#            ikey = toks[0]
#        okey = toks[-1]
#    
#        if ikey == "" or okey == "":
#            print "Big problem in adding keywords ... line : ", line
#            sys.exit(1)
#        data_QCkeys[ikey] = okey
#    
#    # Read the keywords from the QC files
#    data_QC = {"NB118": {}, "J": {}, "H": {}, "Ks": {}, "Y": {}, "Z": {}}
#    keys_QC = data_QCkeys.keys()
#    for f in list_QCfiles:
#        pycat = pyfits.open(f)
#        data = pycat[1].data
#        keys = pycat[1].data.names
#    
#        # Which survey
#        surveyname = get_survey_keyname(args.survey)
#    
#        inds = numpy.where(data['surveyname'] == surveyname)
#        filenames = list(set(data['filename'][inds]))
#    
#        for filename in filenames:
#            inds = numpy.where(data['filename'] == filename)
    
    ####################
    # Keys from stacks #
    ####################
    
    # Check the stack dir
    if not os.path.isdir(args.stack_dir):
        logging.error("Impossible to find stack directory %s" % args.stack_dir)
        sys.exit(1)
    
    # Read the ZP file (stack_Key (can be HIARARCH) newKey (8 chars max))
    data_STkeys = {}
    list_keystacks = []
    if args.stack_addzp != "" and not os.path.isfile(args.stack_addzp):
        logging.error("Impossible to open %s" % args.stack_addzp)
        sys.exit(1)
    lines = os.popen("cat " + args.stack_addzp)
    for line in lines:
        if line.strip()[0] == "#":
            continue
        toks = line.strip().split()
        ikey = ""
        okey = ""
        if toks[0] == "HIERARCH":
            ikey = " ".join(toks[:-1])
        else:
            ikey = toks[0]
        okey = toks[-1]
    
        if ikey == "" or okey == "":
            logging.error("Big problem in adding keywords ... line : %s " % line)
            sys.exit(1)
        data_STkeys[ikey] = okey
        list_keystacks.append(ikey)
    
    # Read the keywords from the stacks
    data_stacks = {}
    data_progenitors = {}
    list_st = glob.glob(args.stack_dir + "/v*st.fits")  ####### DEBUG
    if len(list_st) == 0:
        print "Big problem ... no stacks !! ", args.stack_dir
        sys.exit(1)
    
    logging.info(">> Read keywords in stacks :")
    prog_pattern = re.compile(r'PROV(\d\d\d\d)')
    
    for st in list_st:
        stname = st.split("/")[-1].replace(".fits", "")
    
        data_stacks[stname] = []
        data_progenitors[stname] = []
    
        logging.debug("  -- %s" % st)
        pyim = pyfits.open(st)
    
        # Get keywords
        filter = pyim[0].header['HIERARCH ESO INS FILT1 NAME']
        for ikey in list_keystacks:
            okey = data_STkeys[ikey]
            vals = []
            for i in range(len(pyim)):
                if ikey in pyim[i].header:
                    vals.append(pyim[i].header[ikey])
            if len(vals) == 0:
                logging.error("Big problem of keys %s in %s " % (ikey, st))
                sys.exit(1)
            data_stacks[stname].append(vals)
    
        # Get progenitors
        keys = pyim[1].header.keys()
        for k in keys: 
            match = prog_pattern.search(k)
            if match and match.group(1) != "0000":
                v = pyim[1].header[k].split(".")[0]
                data_progenitors[stname].append(v)
    
        pyim.close()
    
    #############
    # Real work #
    #############
    
    logging.debug("Working on :")
    for im in imlist:
        logging.debug("  -- %s" % im)
    
        if args.over:
            orig = im.split(".fits")[0] + "_orig.fits"
            os.system("mv " + im + " " + orig)
            out = im
            im = orig
        else:
            out = im.split(".fits")[0] + args.osuff
    
        convim = convertim_lib.conv_im(im, "VISTA")
        convim.open_image()
    
        doit = convim.convert_im("VISTA", "WIRCAM", "yes")
    
        if len(data_STkeys.keys()) > 0:
            doit2 = convim.add_keys_fromStack(data_STkeys, data_stacks, list_keystacks, data_progenitors)

        if args.over:
            logf.write(out + " " + str(doit) + "\n")
        else:
            logf.write(im + " " + str(doit) + "\n")
    
        convim.close_file(out)
    
    logf.close()

# Command line running
if __name__ == '__main__':

    # Get the argument parser and parse the command line
    parser = get_parser()
    args = parser.parse_args()

    # Run the main code
    main(args)

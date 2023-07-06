#!/opt/intel/intelpython2-2019.4-088/intelpython2/bin/python
#------------------------------------------------
# validFraction.py
#------------------------------------------------
# extract fraction of unmasked region of a mask file: 
# - list: list of images, MEF or SEF files
#
# Examples:
#   validFraction.py -l list (of v2*_mask.fits)    
#------------------------------------------------

import sys, re, os
import math
import numpy as np
import astropy.io.fits as pyfits
from optparse import OptionParser

parser = OptionParser()
parser.add_option('-l', '--list',   dest='flist',  help='List of images', type='string', default="")

# Parse command line
try:
    opts,args = parser.parse_args(sys.argv[1:])
except:
    print "Error ... check usage with validFrction.py -h "
    sys.exit(1)

imlist = []
file = open(opts.flist, 'r')
lines = file.readlines()
file.close()

for line in lines:
    ima  = line.split()[0]
    pima = pyfits.open(ima)

    n_ext = len(pima)
    if n_ext == 1:
        fr = np.mean(pima[0].data)
        print "%-22s  %0.2f "%(ima.split('.')[0], 100*fr)
    else:
        fr = []
        for i in range(1,n_ext):
            fr.append(np.mean(pima[i].data))
        print "%-20s"%ima.split('.')[0], ' '.join(["%5.2f"%x for x in fr])
    
    pima.close()

exit(0)

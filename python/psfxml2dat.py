#!/opt/intel/intelpython2-2019.4-088/intelpython2/bin/python
#
#-----------------------------------------------------------------------------
# Convert selected columns to ascii table file
#-----------------------------------------------------------------------------

import math,os,sys
import numpy as np
from astropy.io.votable import parse
from optparse import OptionParser

parser = OptionParser()
parser.add_option('-l', dest='inlist', help='list of xml files', type='string', default="PSFsel.lst")
parser.add_option('-o', dest='outtab', help='output table',  type='string', default="PSFsel.dat")

try:
    opts, args = parser.parse_args(sys.argv[1:])
except:
    print "SYNTAX: xml2dat.py -h "
    sys.exit(1)

## print(opts.inlist, opts.outtab)
# Open output table and print header
w = open(opts.outtab,'w')
#w.write("# Name              FWHM   Ell   PixSc\n")
#w.write("# Name              FWHM   Elli\n")

# loop over xml files and write data to outpu table
inlist = open(opts.inlist, 'r')
for f in inlist:
    xml = f.strip()
    #print("Read from {:}".format(xml))

    vot    = parse(xml)
    fields = vot.get_table_by_id("PSF_Fields")     # Fields table
    ## uncomment next line to lists col names, but not in nice way
    ##cols   = fields.array.dtype.names; print(cols); sys.exit() 
    
    name     = fields.array["Catalog_Name"].data
    fwhm     = fields.array["FWHM_WCS_Mean"].data
    elli     = fields.array["Ellipticity_Mean"].data
    scal     = fields.array["PixelScale_WCS_Mean"].data

    for n in range(len(name)):
        ff = name[n].split('_')[:2]    # file root name
        w.write("{:18s}{:7.3f} {:7.4f}  \n".format(ff[0]+"_"+ff[1], fwhm[n], elli[n]) )

w.close()



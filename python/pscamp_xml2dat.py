#!/opt/intel/intelpython2-2019.4-088/intelpython2/bin/python
#-----------------------------------------------------------------------------
# module pscamp_xml2dat.py: used in pscamp.sh
# - extracts selected data from xml file produced by scamp
# AMo - Oct.20:  adapted for DR5
#-----------------------------------------------------------------------------

import math,os,subprocess,sys
import numpy as np
from astropy.io.votable import parse

xml="pscamp.xml"
dat="pscamp.dat"

vot    = parse(xml)
fields = vot.get_table_by_id("Fields")     # Fields table
cols   = fields.array.dtype.names  # lists col names, but not in nice way

# How to get list of table names??
# in practice: Fields, FGroups, Astrometric_Instruments, Photometric_Instruments, Warnings

## print list of columns of Fields table
#for n in range(len(cols)):    print n, cols[n]
#sys.exit(0)

name     = fields.array["Catalog_Name"].data
#ndetetc  = fields.array["NDetect"].data            # Num detections
#objtype  = fields.array["Astr_Instrum"].data       # paw / cosoms
contrast = fields.array["XY_Contrast"].data       
#coords   = fields.array["Field_Coordinates"].data  # RA/Dec
#obsdate  = fields.array["Observation_Date"].data        
#exptime  = fields.array["Exposure_Time"].data        
#airmass  = fields.array["AirMass"].data        
#pixscale = fields.array["Pixel_Scale"].data        
chi2int  = fields.array["Chi2_Internal"].data        
chi2ref  = fields.array["Chi2_Reference"].data        
xshift   = fields.array["DX"].data*3600.
yshift   = fields.array["DY"].data*3600.
zpcorr   = fields.array["ZeroPoint_Corr"].data

shift    = (xshift**2 + yshift**2)**0.5              # total shift

# Build .dat file; columns are:
f = open(dat,'w')
#f.write("# From table Fields of %s\n"%xml)
#f.write("# col 1. root name\n")
#f.write("# col 2. XY-contrast\n")
#f.write("# col 3. ZP-corr      [mag]\n")
#f.write("# col 4. chi2-int\n")
#f.write("# col 5. chi2-ref\n")
#f.write("# col 6. x-shift   [arcsec]\n")
#f.write("# col 7. y-shift   [arcsec]\n")
#f.write("# col 8. net shift [arcsec]\n")
#f.write("#-------------------------------------------\n")

f.write("# name              XY-cnt  ZPcorr     X2-i     X2-e   x-shft  y-shft     shft\n")
#        v20091222_00368     11.821   0.008     3.64     8.24   -0.044  -0.007    0.044

for n in range(len(name)):
    ff = name[n].split('.')[0]    # file root name
    f.write("%-18s %7.3f %7.3f %8.2f %8.2f  %7.3f %7.3f %8.3f\n"%
            (ff, contrast[n],zpcorr[n],  chi2int[n],chi2ref[n],  xshift[n],yshift[n],  shift[n]) )

f.close()

# list of files with low contrast
lc = contrast < 2
nlc = name[lc]
print "Nun files with low contrast: %0i of %i"%(len(nlc),len(name))


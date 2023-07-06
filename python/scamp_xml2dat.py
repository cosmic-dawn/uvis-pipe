#!/opt/intel/intelpython2-2019.4-088/intelpython2/bin/python
# -*- coding: utf-8 -*-

import numpy as np
import math,os,sys
import subprocess
from astropy.io.votable import parse

if len(sys.argv) == 1:
    print("  SYNTAX: scamp_xml2dat.py scamp_file.xml {fields}")
    print("          if fields is given, then print field names")
    sys.exit()

if len(sys.argv) >= 3:
    print_list = True
else:
    print_list = False

xml = sys.argv[1]
dat = xml.split('.xml')[0]+".dat"

vot    = parse(xml)
fields = vot.get_table_by_id("Fields")     # Fields table
cols   = fields.array.dtype.names  # lists col names, but not in nice way

# How to get list of table names??
# in practice: Fields, FGroups, Astrometric_Instruments, Photometric_Instruments, Warnings

## print list of columns of Fields table
if (print_list == True):
    for n in range(len(cols)):    print n, cols[n]
    sys.exit(0)

# Nominal fields to export:
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

shift    = (xshift**2+yshift**2)**0.5              # total shift

# Build .dat file
f = open(dat,'w')
#f.write("# From table Fields of %s\n"%xml)
#f.write("# col 1. root name\n")
#f.write("# col 2. XY-contrast\n")
#f.write("# col 3. ZP-corr     [mag]\n")
#f.write("# col 4. chi2-int\n")
#f.write("# col 5. chi2-ref\n")
#f.write("# col 6. x-shift     [arcsec]\n")
#f.write("# col 7. y-shift     [arcsec]\n")
f.write("# File            contrast     ZPcorr  chi2-int   chi2-ref   x-shift  y-shift     shift\n")
#        v20091222_00368     18.127     0.01733  181.391    355.456    -1.468    6.683     6.843 
#f.write("#-------------------------------------------\n")

for n in range(len(name)):
    ff = name[n].split('.')[0]    # file root name
    f.write("%-18s %7.3f %11.5f %8.3f %10.3f  %8.3f %8.3f  %8.3f \n"%(ff, contrast[n],zpcorr[n], chi2int[n],chi2ref[n], xshift[n],yshift[n], shift[n]) )

#f.write("#-------------------------------------------\n")
f.close()

## files with low contrast
#lc = contrast < 4
#nlc = name[lc]
#print("Num files with low contrast: {:4n} of {:}".format(len(nlc),len(name)))
#
## files with shift > 120
#lc = shift > 120
#nlc = name[lc]
#print("Num files with shift > 120:   {:4n} of {:}".format(len(nlc),len(name)))
#
## files with large or small |ZP corr| > 15 mag
#lc = abs(zpcorr) > 15
#nlc = name[lc]
#print("Num files large ZP corr:     {:4n} of {:}".format(len(nlc),len(name)))

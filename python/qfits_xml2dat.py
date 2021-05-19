#-----------------------------------------------------------------------------
# Convert selected columns to ascii table file
#-----------------------------------------------------------------------------

import math,os,sys
import numpy as np
from astropy.io.votable import parse

vot = parse('scamp.xml')
for t in vot.iter_fields_and_params(): print(t)



# loop over xml files and write data to outpu table
inlist = open(opts.list, 'r')
for f in inlist:
    xml = f.strip()
    #print("Read from {:}".format(xml))

    vot    = parse(xml)
    fields = vot.get_table_by_id("PSF_Fields")     # Fields table
    cols   = fields.array.dtype.names  # lists col names, but not in nice way

    name     = fields.array["Catalog_Name"].data
    fwhm     = fields.array["FWHM_WCS_Mean"].data
    elli     = fields.array["Ellipticity_Mean"].data
    scal     = fields.array["PixelScale_WCS_Mean"].data
    for n in range(len(name)):
        ff = name[n].split('_')[:2]    # file root name
        w.write("{:18s}{:6.2f} {:6.3f} {:6.3f}  \n".format(ff[0]+"_"+ff[1], fwhm[n], elli[n], scal[n]) )

w.close()



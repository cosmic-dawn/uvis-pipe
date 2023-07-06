#!/opt/intel/intelpython3-2022.2.1/intelpython/python3.9/bin/python
#
#-----------------------------------------------------------------------------
# write PSFsel.dat for DR6: for each file, the table contains:
# filename  mean(fwhm) rms(fwhm) Nchips  mean(elli) rms(elli) Nchips
# where the mean and rms are over the valid chips
#
# AMo, 2023.may
#-----------------------------------------------------------------------------

import math,os,sys
import numpy as np
from astropy.io.votable import parse

if (len(sys.argv) == 1) | (sys.argv[1] == '-v'):
    print("  SYNTAX:  {:} PSFEx_output_file(s).xml".format(sys.argv[0].split('/')[-1]))
    print("  PURPOSE: write the fwhm, elli, and pix.scale derived for each chip to an ascii file")
    sys.exit()

Nfiles = len(sys.argv)
#print(">> Found {:} files to process".format(Nfiles-1))

for n in range(1,Nfiles):
    xml_file = sys.argv[n]
    out_file = "PSFsel.dat" #; print(xml_file)

    vot    = parse(xml_file, verify='ignore')
    fname = "PSF_Extensions"                #; print(fname)
    fields = vot.get_table_by_id(fname)     # Fields table
    cols   = fields.array.dtype.names       #; print(cols) #; sys.exit() 
    
#    w = open(out_file,'w')                  # Open output table for writing
    name = xml_file.split('_psf')[0]
    fwhm = fields.array["FWHM_WCS_Mean"].data           
    st1 = "{:}   {:5.3f} {:5.3f} {:-2n}".format(name, np.mean(fwhm), np.std(fwhm), len(fwhm))

    elli = fields.array["Ellipticity_Mean"].data        
    st2 = "   {:5.3f} {:5.3f} {:-2n}".format(np.mean(elli), np.std(elli), len(elli))
    print(st1+st2)
    
sys.exit()

#rm v20*.xml
#for f in $(cat list_N); do ln -s ../xml/${f%.fits}_psfex.xml .; done
#mkPSFsel.py *.xml > PSFsel_N-new.dat

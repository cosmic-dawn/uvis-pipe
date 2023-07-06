#!/opt/intel/intelpython3-2022.2.1/intelpython/python3.9/bin/python
#
#-----------------------------------------------------------------------------
# write the fwhm, elli, and pix.scale derived for each chip to an ascii file
# AMo, 2022.nov.17
#-----------------------------------------------------------------------------

import math,os,sys
import numpy as np
from astropy.io.votable import parse

if (len(sys.argv) == 1) | (sys.argv[1] == '-v'):
    print("  SYNTAX:  {:} PSFEx_output_file(s).xml".format(sys.argv[0].split('/')[-1]))
    print("  PURPOSE: write the fwhm, elli, and pix.scale derived for each chip to an ascii file")
    sys.exit()

Nfiles = len(sys.argv)
print(">> Found {:} files to process".format(Nfiles-1))

for n in range(1,Nfiles):
    xml_file = sys.argv[n]
    out_file = xml_file.split('.xml')[0] + ".dat" #; print(xml_file)

    vot    = parse(xml_file, verify='ignore')
    fname = "PSF_Extensions"                #; print(fname)
    fields = vot.get_table_by_id(fname)     # Fields table
    cols   = fields.array.dtype.names       #; print(cols) #; sys.exit() 
    
    w = open(out_file,'w')                  # Open output table for writing
    name = xml_file.split('_psf')[0]
    ntot = fields.array["NStars_Loaded_Total"].data
    nacc = fields.array["NStars_Accepted_Total"].data
    fwhm = fields.array["FWHM_WCS_Mean"].data           
    elli = fields.array["Ellipticity_Mean"].data        
    scal = fields.array["PixelScale_WCS_Mean"].data     
    
    w.write('## PSF info from {:} \n'.format(xml_file))
    string = "Ntot   " + ' '.join(["{:7n}".format(x) for x in ntot])  ; w.write(string + "\n") 
    string = "Nacc   " + ' '.join(["{:7n}".format(x) for x in nacc])  ; w.write(string + "\n") 
    string = "fwhm   " + ' '.join(["{:7.4f}".format(x) for x in fwhm])  ; w.write(string + "\n") 
    string = "elli   " + ' '.join(["{:7.4f}".format(x) for x in elli])  ; w.write(string + "\n") 
    string = "scale  " + ' '.join(["{:7.4f}".format(x) for x in scal])  ; w.write(string + "\n") 
    w.close()
#    os.system("cat "+out_file)
sys.exit()

# list of columns in Extenstions table

# Extension
# NStars_Loaded_Total
# NStars_Loaded_Min
# NStars_Loaded_Mean
# NStars_Loaded_Max
# NStars_Accepted_Total
# NStars_Accepted_Min
# NStars_Accepted_Mean
# NStars_Accepted_Max
# FWHM_FromFluxRadius_Min
# FWHM_FromFluxRadius_Mean
# FWHM_FromFluxRadius_Max
# Sampling_Min
# Sampling_Mean
# Sampling_Max
# Chi2_Min
# Chi2_Mean
# Chi2_Max
# FWHM_Min
# FWHM_Mean
# FWHM_Max
# FWHM_WCS_Min
# FWHM_WCS_Mean
# FWHM_WCS_Max
# Ellipticity_Min
# Ellipticity_Mean
# Ellipticity_Max
# Ellipticity1_Min
# Ellipticity1_Mean
# Ellipticity1_Max
# Ellipticity2_Min
# Ellipticity2_Mean
# Ellipticity2_Max
# MoffatBeta_Min
# MoffatBeta_Mean
# MoffatBeta_Max
# Residuals_Min
# Residuals_Mean
# Residuals_Max
# FWHM_PixelFree_Min
# FWHM_PixelFree_Mean
# FWHM_PixelFree_Max
# FWHM_PixelFree_WCS_Min
# FWHM_PixelFree_WCS_Mean
# FWHM_PixelFree_WCS_Max
# Ellipticity_PixelFree_Min
# Ellipticity_PixelFree_Mean
# Ellipticity_PixelFree_Max
# Ellipticity1_PixelFree_Min
# Ellipticity1_PixelFree_Mean
# Ellipticity1_PixelFree_Max
# Ellipticity2_PixelFree_Min
# Ellipticity2_PixelFree_Mean
# Ellipticity2_PixelFree_Max
# MoffatBeta_PixelFree_Min
# MoffatBeta_PixelFree_Mean
# MoffatBeta_PixelFree_Max
# Residuals_PixelFree_Min
# Residuals_PixelFree_Mean
# Residuals_PixelFree_Max
# Asymmetry_Min
# Asymmetry_Mean
# Asymmetry_Max
# Area_Noise_Min
# Area_Noise_Mean
# Area_Noise_Max
# PixelScale_WCS_Min
# PixelScale_WCS_Mean
# PixelScale_WCS_Max

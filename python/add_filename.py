#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
'''
-----------------------------------------------------------------------------
Write filename kwd and mean skylevel to ext0 of each MEF file:  
... for some reason this is missing is files post 2019 nov.
SYNTAX: add_filename.py v20*.fits
METHOD:

AMo 16.apr.23
-----------------------------------------------------------------------------
'''

import os,sys
import numpy as np
import astropy.io.fits as fits

# check that we are in an images directory
path = os.getcwd(); 
dire = path.split('/')[-2]
#if dire != 'images':
#    print("### ERROR: not in a input data directory directory ... quitting")
#    sys.exit()

for n in range(1,len(sys.argv)):
    ima = sys.argv[n] ; print(ima)
    pima = fits.open(ima, mode='update')
    hd = pima[0].header
    hd['FILENAME'] = ima[:15]+'.fits'
    # read skylevel 
    sky=[] ; noi=[]
    for e in range(1,17):
        sky.append(pima[e].header['SKYLEVEL'])
        noi.append(pima[e].header['SKYNOISE'])
#    print(np.mean(sky), np.mean(noi), np.std(sky) )
    hd['SKYLEVEL'] = (np.mean(sky), 'mean skylevel of all chips')
    hd['SKYNOISE'] = (np.mean(noi), 'mean skynoise of all chips')
    hd['SKYSTDEV'] = (np.std(sky),  'stdev of skylevel of all chips')
   
    pima.close(output_verify='silentfix+ignore')

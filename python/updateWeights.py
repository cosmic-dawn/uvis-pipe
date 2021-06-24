#!/usr/bin/env python

'''
--------------------------------------------------------------------
 Mask area where no sky is calculated: combine the zeroes.fits (same for all images) with the .sky.fits
 file to create a _zeroes.file for each input file
 input : l,list   : list of sky-subtracted images
       : z,zeroes : name of zeroes.fits images; def = zeroes.fits
 It is assumed that there are pure sky images named as in list, but ending with _sky.fits
 Method: for mask pix p: if sky(p)=0; then mask(p)=0; else mask(p)=zeroes(p)

 18-02-20, AMo: taylored for UltraVista:
 - modified to use single zeroes file;
 - it is assumed that all images have 16 extensions
 - removed superfluous checks
 06-07-18, AMo: changed to updateWeights: just update the weight file. 
--------------------------------------------------------------------
'''

import os, sys
import numpy as np
import astropy.io.fits as fits
from optparse import OptionParser

parser = OptionParser()
parser.add_option('-l', '--list', dest='imlist', help='list of fits sub files', type='string', default='')

try:
    opts,args = parser.parse_args(sys.argv[1:])
except:
    print "Error ... check usage with updateWeights.py -h ";
    sys.exit(1);


file = open(opts.imlist, 'r')
lines = file.readlines()
file.close()

for line in lines:
    im = line.split()[0]
    sky = im.split('.fits')[0] + '_sky.fits'           # input sky image
    wgt = im.split('.fits')[0] + '_weight.fits'        # output weight
    #ori = im.split('.fits')[0] + '_weight_orig.fits'   # original weight file
    
    ss = fits.open(sky)
    n_ext = len(ss)   

    if (n_ext != 17):
        print "ERROR: found only %i extensions ... skip %s"%(n_ext, im)
    else:
        ww = fits.open(wgt, mode="update")
        
        tot=0
        for i in range(1,n_ext):
            zz = np.nonzero(ss[i].data == 0)
            tot += len(zz[0])
            #print ">> ext %i, npix %i"%(i, len(zz[0]))
            ww[i].data[zz] = 0
        
        ww[0].header['history'] = "# weights updated based on %s"%sky
        ww.close(output_verify='silentfix+ignore')
        print ">> updated weight %s; %i pixels masked" %(wgt, tot)
        ss.close()

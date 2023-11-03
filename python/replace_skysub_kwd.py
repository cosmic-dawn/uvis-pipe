#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
'''
-----------------------------------------------------------------------------
Replace SKYSUB kwd in files.  The files to edit, the current/wrong SKYSUB kwd 
and the new one are given in the toFix_{filter} files.  The latter are 3-col
tables, with image filename, current skysub, replacement skysub, all w/o the
.fit(s) extension.  
NB. image files have to have write permission.

SYNTAX: replace_skysub_kwd.py fix_file
METHOD:

AMo - 19.sep.23
AMo - 9.oct.23: fixed bug which set sky ext[1] to all exts
-----------------------------------------------------------------------------
'''

import os,sys
#import numpy as np
import astropy.io.fits as fits

fixtab = sys.argv[1]

# Read the fix table
lines = os.popen("cat " + fixtab).readlines()
for line in lines:
    if line.strip()[0] == "#":
        continue
    ima = line.strip().split()[0]
    oldsky = line.strip().split()[1]
    newsky = line.strip().split()[2]
#    print(ima, oldsky, newsky)
    
    for n in range(1,len(sys.argv)):
        pima = fits.open(ima+".fits", mode='update')
#        pima = fits.open(ima+".fits")
        hd = pima[0].header
        for e in range(1,17):
            sss = "Done with {:}.fit[{:}]".format(newsky, e) 
            if (e == 11): 
                print(ima, pima[e].header['SKYSUB'], " replaced with", sss)
            pima[e].header['SKYSUB'] = sss
        pima.close(output_verify='silentfix+ignore')

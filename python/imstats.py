#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
#-----------------------------------------------------------------------------
# mean of each extension


import sys  #, re, os
import numpy as np
import astropy.io.fits as pyfits

ima = sys.argv[1]

## Parse command line
#try:
#    opts, args = parser.parse_args(sys.argv[1:])
#except:
#    print "Error ... check usage with imsub.py -h "
#    sys.exit(1)

# prepare
pima  = pyfits.open(ima)
n_ext = len(pima)     # num extensions

# do the subraction
if n_ext == 1: n_ext = 2  # to handle SEF files

mm = []
nn = []
print("File: "+ima)
for e in range(1, 11):
    mm.append(np.nanmean(pima[e].data))
    loc = np.where(np.isfinite(pima[e].data))[0]
    print("-- ext {:2d}, Nfinite: {:7d} = {:5.2f}%".format(e, len(loc), 100*len(loc)/2048/4096))
                   

pima.close()
mm=np.array(mm)
sys.exit()

#np.set_printoptions(precision=2) ; print(mm)  # -- OK too

x=np.array2string(mm, precision=2)
print(ima,": ", x)  # -- OK





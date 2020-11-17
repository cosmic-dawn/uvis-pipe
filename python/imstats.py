#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
#
#-----------------------------------------------------------------------------
# some basic stats for each extension:
# - mean, median, min, max ... Num of zeroes values
#-----------------------------------------------------------------------------

import sys  #, re, os
import numpy as np
import astropy.io.fits as pyfits

ima = sys.argv[1]

# prepare
pima  = pyfits.open(ima)
n_ext = len(pima)     # num extensions

if n_ext == 1:   # to handle SEF files
    n_ext = 2

mmean = 0 ; mmedi = 0
mmaxi = 0 ; mmini = 0
nzero = 0
print("File: "+ima)
print("ext      mean    median         min       max      %zeros")
for e in range(1, n_ext):
    data = pima[e].data
    loc = np.where(data == 0)[0]
    mean = np.mean(data); medi = np.median(data)
    mini = np.min(data); maxi = np.max(data)
    nz = 100*len(loc)/2048/2048
    print(" {:2d}  {:8.2f}  {:8.0f}   {:9.2f} {:9.2f}   {:9.2f}".format(e, mean, medi, mini, maxi, nz))
    mmean += mean;  mmedi += medi
    mmini = np.min([mini, mmini]);  mmaxi = np.max([mmaxi, maxi])
    nzero += len(loc)

print("net  {:8.2f}  {:8.0f}   {:9.2f} {:9.2f}   {:9.2f}".format(mmean/16, mmedi/16, mmini, mmaxi, 100*nzero/16/2048/2048))

pima.close()
#mm=np.array(mm)
sys.exit()

#np.set_printoptions(precision=2) ; print(mm)  # -- OK too

x=np.array2string(mm, precision=2)
print(ima,": ", x)  # -- OK





#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
#
#-----------------------------------------------------------------------------
# some basic stats for each extension:
# - mean, median, min, max ... Num of zeroes values
#-----------------------------------------------------------------------------

import sys  #, re, os
import numpy as np
import astropy.io.fits as pyfits

fmt = " {:2n} {:8.2f}  {:8.2f}   {:8.2f}   {:8.0f} {:8.0f}   {:6.2f}"
print(len(fmt), fmt.replace("{:2n}","  "))
for n in range(1,len(sys.argv)):
    ima = sys.argv[n]
    
    pima  = pyfits.open(ima)
    n_ext = len(pima)     # num extensions
    
    if n_ext == 1:   # to handle SEF files
        n_ext = 2

    mmean = 0 ; mmedi = 0 ; mstd = 0 ; mmaxi = 0 ; mmini = 0 ; nzero = 0
    print("File: {:}; {:} extenstions".format(ima,n_ext))
    print("ext    mean      median    st.dev       min      max    %zeros")
    thresh = 9500
    for e in range(1, n_ext):
        data = pima[e].data

        loc = ((data > -thresh) & (data < thresh))
        mean = np.mean(data[loc]); medi = np.median(data[loc])
        mini = np.min(data[loc]); maxi = np.max(data[loc])
        std  = np.std(data[loc])

        loc = np.where(data == 0)[0]
        nz = 100*len(loc)/2048/2048
        print(fmt.format(e, mean, medi, std, mini, maxi, nz))
        mmean += mean ; mmedi += medi ; mstd += std
        mmini = np.min([mini, mmini]);  mmaxi = np.max([mmaxi, maxi])
        nzero += len(loc)

    print(fmt.replace(" {:2n}","All").format(mmean/16, mmedi/16, mstd/16, mmini, mmaxi, nzero/16/2048/20.48))
    
    pima.close()
#mm=np.array(mm)
sys.exit()

#np.set_printoptions(precision=2) ; print(mm)  # -- OK too

x=np.array2string(mm, precision=2)
print(ima,": ", x)  # -- OK





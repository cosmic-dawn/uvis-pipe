#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
#
#-----------------------------------------------------------------------------
# Study saturation in image
#-----------------------------------------------------------------------------
# CAVEATS: threshold is independent of chip since all extensions are merged to
# find the max. Script assumes that at least one star in one of the chips is
# saturated; if this does not happen, the satureation level deduced will be
# underestimated.
#-----------------------------------------------------------------------------

import os,sys
import numpy as np
import astropy.io.fits as fits
from scipy.stats import sigmaclip

#-----------------------------------------------------------------------------

print(">> found {:} files to process".format(len(sys.argv)-1))
plot = True

#-----------------------------------------------------------------------------

if plot == True:
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    mpl.rcParams['xtick.direction'] = 'in'
    mpl.rcParams['ytick.direction'] = 'in'
    mpl.rcParams['xtick.top'] = "True"
    mpl.rcParams['ytick.right'] = "True"
    mpl.rcParams['xtick.labelsize'] = 8
    mpl.rcParams['ytick.labelsize'] = 8
    mpl.rcParams['xtick.minor.visible'] = True

#-----------------------------------------------------------------------------

for n in range(1, len(sys.argv)):
    root = sys.argv[n]
    #-----------------------------------------------------------------------------
    # Read the catalog and loop through all tables
    #-----------------------------------------------------------------------------
    try:
        ima = fits.open(root)
    except:
        print("ERROR: can't read {:}".format(root))
        continue

    print("- {:}".format(root))
    if root[0] == 'o':  root = 'weights/'+root.split('/')[1]

    # get weight file
    wgtfile = root.split('.')[0] + '_weight.fits'

    if os.path.isfile(wgtfile):
        wgt = fits.open(wgtfile)
    else:
        wgt =fits.open( '../weights/' + wgtfile)

#-----------------------------------------------------------------------------

    if plot == True:
        fig, axs = plt.subplots(4,4, sharex=True, sharey=True, figsize=(12,8))
        plt.subplots_adjust(hspace=.0, wspace=0)
        nn = 0 # chip counter in plot

    # lists for parameters
    ms = []; sd = []; me = []  # mean sky, stdev, median
    ss = []; bb = []  # min, max, nfinitie

    print("chip  mini   medi    sky   maxi")
    for e in range(1,17):
        # compute chip * weight and get min, max, and "mode"
        arr = np.multiply(ima[e].data, wgt[e].data)
        arr = arr.flatten()/1000.
        arr = arr[arr.nonzero()]
        
        if plot == True:
            nny = int(nn/4) ; nnx = nn - 4*nny 
            hist, edges, pats = axs[nny, nnx].hist(arr, bins=220, range=(0,55), log=True)
            #hist[hist < 0.5] = 0.1                # to avoid plotting issues
            bins = (edges[1:] + edges[:-1])/2.    #; print(edges); print(bins)
            sky,low,upp = sigmaclip(arr, low=3, high=3); msky = np.mean(sky)
            mini = np.min(arr) ; maxi = np.max(arr) ; medi = np.median(arr)
            #idx = np.where(hist == np.max(hist))[0]    ; mode = bins[idx[0]]
            print(" {:2.0f} {:6.2f} {:6.2f} {:6.2f} {:6.2f}".format(e, mini, medi, msky, maxi))
            
            ss.append(mini); bb.append(maxi)
            ms.append(msky); sd.append(np.std(sky)); me.append(medi)

            axs[nny, nnx].annotate('[%0i]'%e, (53,1000000), xycoords='data', ha='right', size=10)
            axs[nny, nnx].annotate('mini: %5.2f'%mini, (55,100000), xycoords='data', ha='right', size=10)
            axs[nny, nnx].annotate('msky: %5.2f'%msky, (55,10000), xycoords='data', ha='right', size=10)
            axs[nny, nnx].annotate('maxi: %5.2f'%maxi, (55,1000), xycoords='data', ha='right', size=10)

#            axs[nny, nnx].set_yscale('log')
            axs[nny, nnx].set_ylim([0.8,1e7])
            axs[nny, nnx].grid(color='grey', ls=':')
            nn += 1
        else:
            print("chip {:2.0f}  {:0.2f}  {:0.2f}".format(e, np.median(arr), np.max(arr)))

    print(" + chip  ", ' '.join(["{:5.0f} ".format(x) for x in np.arange(1,17)]))
    print(" +  min  ", ' '.join(["{:6.2f}".format(x) for x in ss]))
    print(" + medi  ", ' '.join(["{:6.2f}".format(x) for x in me]))
    print(" + msky  ", ' '.join(["{:6.2f}".format(x) for x in ms]))
    print(" +  std  ", ' '.join(["{:6.2f}".format(x) for x in sd]))
    print(" +  max  ", ' '.join(["{:6.2f}".format(x) for x in bb]))

    if plot == True:
        if root[0] == 'w':  root = root.split('/')[1]
#        path = os.getcwd(); ff = path.split('/')[4]
        name = ima[0].header['FILENAME']
        filt = ima[0].header['FILTER']
        fig.suptitle(name +" ("+filt[0]+")", y=0.91)
        outplot = root.split('.')[0]+"_data.png" # ; print(outplot)
        fig.savefig(outplot, bbox_inches="tight")
        plt.clf()
#        fig.close()

    ima.close(); wgt.close()  #; bpm.close()

sys.exit()
#-----------------------------------------------------------------------------

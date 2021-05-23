#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
#
#-----------------------------------------------------------------------------
# Estimate saturation in image and other parameters
#-----------------------------------------------------------------------------
# Multiply image by its weight in order to set to zero unreal values (<0 or very large)
# then build histograme of pixel values > 0 with bins of 500 ADUs, then estimate,
# for each chip:
# - sky level (by sigma-clipping around the median) and its rms
# - saturation value (second highest non-empty bin)
# and finally plot the histograms to root_dataHisto.png, and write estimated
# values to root_dataHisto.dat.
# NB: takes about 20 sec per frame.
# AMo - Oct.20
#-----------------------------------------------------------------------------

import os,sys
import numpy as np
import astropy.io.fits as fits
from scipy.stats import sigmaclip

#-----------------------------------------------------------------------------

plot = True
path = os.getcwd(); 
dire = path.split('/')[-1]  #; print(dire)

if dire != 'images':
    print("### ERROR: not in an 'images' directory ... quitting")
    sys.exit()

Nfiles = len(sys.argv) -1

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
# verbose mode

if Nfiles > 3: verbose = False

if (sys.argv[-1][:3] == 'ver'): 
    verbose = True
    print(" ####  Verbose mode ####")
    Nfiles = Nfiles -1

#-----------------------------------------------------------------------------

print(">> found {:} files to process".format(Nfiles))

for n in range(1, len(sys.argv)):
    #-----------------------------------------------------------------------------
    # Read the catalog and loop through all tables
    #-----------------------------------------------------------------------------
    infile = sys.argv[n] 
    if verbose == True: print("- File {:}".format(infile))

    if Nfiles == 1:
        root = infile.split('.')[0]   # does not contain 'origs/' in name
        wgt = fits.open('weights/'+ root + '_weight.fits')
    else:
        ima = fits.open(infile)
        root = infile.split('origs/')[1]
        root = root.split('.')[0] 
        
    
    pname = root + "_dataHisto.png" 
    if os.path.isfile(pname):
        print("ATTN: {:} already done .. continue".format(pname))
        continue

    ima = fits.open('origs/'  + root + ".fits")
    wgt = fits.open('weights/'+ root + '_weight.fits')

    # prepare output data file
    ofname = root + "_dataHisto.dat"
    ofile = open(ofname, 'w')

    # prepare figure
#    pname = root + "_dataHisto.png" 
    fig, axs = plt.subplots(4,4, sharex=True, sharey=True, figsize=(14,8))
    plt.subplots_adjust(hspace=.0, wspace=0)
    nn = 0 # chip counter in plot

    # lists for output parameters
    ss = []; bb = []  # min, max
    ms = []; sd = []; me = []  # mean sky, stdev, median

    ylim = [0.8,1e7]
    if verbose == True: print("chip  mini   medi   sky   rms    maxi")
    for e in range(1,17):
        # compute chip * weight and get min, max, and "mode"
        arr = np.multiply(ima[e].data, wgt[e].data)
        arr = arr.flatten()/1000.
        arr = arr[arr.nonzero()]
        
        mini = np.min(arr) ; maxi = np.max(arr) ; medi = np.median(arr)
        ss.append(mini); bb.append(maxi); me.append(medi)
        # use sigmaclip to get mean sky level
        sky,low,upp = sigmaclip(arr[(arr < 1.5*medi)], low=3, high=2); msky = np.mean(sky); ssky = np.std(sky)
        ms.append(msky); sd.append(ssky)

        nny = int(nn/4) ; nnx = nn - 4*nny # subplot counter
        hist, edges, pats = axs[nny, nnx].hist(arr, bins=111, range=(-0.25,55.25), log=True)
        bins = (edges[1:] + edges[:-1])/2.    #; print(edges); print(bins)

        # use center of second hisghest bin to estimage saturation
        xx = hist.nonzero()[0]
#        xx = xx[0]  
#        print(xx[-5:]) ; print(bins[xx[-5:]]) ; sys.exit()
#        print(maxi, bins[xx[-2:]])
        maxi = bins[xx[-2]]
        axs[nny, nnx].plot([maxi,maxi], ylim, linestyle='-.', color='r', lw=1)
        axs[nny, nnx].annotate('%0.2f'%maxi, (maxi+0.8,450), xycoords='data', ha='left', va='center', rotation='vertical', size=8)

        # show sky level
        axs[nny, nnx].plot([msky,msky], ylim, linestyle='-.', color='g', lw=1)
        axs[nny, nnx].annotate('%0.2f'%msky, (msky-0.3,ylim[1]*0.1), xycoords='data', ha='right', rotation='vertical', size=8)
        
        # annotate chip number
        axs[nny, nnx].annotate('[%0i]'%e, (58,1000000), xycoords='data', ha='right', size=10)  # chip number

        if nny == 3: axs[nny, nnx].set_xlabel('pixel ADUs / 1000', fontsize=8)
        if nnx == 0: axs[nny, nnx].set_ylabel('Number', fontsize=8)

        axs[nny, nnx].set_ylim(ylim)
        axs[nny, nnx].set_xlim(-4,59)
        axs[nny, nnx].grid(color='grey', ls=':')
        nn += 1
        if verbose == True: print(" {:2.0f} {:6.2f} {:6.2f} {:6.2f} {:5.2f} {:6.2f}".format(e, mini, medi, msky, ssky, maxi))

    # write these stats to file
    ofile.write("chip " + ' '.join(["{:5.0f} ".format(x) for x in np.arange(1,17)]) + "\n")
    ofile.write("min  " + ' '.join(["{:6.2f}".format(x) for x in ss]) + "\n")
    ofile.write("medi " + ' '.join(["{:6.2f}".format(x) for x in me]) + "\n")
    ofile.write("msky " + ' '.join(["{:6.2f}".format(x) for x in ms]) + "\n")
    ofile.write("std  " + ' '.join(["{:6.2f}".format(x) for x in sd]) + "\n")
    ofile.write("max  " + ' '.join(["{:6.2f}".format(x) for x in bb]) + "\n")
    ofile.close() #; print(">> Wrote " + ofname)

    # finalise plot and write it out
    name = ima[0].header['FILENAME']
    filt = ima[0].header['FILTER']
    fig.suptitle(name +" ("+filt[0]+")", y=0.91)
    fig.savefig(pname, bbox_inches="tight")
    plt.clf()

    print(" -- Done: wrote {:} and {:}".format(ofname, pname))
    ima.close(); wgt.close()

sys.exit()
#-----------------------------------------------------------------------------

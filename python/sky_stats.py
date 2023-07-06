#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
#-----------------------------------------------------------------------------
# Study sky level, by chip, in input images (orig files)
#-----------------------------------------------------------------------------
# Images have a significant number of pixels with very large positive or
# negative values, and in some chips many bad pixels with values close to 0 .  
# Also, sky level is chip-dependent. Therefore, for each chip:
# - Read the pixel values, and apply a very generous kappa-sigma clipping to
#   retain the values near the sky
# - determine the mean, std, and median, and read the CASU skylevel kwd
# - write a table with these values for each chip, and add a "descriminator",
#   consisting of the averages of the rms of the sky in chips 1-8 (bottom 
#   of FPA) and 9-16 (top of the FPA), and the ratio top/bottom values. 
#   ==> when that ratio is > 5 there is a major problem
# - plot histograms of the pixel values in a range of 0.5* to 2* mean sky
# NB: by default, if output _stats.dat file already exists, the processing 
#     is skipped; so must delete output files to force reprocessing
#-----------------------------------------------------------------------------

import os,sys
import numpy as np
import astropy.io.fits as fits
from scipy.stats import sigmaclip

import matplotlib as mpl
import matplotlib.pyplot as plt
mpl.rcParams['xtick.direction'] = 'in'
mpl.rcParams['ytick.direction'] = 'in'
mpl.rcParams['xtick.top'] = "True"
mpl.rcParams['ytick.right'] = "True"
mpl.rcParams['xtick.labelsize'] = 8
mpl.rcParams['ytick.labelsize'] = 8
mpl.rcParams['xtick.minor.visible'] = True

doPlot  = True  # to do (or not) the png plot
verbose = False  # print a 1-line "Done" message if False, the stats table if True

#-----------------------------------------------------------------------------

print(">> found {:} files to process".format(len(sys.argv)-1))

#-----------------------------------------------------------------------------
# general params
#-----------------------------------------------------------------------------
kappa = 4                 # why not ....
#-----------------------------------------------------------------------------
# Begin loop over input ldac files
#-----------------------------------------------------------------------------
for n in range(1, len(sys.argv)):
    root = sys.argv[n]

    # to do or not to do
    filename = root.split('.')[0]+ "_sky-stats.dat"  # output data file
    if (os.path.exists(filename)):
        nlines = sum(1 for line in open(filename))
        if int(nlines) == 8:   # this is ok
            isFile = True
        else:
            print("ATTN: remove old file", filename)
            os.remove(filename)
            isFile = False
    else:
        isFile = False

    # also check if plot exists, if not then do it.
    plotname = root.split('.')[0]+ "_sky-histo.png"
    if (os.path.exists(filename)):
        doPlot = False
    else:
        doPlot = True

    if (isFile == True) & (doPlot == False):
        print("# {:} already done ... skipping".format(filename))
        continue

    # Read the file, then loop through the chips
    try:
        ima = fits.open(root)
    except:
        print("ERROR: can't read {:}".format(root))
        continue

    try:
        filt = ima[0].header['FILTER']
    except:
        filt = ima[0].header['HIERARCH ESO INS FILT1 NAME']

    head = "## sky stats for {:}, filter {:}, kappa = {:}".format(root, filt[0], kappa)
    if root[0] == 'o':  wgt = 'weights/'+root.split('/')[1]

#-----------------------------------------------------------------------------

    if doPlot == True:
        fig, axs = plt.subplots(4,4, sharex=True, sharey=True, figsize=(13,9))
        plt.subplots_adjust(hspace=0, wspace=0)

    # lists for parameters
    ms = []; sd = []; me = []  # mean sky, stdev, median
    ss = []; bb = []; lv = []  # min, max, skylevel kwd
    ff = []                    # fraction of pixels outside of kappa-sigma cuts

    ## print("chip    mini     maxi    mean   std   medi    casu [k]")
    for e in range(1,17):
        try:
            slev = ima[e].header['SKYLEVEL']             # casu sky level
        except:
            slev = 1.0

        # compute chip * weight and get min, max, and "mode"
        arr = ima[e].data 
        arr = arr.flatten()
        if (slev != 1.0): 
            anorm = 1000.
        else:
            anorm = 1.

        arr = arr/anorm 
        slev = slev/anorm
        xlims = np.sort([0.5*slev, 2*slev])   # in some rare cases slev is negative!! Use this to avoid crash
        ylims = [0.7,1e7]

        sky,low,upp = sigmaclip(arr[arr > 0], low=kappa, high=kappa)  # try to determine sky level
        msky = np.mean(sky) ; medi = np.median(sky) ; mstd = np.std(sky)    # mean, median, rms
        mini = np.min(sky)  ; maxi = np.max(sky)     # mini, maxi
        
        ss.append(mini); bb.append(maxi)
        ms.append(msky); sd.append(mstd); me.append(medi)
        lv.append(slev); ff.append(100 - 100*len(sky)/2048/2048)

       # fmt=" {:2.0f} {:8.2f} {:8.2f}  {:6.2f} {:6.2f}  {:6.2f}  {:6.2f}"
       # print(fmt.format(e, mini, low, msky, mstd, medi, slev))

        if doPlot == True:
            nn = e-1 ; nnx = nn%4; nny = 3-int(nn/4)   # select plot panel
#            hist, edges, pats = axs[nny, nnx].hist(arr, bins=80, range=(-30.,50), log=True, histtype="step")
            hist, edges, pats = axs[nny, nnx].hist(arr, bins=80, range=(xlims), log=True, histtype="step", label="tt")
            hist[hist < 0.5] = 0.1                # to avoid plotting issues
            bins = (edges[1:] + edges[:-1])/2.    #; print(edges); print(bins)
            axs[nny, nnx].annotate('[%0i]'%e, (0.05,0.85), xycoords='axes fraction', ha='left', size=10)
            axs[nny, nnx].annotate('mean: %5.2f'%msky, (0.98,0.9), xycoords='axes fraction', ha='right', size=8)
            axs[nny,nnx].plot([medi,medi],ylims, linestyle="-.", color='green', lw=1)
            if (nny == 3): axs[nny, nnx].set_xlabel("pixel value [kADU]")
            if (nnx == 0): axs[nny, nnx].set_ylabel("Number")

            axs[nny, nnx].set_ylim(ylims)
            axs[nny, nnx].grid(color='grey', ls=':')
#        else:
#            print("chip {:2.0f}  {:0.2f}  {:0.2f}".format(e, np.median(sky), np.max(sky)))

    # finished loop through extensions ...
    # finish up

    if doPlot == True:
        name = root 
        fig.suptitle(name +" ("+filt[0]+")", y=0.91)
#        outplot =  # ; print(outplot)
        fig.savefig(plotname, bbox_inches="tight")
        plt.close(fig)

    # Print the stats to output data file
    outfile = open(filename, 'w') ; outfile.write(head + "\n")
    string = "chip   " + ' '.join(["{:7.0f}".format(x) for x in np.arange(1,17)])
    outfile.write(string + "\n") 
#    string = " min   " + ' '.join(["{:7.3f}".format(x) for x in ss])
#    outfile.write(string + "\n") 
#    string = " max   " + ' '.join(["{:7.3f}".format(x) for x in bb])
#    outfile.write(string + "\n") 
    string = "mean   " + ' '.join(["{:7.3f}".format(x) for x in ms])
    outfile.write(string + "\n") 
    string = "medi   " + ' '.join(["{:7.3f}".format(x) for x in me])
    outfile.write(string + "\n")
    string = "casu   " + ' '.join(["{:7.3f}".format(x) for x in lv])
    outfile.write(string + "\n")
    string = " std   " + ' '.join(["{:7.3f}".format(x) for x in sd]) 
    outfile.write(string + "\n")
    string = "%out   " + ' '.join(["{:7.3f}".format(x) for x in ff])
    outfile.write(string + "\n")
    # descriminator
    av1=np.mean(sd[:7]); av2=np.mean(sd[8:])  
    if (av2/av1 > 5.): msg="#### PROBLEM CHIPS 9-16 ####"
    else: msg="" 
    string = "desc     <std(bot)>: {:0.3f} , <std(top)>: {:0.3f}  ==>  ratio: {:6.3f}  {:}".format(av1, av2, av2/av1, msg)
    outfile.write(string + "\n")

    # and close file
    outfile.close()  #; print(">> Wrote {:} ... ".format(filename))
    if (verbose == True ):
        os.system("cat "+filename)      # print output stats file
#    else:
#        print("# DONE ", head[3:])

    ima.close(); 

sys.exit()
#-----------------------------------------------------------------------------

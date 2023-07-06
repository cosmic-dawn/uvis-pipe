#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
#
#-----------------------------------------------------------------------------
# Flag saturated objects in ldacs:  
# this version takes the files from the input line; must be run from the
# directory containing the files.  It looks for the original ldac, copies
# it, then flags the saturated source and writes some information kwds in
# the primary header.
#
# v. AMo Oct.20: using flux_max: simple threshold of 0.4 max(flux_max)
# input : ldac files, here the v20*_orig.ldac
# outputs: same file with saturated stars flagged
#        : satcheck.png plot with flux_max vs flux_rad for each chip
#
# Legend:
# - blue: valid sources,
# - yellow: sources flagged by SExtractor (flag > 4; saturated or blended or more)
# - red "x" sources saturated according to this code 
# - The red dot-dashed horizontal line is the saturation threshold,
# - the vertical green dot-dashed line gives the mean Flux_radius for the stars.
#-----------------------------------------------------------------------------

import os,sys
import numpy as np
import astropy.io.fits as fits
from scipy.stats import sigmaclip

path = os.getcwd(); 
dire = path.split('/')[-1]

verbose = False
debug   = False

Nfiles = len(sys.argv) 

if (Nfiles == 1):
    print(" ### ERROR: must give files to process ... ")
    print(" ### SYNTAX:  flag_saturation.py files ")
    sys.exit()

if (Nfiles == 2) & (not os.path.isfile(sys.argv[1])):
    # here the argument contains wildcards that do not resolve into existing files
    print("### ERROR: No files found: probably wildcards do not resolve into existing files - quitting")
    sys.exit(9)


if Nfiles > 3: verbose = False

if (sys.argv[-1][:3] == 'ver'): 
    verbose = True
    print(" ####  Verbose mode ####")
    Nfiles = Nfiles -1

if sys.argv[-1][:3] == 'deb': 
    debug   = True
    verbose = True
    print(" ####  DEBUG mode ####")
    Nfiles = Nfiles -1

print(">> found {:} files to process".format(Nfiles-1))

#-----------------------------------------------------------------------------
#plot = False
plot = True
factor = 0.7             # to convert saturation to threshold
#-----------------------------------------------------------------------------

if plot == True:
    print("   -----  Do Plot  -----")
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
# now for real work:
#-----------------------------------------------------------------------------

for n in range(1, Nfiles):
    fname = sys.argv[n]
    
    pname = fname.split('_orig')[0] + "_satcheck.png"
    if os.path.isfile(pname):
        print("ATTN: {:} already done .. continue".format(pname))
        continue

#    #-----------------------------------------------------------------------------
#    # copy back the original, if it exists
#    #-----------------------------------------------------------------------------
#    fname = orig.split('_orig')[0] + '.ldac'
#    com = "cp -a {:} {:}; chmod 644 {:}".format(orig, fname, fname)
##    print(com); sys.exit()
#    os.system(com)

    #-----------------------------------------------------------------------------
    # Read the catalog and loop through all tables
    #-----------------------------------------------------------------------------
    try:
        cat = fits.open(fname, mode='update')
    except:
        print("ERROR: can't read {:}".format(fname))
        continue

    # get filter from ext 1
    hd1 = cat[1].data[0][0]
    xxx = hd1[np.where(hd1.find('FILTER') == 0)[0]]  ; filt = xxx[0].split()[2][1]

    if plot == True:
#        print(" do Plot ...")
        fig, axs = plt.subplots(4,4, sharex=True, sharey=True, figsize=(14,8))
        plt.subplots_adjust(hspace=0.0, wspace=0.0)

    l_fwhm  = [] # lists for mean fwhm of stars
    l_fmax  = [] # lists for mean flux max
    l_stdev = [] # its stdev
    l_nstrs = [] # num selected as stars
    l_nsat  = [] # num saturated
    l_satur = [] # saturation level above sky backgroud
    l_bgd   = [] # mean background
    l_SatLev = [] # absolute saturation level (for keyword)

    print(">> Begin loop on chips")
    for chip in range(1,17):
        nn = chip - 1
#        nny = int(nn/4) ; nnx = nn - 4*nny 
        nnx = 3 - int(nn/4) ; 
        nny = nn%4 # ; print(chip, nnx,nny)
        
        ext = 2*chip
        # --------- Read needed table columns --------- 
        mag  = cat[ext].data.field("MAG_AUTO")
#        mag  = -2.5 * np.log10(cat[ext].data.field("FLUX_APER"))
        fmax = cat[ext].data.field("FLUX_MAX")   # special name for flagging
        frad = cat[ext].data.field("FLUX_RADIUS")
        flag = cat[ext].data.field("FLAGS")      # special name for flagging
        elli = cat[ext].data.field("ELLIPTICITY")  # 
#        elon = cat[ext].data.field("ELONGATION")  # 
        bgd  = cat[ext].data.field("BACKGROUND") # 

        # --------- find reference value --------- 
        if debug == True: print("DEBUG=0: chip {:} - {:} sources".format(chip,len(fmax)))
        
        satval = np.max(fmax)    # chip saturation value ... simple method
        # take second largest value as reference, if there are two
        big = np.where(fmax > 0.9 * satval)[0]
        sort = np.sort(fmax[big])
        if len(big) >= 2:
            satval = sort[-2]  
        else:
            satval = sort[-1]

        if ((len(big) == 1) & (satval >= factor * 10000)): satval = factor * 10000
            
        # --------- set threshold --------- 
        thresh = factor * satval # chip threshold value
        if thresh <= 10000: thresh = 10000    # do not go below this level
        l_satur.append(thresh)

        # --------- Num objects to flag (above threshold) --------- 
        to_flag = np.where(fmax > thresh)[0]
      #  print(chip, len(flag), len(to_flag))
      #  np.set_printoptions(precision=2); print(np.sort(frad[to_flag]))
        l_nsat.append(len(to_flag))

##        # --------- Now write keywords --------- 
##        if chip == 1: 
##            cat[0].header.set('TH_FACTR', factor, comment="saturation threshold factor")
##        cat[0].header.set('SATUR-{:02n}'.format(chip), np.log10(satval), comment="log(chip saturation level)")
##        cat[0].header.set('THRES-{:02n}'.format(chip), np.log10(thresh), comment="log(chip saturation threshold)")
##
##        # --------- and do actual flagging --------- 
##        print(">> chip {:02n} - flag {:02n} detections".format(chip, len(to_flag)))
        flag[fmax > thresh] = 7

        #-----------------------------------------------------------------------------        
        # now build plot
        #-----------------------------------------------------------------------------
        lt = np.log10(thresh) 
        loc = ((mag < 50) & (fmax > 0)) #; print("--------------- chip", chip, len(loc)) # the valid values
        mag  = mag[loc]
        fmax = fmax[loc]
        frad = frad[loc]
        flag = flag[loc]
        bgd  = np.mean(bgd[loc]) ; l_bgd.append(bgd)  # mean background level
        # estimate best value of seeing 
        # - select the stars (recall: fwhm =~ 2*Frad)
        stars = ((frad > 1) & (frad < 5) & (fmax > 1000) & (fmax < thresh))   
        if debug == True: print("DEBUG-1: {:} sources, {:} stars found ".format(len(frad), len(frad[stars])))
        fwhm, low,upp = sigmaclip(frad[stars], low=2, high=2)
        if debug == True: print("DEBUG-2: {:} sources, {:} stars found ==> {:} selected".format(len(frad), len(frad[stars]), len(fwhm)))
        l_nstrs.append(len(frad[stars]))
        mean_frad = np.mean(fwhm)  ; l_fwhm.append(mean_frad)
        std_frad  = np.std(fwhm)   ; l_stdev.append(std_frad)
        mean_fmax = np.mean(fmax)  ; l_fmax.append(mean_fmax)
        AbsSatLevel = bgd + thresh ; l_SatLev.append(AbsSatLevel)
        debug = True
        if debug == True: print("DEBUG-3: mean fvwm: {:0.2f}, stdev: {:0.2f}".format(mean_frad, std_frad))
        debug = False
        if verbose == True: print(" {:2n}  {:4.2f}  {:5.0f}  {:5.0f}  {:5.0f}".format(chip, mean_frad,  bgd, thresh, bgd+satval))
        cat[0].header.set('MFRAD-{:02n}'.format(chip), mean_frad, comment="typical flux_radius of stars")

        if plot == True:
            val = fmax < thresh
            sat = fmax >= thresh
            fff = flag >= 4
#            ell = frad > 5   # ie, fwhm >~ 2.5
            axs[nny, nnx].plot(frad[val], np.log10(fmax[val]), 'b.', label='valid')
            axs[nny, nnx].plot(frad[fff], np.log10(fmax[fff]), 'y.', label='flag >= 4', markersize=10)
            axs[nny, nnx].plot(frad[sat], np.log10(fmax[sat]), 'rx', label='Fmax > sat',  markersize=3)
#            axs[nny, nnx].plot(frad[ell], np.log10(fmax[ell]), 'r+', label='Frad > 5.0',  markersize=3)
            # threshold
            axs[nny, nnx].plot([0.5,3.89], np.log10([thresh,thresh]), linestyle='-.', color='r', lw=1)
            axs[nny, nnx].annotate("%0.2f"%lt, (4, lt-0.09), xycoords='data', ha='left',  size=8)
            # mean flux_rad
            axs[nny, nnx].plot([mean_frad, mean_frad], [1,5], linestyle='-.', color='g', lw=1.6)
            em = mean_frad - 5*std_frad
            ep = mean_frad + 5*std_frad
            axs[nny, nnx].plot([em, em], [1,5], linestyle='-.', color='g', lw=1)
            axs[nny, nnx].plot([ep, ep], [1,5], linestyle='-.', color='g', lw=1)
            axs[nny, nnx].annotate("%0.2f"%mean_frad, (mean_frad-0.1, 4.5), xycoords='data', ha='right',  size=8)
            # sky background
#            b = np.log10(bgd)
#            axs[nny, nnx].annotate("bgd={:0.2f}, satur={:0.2f}".format(bgd, bgd+thresh), [10,2],  xycoords="data", horizontalcolor='b', size=8)
            
            axs[nny, nnx].set_xlim([-0.3,10.3]); axs[nny, nnx].set_ylim([1.9,4.9])
            axs[nny, nnx].annotate('[%0i]'%chip, (10,4.40), xycoords='data', ha='right', size=10)
            axs[nny, nnx].grid(color='grey', ls=':')
            if nny == 3: axs[nny, nnx].set_xlabel('flux_radius', fontsize=8)
            if nnx == 0: axs[nny, nnx].set_ylabel('log(flux_max)', fontsize=8)
            if chip == 2: axs[nny, nnx].legend(loc='center right', fontsize=8)

        
#    print(">> Finished loop on chips")
    cat.close()

    # strings for output data table
    str_thres = "thresh  " + ' '.join("{:5.0f}".format(x) for x in l_satur) + "\n"
    str_nsat  = "Nsatur  " + ' '.join("{:5.0f}".format(x) for x in l_nsat)  + "\n"
    str_nstrs = "Nstars  " + ' '.join("{:5.0f}".format(x) for x in l_nstrs) + "\n"
    str_fwhm  = "frad    " + ' '.join("{:5.2f}".format(x) for x in l_fwhm)  + "\n"
    str_fmax  = "fmax    " + ' '.join("{:5.0f}".format(x) for x in l_fmax)  + "\n"
    str_stdev = "stdev   " + ' '.join("{:5.2f}".format(x) for x in l_stdev) + "\n"
    str_bgd   = "bgd [k] " + ' '.join("{:5.2f}".format(x/1000) for x in l_bgd)    + " \n"
    str_satlv = "Sat [k] " + ' '.join("{:5.2f}".format(x/1000) for x in l_SatLev) + " \n"
    if verbose == True: print(str_nstrs + str_fwhm + str_fmax + str_stdev + str_satur + str_bgd)

    # Write output data file with
    code = "_satcheck"
    outname = fname.split(".")[0] + code + ".dat"
    outfile = open(outname, 'w')
    outfile.write(str_thres + str_nsat + str_nstrs + str_fwhm + str_fmax + str_stdev + str_bgd + str_satlv)
    outfile.close() ; print(">> Wrote " + outname)
    
    # Now finalize the plot
    if plot == True:
        outplot = fname.split(".")[0] + code + ".png"  
        fig.suptitle(fname +" ("+filt+")", y=0.91)
        if not debug == True: 
            fig.savefig(outplot, bbox_inches="tight")
            print("   - and " + outplot)

        plt.close()


#-----------------------------------------------------------------------------

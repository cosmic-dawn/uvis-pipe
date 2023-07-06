#!/opt/intel/intelpython3-2019.4-088/intelpython3/bin/python
#
#-----------------------------------------------------------------------------
# Flag saturated objects in ldacs:  

# this version takes the files from the input line; must be run from the
# directory containing the files.  I suggest making a copy of the original
# before working on it because the process is not reversible.  The script
# identifies saturated sources as described below and flags them, then writes
# the 'hard' saturation level and the saturation threshold into the extension
# header (SATLEVEL and SATTHRES).

# The flagging process: the ldac files give for each source mag_auto, flux_max,
# flux_radius, fwhm, ellipticity, flags, and background (and more).  They were
# produced by SExtractor using an "initial" saturation threshold of XXX (28000
# in qFits), so any source with a pixel above that level is already flagged
# (recall that flags > 4 indicates saturation).  We plot log(flux_max)
# vs. flux_radius; this will show (a) a vertical locus of stars near the
# nominal value of flux_radius, (b) a horizontal locus of saturated pixel
# values near the "hard" saturation level, and (c) other extended sources with
# usually rather low flux_max.  NB. Flux_max is the source flux only; for the
# original pixel value one must add to it the local background.

# It can occur that there are no saturated sources (typically when seeing is
# large). In this case we set the hard saturation level at 29000.  If ther

# An initial thr

#
# v. AMo Oct.20: using flux_max: simple threshold of 0.4 max(flux_max)
# input : ldac files, here the v20*_orig.ldac
# outputs: same file with saturated stars flagged and without _orig in name
#        : satcheck.png plot with flux_max vs flux_rad for each chip
#-----------------------------------------------------------------------------
# short-cuts
# copy _orig to .ldac : for f in v20*orig.ldac; do cp $f ${f%_orig.ldac}.ldac; done
# copy .ldac to _orig : for f in v20*.ldac; do cp $f ${f%.ldac}_orig.ldac; done

# grep maxFmax v2*satcheck.dat | sed 's/_Y._satcheck.dat:maxFmax/  /'> maxFmax_Y.dat
# for n in $(seq 2 17); do sort -nk$n maxFmax_Y.dat | tail -1 ; done
#-----------------------------------------------------------------------------
#  signal  log10
#----------------
#  32000   4.505
#  31500   4.498
#  31000   4.491
#  30500   4.484
#  30000   4.477
#  29500   4.470
#  29000   4.462
#  28500   4.455
#  28000   4.447
#  27500   4.439
#  27000   4.431
#  26500   4.423
#  26000   4.415
#  25500   4.407
#  25000   4.398
#  24500   4.389
#  24000   4.380
#  23500   4.371
#  23000   4.362
#  22500   4.352
#  22000   4.342
#  21500   4.332
#  21000   4.322
#  20500   4.312
#  20000   4.301
#-----------------------------------------------------------------------------

import os,sys
import numpy as np
import astropy.io.fits as fits
from scipy.stats import sigmaclip
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
#from saturation_sub import *

#-----------------------------------------------------------------------------
def merge_ldac(pyim,list_extent=""):

    # Check which extentions to concatenate
    list_ext = []
    if list_extent != "":
        list = list_extent.split(",")
        try:
            for l in list:
                list_ext.append(int(l))
        except:
            print("Incorrect list of extentions")
            sys.exit(1)
        if max(list_ext) >= next:
            print("List of extentions out of range ... next={:} ... ext list={:}".format(str(next),list_extent))
            sys.exit(1)
    else:
        for i,ext in enumerate(pyim):
            if not "EXTNAME" in ext.header:
                continue
            extname = ext.header["EXTNAME"]
            if extname == "LDAC_OBJECTS":
                #print extname,i
                list_ext.append(i)

    # copy the first header
    t0 = pyim[0]
    hdu0 = t0.copy()

    # Merge it
    list_tables = []
    nrows = 0
    for i in list_ext:
        nrows += pyim[i].data.shape[0]

    t1 = pyim[list_ext[0]]
    hdu = fits.BinTableHDU.from_columns(t1.columns,nrows=nrows)

    nrows_curr = t1.data.shape[0]
    for i in range(len(t1.columns)):
        nrows_curr = t1.data.shape[0]
        for iext in list_ext[1:]:
            nmax = nrows_curr + pyim[iext].data.shape[0]
        
            hdu.data.field(i)[nrows_curr:nmax]=pyim[iext].data.field(i)
            nrows_curr += pyim[iext].data.shape[0]
    hdu.header.set("EXTNAME","LDAC_OBJECTS")

    hdulist = fits.HDUList(hdus=[hdu0,hdu])
    
    return hdulist

#=============================================================================

path = os.getcwd(); 
dire = path.split('/')[-1]
if dire != 'images':
    print("### ERROR: not in an 'images' directory ... ^C to quit")
#    sys.exit()

verbose = False
debug   = False
Force = False
DRY   = False

Nfiles = len(sys.argv) 

if (Nfiles == 1):
    print(" ### ERROR: must give files to process ... ")
    print(" ### SYNTAX:  flag_saturation.py files ")
    sys.exit()

#if (Nfiles == 2) & (not os.path.isfile(sys.argv[1])):
#    # here the argument contains wildcards that do not resolve into existing files
#    print("### ERROR: No files found: check wildcards - quitting")
#    sys.exit(9)

if Nfiles > 3: verbose = False

if (sys.argv[-1][:3] == 'ver'): 
    print(" ####  Verbose  mode ####")
    verbose = True
    Nfiles = Nfiles -1

if sys.argv[-1][:3] == 'deb': 
    print(" ####  DEBUG mode  ####")
    debug   = True
    verbose = True
#    force   = True
    Nfiles = Nfiles -1

if sys.argv[-1][:3] == 'for': 
    print(" ####  FORCE mode  ####")
    force   = True
    Nfiles = Nfiles -1

if sys.argv[-1][:3] == 'dry': 
    print(" ####  DRY mode  ####")
    DRY   = True
    Nfiles = Nfiles -1

print(">> found {:} files to process".format(Nfiles-1))

#-----------------------------------------------------------------------------
doPlot = True
#doPlot = False
factor = 0.60             # to convert saturation to threshold
#-----------------------------------------------------------------------------

if doPlot == True:
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
    fname = sys.argv[n]          #; print(fname)

    #-----------------------------------------------------------------------------
    # Read the ldac catalog and continue setups
    #-----------------------------------------------------------------------------

    try:
        cat = fits.open(fname)
    except:
        print("### ERROR ### file not found {:}".format(fname))
        continue

#    for i,ext in enumerate(cat):
#        if i == 2: print(i, ext.header)
#    sys.exit()

    #-----------------------------------------------------------------------------
    # get filter and paw number from ext 1
    #-----------------------------------------------------------------------------
    hd1 = cat[1].data[0][0]
    xxx = hd1[np.where(hd1.find('FILTER') == 0)[0]]  ; filt = xxx[0][11:12] #.split()[2][1]
    xxx = hd1[np.where(hd1.find('OBJECT') == 0)[0]]  ; paw  = xxx[0][11:15]
    cat.close()

    #-----------------------------------------------------------------------------
    # build name of output files and check if they already exist
    #-----------------------------------------------------------------------------
    root = fname.split('.')[0] 
    pname = root + "_{:}{:}_satcheck".format(filt,paw[3])   # name for png and .dat files
    if (os.path.isfile(pname)) &  (Force != True):
        print("ATTN: {:} already done ... continue".format(pname))
        continue

    print(">> Begin {:} ({:}/{:})".format(fname,filt,paw), end=' ... ')

    # lists to hold values for each chip
    l_fmax   = [] # max of F_max
    l_satval = [] # value of saturation level
    l_thresh = [] # value of saturation level
    l_bgave  = [] # mean background
    l_bgrms  = [] # rms of sky background
    l_nobj   = [] # number of objects
    l_nstrs  = [] # num selected as stars
    l_nsat   = [] # num saturated
    l_frad   = [] # lists for mean frad of stars
    l_stdev  = [] # rms of frad of 'stars'
 
    if doPlot == True:
        fig, axs = plt.subplots(4,4, figsize=(14,14), sharex=True, sharey=True)
        plt.subplots_adjust(hspace=0.0, wspace=0.0)

    #-----------------------------------------------------------------------------
    # now reopen in mode update for real work
    #-----------------------------------------------------------------------------
    cat = fits.open(fname, mode='update')

    #-----------------------------------------------------------------------------
    # Merge all extensions to get overall structure; and attempt to determine 
    # range of stars from histogram of Flux_radius
    #-----------------------------------------------------------------------------
    full = merge_ldac(cat)  #;  print(full[1].columns)
    allfmax = full[1].data.field("FLUX_MAX") + full[1].data.field("BACKGROUND")
    allmag  = full[1].data.field("MAG_AUTO")
    allfrad = full[1].data.field("FLUX_RADIUS")
    allelli = full[1].data.field("ELLIPTICITY")
    allflag = full[1].data.field("FLAGS")

    # build histogram to get location of stars only
    ff,hh = plt.subplots(1,1, figsize=(10,10))
    plt.subplots_adjust(hspace=0.0, wspace=0.0)
    bins = np.arange(2.5, 4.5,.02)
    sel = (allmag < 50) & (allfrad > .8) & (allflag < 4) # reject spurious data
    n_entries = len(allfmax)
    rmin = np.log10(np.min(allfmax)) ; rmax = np.log10(np.max(allfmax))
    bins=25
    # do histogram
    n,b,p = hh.hist(np.log10(allfmax[sel]), bins=bins, histtype='step')  #, range=(2.8,4.5))
    level = 1*np.max(n[-3:])
    # find bins with counts below level and bin(max) < x < bin[-2]
    bc = 0.5*(b[1:] + b[:-1])   # bin centers
    bw = b[4]-b[3]              # bin width

#    print("\n", n)     #############
    stars_min_bin = (np.where(n==np.max(n[:10]))[0]+3)[0]
    stars_max_bin = (np.where(n==np.max(n[-5:]))[0]-3)[-1]
#    print(stars_min_bin, stars_max_bin) ###  ; sys.exit()
    stars_min_fmax = bc[stars_min_bin]
    stars_max_fmax = bc[stars_max_bin]
    level = 0.7*(n[stars_min_bin] + n[stars_max_bin])
#    print(stars_min_fmax, stars_max_fmax, level)   ###########
    bins_stars = (bc > stars_min_bin) & (bc < stars_max_bin) & (n < level) 
    bin_min = stars_min_fmax
    bin_max = stars_max_fmax  #### ;    print(bin_min, bin_max)   ##########
    hh.plot([bin_min,bin_max], [level,level], linestyle='-.', color='peru', lw=1)
    h = [0,level]
    hh.plot([bin_min,bin_min], h, linestyle='-.', color='peru', lw=1)
    hh.plot([bin_max,bin_max], h, linestyle='-.', color='peru', lw=1)
    hh.set_xlabel('log(flux_max)', fontsize=8)
    hh.set_ylabel('Number of sources', fontsize=8)
    outname = root + "_{:}{:}_allstars".format(filt,paw[3])
    ff.savefig(outname, bbox_inches="tight")
#    print("   ####################   QUIT HERE") ; sys.exit()   #####
    #-----------------------------------------------------------------------------
    # begin loop on chips
    #-----------------------------------------------------------------------------
    bgd_rej_thresh = 20000          # backgroud rejection threshold

    for chip in range(1,17):
        if debug == True: print("DEBUG-0: >> chip {:2n}".format(chip))
        nn = chip - 1 ; nny = int(nn/4) ; nnx = nn - 4*nny 
        ext = 2*chip
        noPlot = False            # re-initialize it for each loop

        # --------- Read needed table columns --------- 
        mag  = cat[ext].data.field("MAG_AUTO")
        fmax = cat[ext].data.field("FLUX_MAX")   # special name for flagging
        frad = cat[ext].data.field("FLUX_RADIUS")
        elli = cat[ext].data.field("ELLIPTICITY")
        flag = cat[ext].data.field("FLAGS")      # special name for flagging
        bgd  = cat[ext].data.field("BACKGROUND") # 

        Nobj = len(fmax)   ; l_nobj.append(Nobj)

#        cols = cat[ext].columns ; print(cols.names)  #; cols.info()  ; sys.exit()
#        imaf = cat[ext].data.field("IMAFLAGS_ISO")      # special name for flagging
#        print(flag) ; print(imaf) ; sys.exit
        
        # check for spurious objects, and notify if present; 
        spurious = ((mag > 50) | (fmax < 0))  ; n_spurious = len(mag[spurious])
        if n_spurious > 0:
            print("   ### ATTN ### chip {:} found {:} spurious sources with mag > 50 or fmax < 0".format(chip, n_spurious))

        # No sources found ... hard to imagine, but can happen in bgd is very high
        if Nobj == 0:
            print("   ### PROBLEM ### chip {:2n}: no objects found".format(chip))
            fmax = np.array([2000])      # special bogus valuse to get decent plot
            bgd  = np.array([25500])
            noPlot = True

        # bring fmax up to original pixel values (fmax is stellar flux only)
        fmax = fmax + bgd      ;  l_fmax.append(np.max(fmax))   
        bgd_ave = np.mean(bgd) ;  l_bgave.append(bgd_ave)         # mean bgd and its rms
        bgd_rms = np.std(bgd)  ;  l_bgrms.append(bgd_rms)  

        # --------- find reference value --------- 
        # First: take the sigma-clipped mean of the highest f_max values; 
        big = np.where(fmax > np.max([np.max(fmax)-2000, bgd_rej_thresh]))[0]

        nbig = len(big)
        if nbig > 1:
            sat2, low,upp = sigmaclip(fmax[big], low=2, high=2) 
            sat_ave = np.mean(sat2) ; sat_rms = np.std(sat2) ; len_sat = len(sat2)
        else:
            if bgd[0] == 25500:
                sat_ave = 27500 
            else:
                sat_ave = 27500
            sat_rms = 999; len_sat = 1
            bgd = sat_ave - 2000
            print("   ### ATTN ### chip {:}: no saturated stars, bgd: {:0.0f}".format(chip, bgd))

        satval = sat_ave       ;  l_satval.append(satval)       # "hard" saturation value
        headroom = sat_ave - bgd_ave

        if verbose == True:
            form = ">> chip {:02n} - Satval/rms {:5.0f} / {:3.0f} (log {:0.2f}) using {:2n} stars of {:}"
            print(form.format(chip, sat_ave,sat_rms, np.log10(sat_ave), len_sat, len(fmax[big])))

        # --------- initial threshold --------- 
#        thresh1 = np.max([sat_ave - 0.2*headroom, bgd_rej_thresh])
        thresh1 = 10**bin_max
##        print("++++  SET initial thresh {:0.0f} / {:0.0f} / {:0.0f} / {:0.0f}".format( sat_ave, bgd_ave, headroom, thresh1))

        #-----------------------------------------------------------------------------        
        # Begin selecting ... and build lists of params for output table
        #-----------------------------------------------------------------------------
        # estimate best value of seeing to select the stars proper
        lmin = bgd_ave + 0.1*headroom  #;  lmax = np.max([sat_ave - 0.2*headroom, 26000])
        lmax = 10**bin_max
        stars = ((frad > 0.8) & (frad < 4.) & (fmax > lmin) & (fmax < lmax)) # & (elli <= 0.11 ))   # select the stars
        Nstars = len(frad[stars])
        form = "DEBUG-2: {:} sources, {:} stars found ==> {:} selected"
        if Nstars == 0:
            print("   ### ATTN ### chip {:}: sky level {:0.0f} much too high".format(chip, bgd_ave))
            frad2 = np.array([0]); low = 0; upp = 0 ; bgd_ave = 0.1 ; mean_frad = 0.1
            l_nstrs.append(0)
            noPlot = True
            if debug == True: 
                print(form.format(0,0,0))
        else:
            frad2, low,upp = sigmaclip(frad[stars], low=2.5, high=3.)  # locus of stars
            if debug == True: 
                print(form.format(len(frad2), len(frad[stars]), len(frad2)))
            l_nstrs.append(len(frad2))      # number of stars

        if len(frad2) > 2:
            mean_frad = np.mean(frad2) ; std_frad  = np.std(frad2)
        else:
            mean_frad = 0.1 ; std_frad  = 0
            print("   ### ATTN ### chip {:}: No star with valid frad".format(chip))
            noPlot = True

        if verbose == True:
            print("  Frad: low/mean/upp: {:0.2f}/{:0.2f}/{:0.2f}".format(low,mean_frad,upp))

        l_frad.append(mean_frad) ; l_stdev.append(std_frad)
        if debug == True: print("DEBUG-3: mean Frad: {:0.2f}, stdev: {:0.2f}".format(mean_frad, std_frad))
        if verbose == True: print(">> chip {:02n}  {:4.2f}  {:5.0f}  {:5.0f}  {:5.0f}".format(chip, mean_frad,  bgd_ave, thresh1, satval))
        # and write it to the chip's extension
#        cat[ext].header.set('MFRAD', mean_frad, comment="mean flux_radius of stars")

        # --------- final threshold --------- 
        # Frad significantly larger than for normal star
        lim0 = np.max([1.2*mean_frad, upp])
        # Fmax
        lim1 = sat_ave - 0.1*headroom
        bad = (frad > lim0) & (fmax > lim1)
        if len(fmax[bad]) > 0:
            thresh2 = thresh1  #0.999 * np.min(fmax[bad])
        else:
            thresh2 = thresh1

        if thresh2 <= 0:
            print("### PROBLEM ### negative threshold for for chip {:2n}: {:0.0f}".format(chip, thresh2))
            thresh2 = 1
        l_thresh.append(thresh2)

        # --------- Num objects to flag (above threshold) --------- 
        to_flag = np.where(fmax > thresh2)[0]  #### ; print(to_flag)
        l_nsat.append(len(to_flag))
        
#        if noPlot == True: print(" +++ Skip plot for chip ",chip)
#            continue
                 
        # --------- Now write keywords --------- 
        if chip == 1: 
            cat[0].header.set('TH_FACTR', factor, comment="saturation threshold factor")
        cat[ext].header.set('SATLEVEL', np.int(satval),  comment="hard saturation level")
        cat[ext].header.set('SATTHRES', np.int(thresh2), comment="saturation threshold")
        #-----------------------------------------------------------------------------        
        # now build plot
        #-----------------------------------------------------------------------------
        if (doPlot == True) & (noPlot != True):
            # want to plot original pixel values, thus need to add bgd to ldac values
            val = fmax <  thresh2     # valid
            sat = fmax >= thresh2     # saturated
            fff = flag >= 4

            # data y-range and margins; lims for plotting vertical lines
            yrng = np.log10(np.max(fmax)) - np.log10(bgd_ave) ; ymar = 0.05*yrng
            ylim = [np.log10(np.min(bgd))-ymar, np.log10(np.max(fmax))+ymar]  # relative

            # all data in light grey
            axs[nny, nnx].plot(allfrad, np.log10(allfmax), 'b.', color="lightgrey", markersize=3)   # plot all
            # data for this chip
            lsl = np.log10(sat_ave)
            axs[nny, nnx].plot(frad, np.log10(fmax), 'b.', label='valid')   # plot all
            axs[nny, nnx].plot(frad[fff], np.log10(fmax[fff]), 'y.', label='SE flag > 4', markersize=10)
            axs[nny, nnx].plot(frad[sat], np.log10(fmax[sat]), 'rx', label='saturated', markersize=3)
            axs[nny, nnx].plot([0.5,3.89], [lsl,lsl], linestyle='-.', color='y', lw=1)   # Hard saturation
            axs[nny, nnx].annotate("sat.lev: %0.2f"%lsl, (4.2, lsl), xycoords='data', ha='left', va='center', size=8)
            # threshold
            lt1 = np.log10(thresh1)
            lth = np.log10(thresh2)
            axs[nny, nnx].plot([0.5,3.89], [lt1,lt1], linestyle='-.', color='m', lw=1)
            axs[nny, nnx].plot([0.5,3.89], [lth,lth], linestyle='-.', color='r', lw=1)
            axs[nny, nnx].annotate("th-ini: %0.2f"%lt1, (4.2, lt1), xycoords='data', ha='left', va='center', size=8)
            axs[nny, nnx].annotate("th-fin: %0.2f"%lth, (4.2, lth), xycoords='data', ha='left', va='center', size=8)
#            axs[nny, nnx].scatter([1.2*mean_frad],[np.log10(lim1)], marker='+', s=[5])
#            axs[nny, nnx].plot([1.2*mean_frad],[np.log10(thresh1-2000)], 'g+', markersize=25)  #, s=[5])
            # mean flux_rad
            axs[nny, nnx].plot([mean_frad, mean_frad], ylim, linestyle='-.', color='g', lw=1)
            axs[nny, nnx].plot([low,low], ylim, linestyle=':', color='g', lw=1)
            axs[nny, nnx].plot([upp,upp], ylim, linestyle=':', color='g', lw=1)
            # box of stars
            lmin = np.log10(lmin) ; lmax = np.log10(lmax)
            axs[nny, nnx].plot([low,low], [lmin,lmax], linestyle='-', color='peru', lw=1)
            axs[nny, nnx].plot([upp,upp], [lmin,lmax], linestyle='-', color='peru', lw=1)
            axs[nny, nnx].plot([low,upp], [lmin,lmin], linestyle='-', color='peru', lw=1)
            axs[nny, nnx].plot([low,upp], [lmax,lmax], linestyle='-', color='peru', lw=1)
            lbg = np.log10(bgd_ave)
            axs[nny, nnx].plot([0.5,3.89], [lbg,lbg], linestyle='-', color='g', lw=1)
            axs[nny, nnx].annotate("sky bgd: %0.2f"%lbg, (4.2, lbg), xycoords='data', ha='left', va='center', size=8)
#            axs[nny, nnx].plot([low,upp], [lt1,lt1], linestyle='-', color='g', lw=2)
#            axs[nny, nnx].plot([low,upp], [lth,lth], linestyle='-', color='g', lw=2)
            axs[nny, nnx].annotate("Frad: %0.2f"%mean_frad, (mean_frad-0.9, 0.5*(ylim[0]+ylim[1])), xycoords='data', va='center', rotation=90, size=8)
            
#            axs[nny, nnx].set_ylim(ylim)  # 1.9, 4.9
            axs[nny, nnx].grid(color='grey', ls=':')
            axs[nny, nnx].annotate('[%0i]'%chip, (0.95,0.05), xycoords='axes fraction', ha='right', size=10)

            if nny == 3: axs[nny, nnx].set_xlabel('flux_radius', fontsize=8)
            if nnx == 0: axs[nny, nnx].set_ylabel('log(flux_max)', fontsize=8)
#            if chip == 1: axs[nny, nnx].legend(loc='center right', fontsize=8)

            # insert plot
            inset = axs[nny,nnx].inset_axes([0.6,0.3,0.38,0.3])
            bins = np.arange(4.0,4.52,.05)
            n,b,p = inset.hist(np.log10(fmax), bins=bins, range=[4,ylim[1]], histtype='step', orientation='horizontal')
            axs[nny, nnx].set_xlim([-0.3,10.3]) 

#        print("@@ chip ", chip, "; len(to_flag):", len(to_flag), len(flag))
#        print("@@ to_flag: ", to_flag)
        if (len(to_flag) == 0) | (len(flag) == 0):
            print("  ### ATTN: chip {:}: nothing to flag".format(chip))
            continue
        else:
            flag[to_flag] = 4
        
#    print(">> Finished loop on chips")
    cat.close()

    # strings for output data table
    str_fmax   = "maxFmax " + ' '.join("{:5.0f}".format(x) for x in l_fmax)   + "\n"
    str_satval = "hardSat " + ' '.join("{:5.0f}".format(x) for x in l_satval) + "\n"
    str_thresh = "thresh  " + ' '.join("{:5.0f}".format(x) for x in l_thresh) + "\n"
    str_bgave  = "sky_ave " + ' '.join("{:5.0f}".format(x) for x in l_bgave)  + "\n"
    str_bgrms  = "sky_rms " + ' '.join("{:5.2f}".format(x) for x in l_bgrms)  + "\n"
    str_nobj   = "Nobjs   " + ' '.join("{:5.0f}".format(x) for x in l_nobj)   + "\n"
    str_nstrs  = "Nstars  " + ' '.join("{:5.0f}".format(x) for x in l_nstrs)  + "\n"
    str_nsat   = "Nsatur  " + ' '.join("{:5.0f}".format(x) for x in l_nsat)   + "\n"
    str_frad   = "frad    " + ' '.join("{:5.2f}".format(x) for x in l_frad)   + "\n"
    str_stdev  = "stdev   " + ' '.join("{:5.2f}".format(x) for x in l_stdev)  + "\n"
    if verbose == True: print(str_fmax + str_satval + str_thresh  + str_bgave + str_bgrms + str_nobj + str_nstrs + str_nsat + str_frad)

    # Write output data file with
    outname = pname + ".dat"
    outfile = open(outname, 'w')
    outfile.write(str_fmax + str_satval + str_thresh  + str_bgave + str_bgrms + str_nobj + str_nstrs + str_nsat + str_frad)
    outfile.close() 
    print("wrote " + outname, end='')
    
    # Now finalize the plot
    if doPlot == True:
        outplot = pname+ ".png"  
        fig.suptitle("{:} ({:} - {:})".format(fname,filt,paw), y=0.91)
        fig.savefig(outplot, bbox_inches="tight")
        print(" and " + outplot)

        plt.close()
    else:
        print("")

sys.exit()
#-----------------------------------------------------------------------------

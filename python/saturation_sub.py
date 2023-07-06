
#####!/opt/intel/intelpython2-2019.4-088/intelpython2/bin/python

from stats import *
import numpy as np
import astropy.io.fits as fits
import copy

""" Get the mode of a distribution """
def get_mode(list,binsize):

    mini = min(list)
    maxi = max(list)
    nbin = int((maxi-mini)/binsize)

    (count,bmin,bsize,extra) = lhistogram(list,numbins=nbin)
    maxx = -1000000
    maxxx = 0
    for i in range(len(count)):
        if count[i] > maxx:
            maxx = count[i]
            maxxx = bmin+bsize*(i+0.5)
        #print count[i],bmin+bsize*(i+0.5)
    return maxxx

""" Refine a center of a 2D distribution """
def refine_2D_center(list1,list2,ind,sig,nit):

    l1 = list1[ind]
    l2 = list2[ind]
    l1l2 = l1-l2

    #print len(l1),len(l2),len(l1l2)

    (l1l2_new,new_ind) = sigclip_1D(l1l2,sig,nit)
    
    l1l2_mean = np.average(l1l2_new)
    l2_mean = np.average(l2[new_ind])
    l1_mean = l1l2_mean+l2_mean
    return l1_mean,l2_mean
    
""" Iterative 1D Sigclip """
def sigclip_1D(list,sig,nit):

    list_tmp = np.array([x for x in list])
    for i in range(nit):
        median = np.median(list_tmp)
        disp = np.std(list_tmp)
        new_ind = np.where((list<=median+sig*disp) & (list>=median-sig*disp))
        list_tmp = list[new_ind]
        #print len(list_tmp)
    return (list_tmp,new_ind)


""" Merge all LDAC_OBJECT tables in one """
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

#-----------------------------------------------------------------------------
# Flag a catalog - original version: same value for all chips
#-----------------------------------------------------------------------------
def flag_ldac(pyim, out, key, val, flagval):

    # Check which extentions to flag
    list_ext = []
    for i,ext in enumerate(pyim):
        if not "EXTNAME" in ext.header:
            continue
        extname = ext.header["EXTNAME"]
        if extname == "LDAC_OBJECTS":
            list_ext.append(i)

    # Flag it
#    list_tables = []
    nrows = 0
    for i in list_ext:
        hdu = pyim[i]
        
        hdu0 = 0
        try:
        #    print hdu.data.field("IMAFLAGS_ISO")[0]
            hdu0 = hdu
        except:
            ndata = len(hdu.data)
            #print ndata
            c1 = fits.Column(name='IMAFLAGS_ISO',format='I',array=np.zeros(ndata),disp='I3')
            hdu0 = fits.BinTableHDU.from_columns(hdu.columns+c1)

        key = "FLUX_MAX"
        ind0 = np.where(hdu0.data.field(key) >= val)

        hdu0.data.field("FLAGS")[ind0] = 4

        hdu0.header.set('EXTNAME','LDAC_OBJECTS')
        pyim[i] = copy.copy(hdu0)

    pyim.writeto(out)
    pyim.close()

#-----------------------------------------------------------------------------
# Flag a catalog - new version, simplified: chip-dependent saturation level
# - loops through the 'LDAC_OBJECTS' extensions and sets the 'FLAGS' keyword
#   to 7 (nice little prime number that cannot occur otherwise) if 'FLUX_MAX'
#   >= a threshold value determined internally for that chip
# ATTN: input is a fits object; writes the updated LDAC (FITS) catalog
#-----------------------------------------------------------------------------
def flag_ldac_bychip(pyim):

    # get filename and build name of output ldac file
    hd  = pyim[1].data[0][0] 
    xxx = hd[np.where(hd.find('FITSFILE') == 0)]
    out = xx.split()[1][2:16] + "_flagged.ldac"

    key = "FLUX_MAX"  # keyword to use
    flg = "FLAGS"     # keyword to flag
    

    # Check which extentions to flag
#    list_ext = []
#    for i,ext in enumerate(pyim):
#        if not "EXTNAME" in ext.header:
#            continue
#        extname = ext.header["EXTNAME"]
#        if extname == "LDAC_OBJECTS":
#            list_ext.append(i)
#
#    # Flag it
#    list_tables = []

    for i in range(2,33,2):
        hdu = pyim[i]
#        try:
#        #    print hdu.data.field("IMAFLAGS_ISO")[0]
#            hdu0 = hdu
#        except:
#            ndata = len(hdu.data)
#            #print ndata
#            c1 = fits.Column(name='IMAFLAGS_ISO',format='I',array=np.zeros(ndata),disp='I3')
#            hdu0 = fits.BinTableHDU.from_columns(hdu.columns+c1)
        fmax = hdu.data.field(key)
        val = 0.6 * np.max(fmax)                      # values to flag
        sat = np.where(hdu.data.field(key) >= val)   # indices to flag

        hdu.data.field("FLAGS")[sat] = 7
        hdu.header.set('EXTNAME','LDAC_OBJECTS')
        pyim[i] = copy.copy(hdu)

    pyim.writeto(out)
    pyim.close()

#-----------------------------------------------------------------------------

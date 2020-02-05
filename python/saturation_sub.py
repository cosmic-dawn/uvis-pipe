from stats import *
import numpy
import astropy.io.fits as pyfits
import copy

def get_mode(list,binsize):
    """ Get the mode of a distribution """

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

def refine_2D_center(list1,list2,ind,sig,nit):
    """ Refine a center of a 2D distribution """

    l1 = list1[ind]
    l2 = list2[ind]
    l1l2 = l1-l2

    #print len(l1),len(l2),len(l1l2)

    (l1l2_new,new_ind) = sigclip_1D(l1l2,sig,nit)
    
    l1l2_mean = numpy.average(l1l2_new)
    l2_mean = numpy.average(l2[new_ind])
    l1_mean = l1l2_mean+l2_mean
    return l1_mean,l2_mean
    
def sigclip_1D(list,sig,nit):
    """ Iterative 1D Sigclip """

    list_tmp = numpy.array([x for x in list])
    for i in range(nit):
        median = numpy.median(list_tmp)
        disp = numpy.std(list_tmp)
        new_ind = numpy.where((list<=median+sig*disp) & (list>=median-sig*disp))
        list_tmp = list[new_ind]
        #print len(list_tmp)
    return (list_tmp,new_ind)

def merge_ldac(pyim,list_extent=""):
    """ Merge all LDAC_OBJECT tables in one """

    # Check which extentions to concatenate
    list_ext = []
    if list_extent != "":
        list = list_extent.split(",")
        try:
            for l in list:
                list_ext.append(int(l))
        except:
            print "Incorrect list of extentions"
            sys.exit(1)
        if max(list_ext) >= next:
            print "List of extentions out of range ... next=%s ... ext list=%s" % (str(next),list_extent)
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
        ###if pyim[i].data == None:            continue
        nrows += pyim[i].data.shape[0]

    t1 = pyim[list_ext[0]]
    hdu = pyfits.BinTableHDU.from_columns(t1.columns,nrows=nrows)

    nrows_curr = t1.data.shape[0]
    for i in range(len(t1.columns)):
        nrows_curr = t1.data.shape[0]
        for iext in list_ext[1:]:
            ###if pyim[iext].data == None:                continue
            nmax = nrows_curr + pyim[iext].data.shape[0]
        
            hdu.data.field(i)[nrows_curr:nmax]=pyim[iext].data.field(i)
            nrows_curr += pyim[iext].data.shape[0]
    hdu.header.set("EXTNAME","LDAC_OBJECTS")

    hdulist = pyfits.HDUList(hdus=[hdu0,hdu])
    
    return hdulist

def flag_ldac(pyim,out,key,val,flagval):
    """ Flag a catalog """

    # Check which extentions to flag
    list_ext = []
    for i,ext in enumerate(pyim):
        if not "EXTNAME" in ext.header:
            continue
        extname = ext.header["EXTNAME"]
        if extname == "LDAC_OBJECTS":
            #print extname,i
            list_ext.append(i)

    # Flag it
    list_tables = []
    nrows = 0
    for i in list_ext:
        hdu = pyim[i]
        #if hdu.data == None:       continue
        
        hdu0 = 0
        try:
            print hdu.data.field("IMAFLAGS_ISO")[0]
            hdu0 = hdu
        except:
            #print "Add IMAFLAGS_ISO"
            ndata = len(hdu.data)
            #print ndata
            c1 = pyfits.Column(name='IMAFLAGS_ISO',format='I',array=numpy.zeros(ndata),disp='I3')
            hdu0 = pyfits.BinTableHDU.from_columns(hdu.columns+c1)

        if key == "FLUX_MAX":
            ind0 = numpy.where(hdu0.data.field(key) >= val)
        elif key == "MU_MAX":
            ind0 = numpy.where(hdu0.data.field(key) <= val)
        hdu0.data.field("IMAFLAGS_ISO")[ind0] += flagval

        hdu0.data.field("FLAGS")[ind0] = 4

        hdu0.header.set('EXTNAME','LDAC_OBJECTS')
        pyim[i] = copy.copy(hdu0)


    pyim.writeto(out)
    pyim.close()

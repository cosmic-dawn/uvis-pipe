#
# -----------------------------------------------
# Flag saturated object using MU_MAX-MAG diagram
# input : c,catalog  : input catalog or file
#	: o,output   : output catalog
# -----------------------------------------------

from ASCII_cat import *
import os,sys
from stats import *
import numpy
from saturation_sub import *
from optparse import OptionParser
import astropy.io.fits as pyfits

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import matplotlib.cbook as cbook
import matplotlib.ticker as ticker


parser = OptionParser()
parser.add_option('-c','--catalog',dest='catalog',help='Input catalog', type='string', default="")
parser.add_option('-o','--output', dest='output',help='Output catalog', type='string', default="")
parser.add_option('-m','--magkey', dest='magkey',help='Magnitude keyword (def = MAG_AUTO)', type='string', default="MAG_AUTO")
parser.add_option('-f','--flagval',dest='flagval',help='ISOLFLAG values (def = 256)', type='int', default=256)

parser.add_option('--fluxmax',dest='fluxmax',help='Saturation fluxmax value (def = no limit)', type='int',default=0)
parser.add_option('--noplot', dest='noplot',help='Do not produce check plots (default : plot)', action='store_true', default=False)

ismumax = 0

try:
    options,args = parser.parse_args(sys.argv[1:])
except:
    print "Error ... check usage with flag_saturation.py -h "
    sys.exit(1)

trash = []

# Check input catalog
if not os.path.isfile(options.catalog):
    print "Impossible to find the input catalog ..."
    sys.exit(1)

if options.output == "":
    options.output = options.catalog.split(".ldac")[0]+"_noSAT.ldac"
if os.path.isfile(options.output):
    print "-- Output file already exists !"
    sys.exit(1)

### Tuning parameters ###########
central_yratio = 0.6
cental_width = 0.5
nsig_center = 2.0
nsig_nit = 2

width_ylimits = [0.70,0.30]
width_ybin = 0.5
quartile = 0.93

above_factor = 1.2
abovehisto_binsize = 0.25
maxhist_sweepthresh = 0.25

final_thresh_expand = 0.1
################################

# Read the catalog
pyim = pyfits.open(options.catalog)

if not (options.fluxmax != 0 and options.noplot):
    # Merge all extentions 
    hdulist = merge_ldac(pyim)

    # Get the mag / mu lists
    fluxmax = hdulist[1].data.field("FLUX_MAX")
    rem = len(fluxmax[fluxmax < 0])
    if ( rem > 0):
        print("## Attn: remove {:} negative FLUX_MAX values of {:}".format(rem, len(fluxmax)))
    loc = fluxmax >= 0   # find possible neg values and remove them
    fluxmax = fluxmax[loc]
    mumax = -2.5* numpy.log10(fluxmax)

    mag = hdulist[1].data.field(options.magkey)
    mag = mag[loc]

##    # Get mumax 
##    if "MU_MAX" in hdulist[1].columns.names:
##        mumax = hdulist[1].data.field("MU_MAX")
##        ismumax = 1
##    elif "FLUX_MAX" in hdulist[1].columns.names:
##    else:
##        print "Impossible to find MU_MAX or FLUX_MAX ..."
##        sys.exit(1)
    
    try:
        rh = [x[1] for x in hdulist[1].data.field("FLUX_RADIUS")]
    except:
        rh = hdulist[1].data.field("FLUX_RADIUS")

    rh = rh[loc]
    mu__mag = mumax - mag

    # Put the data in a structured array
    ngood = len(numpy.where(mumax < 90.0)[0])
    
    type_data = numpy.dtype([('index', numpy.float32),('rh', numpy.float32), ('mag', numpy.float32), ('mumax', numpy.float32), ('fluxmax', numpy.float32),('mu__mag', numpy.float32)])
    data = numpy.zeros(ngood,dtype=type_data)
    i0 = -1
    for i,rh0,mag0,mumax0,fluxmax0,mu__mag0 in zip(range(len(rh)),rh,mag,mumax,fluxmax,mu__mag): 
        if mumax0 < 90.0:
            i0 += 1
            data[i0] = (i0,rh0,mag0,mumax0,fluxmax0,mu__mag0)

    # Get some limits in mu / mag
    mumax_mode = get_mode(data['mumax'],0.2)
    mumax_min = min(data['mumax'])
    mumax_max = max(data['mumax'])

    #print mumax_min,mumax_max,mumax_mode
    if mumax_min < mumax_mode - 10.0:
        print "-- Problem with min mumax ", mumax_min
    if mumax_max > mumax_mode + 10.0:
        print "-- Problem with min mumax ", mumax_max
    
    # Get some limits in mu-mag
    mu__mag_mode = get_mode(data['mu__mag'], 0.2)


# Fixed saturation level ?
if options.fluxmax != 0:
    # ---- Flag the objects in the catalog ------
    pyim = pyfits.open(options.catalog)
    flag_ldac(pyim,options.output,"FLUX_MAX", options.fluxmax, 256)

    # ---- Plot the results ----
    if not options.noplot:
        fig = plt.figure(1,figsize=(15,6))
        data1 = data[numpy.where(data['mag'] < 30.0)]
        data_sat = data1[numpy.where(data1['fluxmax'] > options.fluxmax)]
    
##        # Mumax/mag
##        ax1 = fig.add_subplot(121)
##        ax1.plot(data1['mag'],data1['mumax'], 'r.')
##        ax1.plot(data_sat['mag'],data_sat['mumax'], 'b.')
##        plt.xlabel('MAG')
##        plt.ylabel('MU_MAX')
##        
##        fig.savefig("test.png")
        
        # rh/mag
        ax2 = fig.add_subplot(122)
        ax2.plot(data1['rh'],data1['mag'], 'r.')
        ax2.plot(data_sat['rh'],data_sat['mag'], 'b.')
        ax2.set_xlim([0, 10.0])
        plt.xlabel('MAG')
        plt.ylabel('RH (px)')
        
        outplot = options.output.split(".ldac")[0]+"_satcheck.png"
        fig.savefig(outplot)
        plt.clf()


    sys.exit(0)


# --------- get central point in mumax_mag -----------
central_y = mumax_mode - (mumax_mode-mumax_min)*central_yratio

sample_central_ind = numpy.where((mumax < central_y+cental_width) & (mumax>central_y-cental_width)) 

central_xc1 = numpy.median(mag[sample_central_ind])
central_xc2 = get_mode(mag[sample_central_ind],0.1)
central_yc = numpy.median(mumax[sample_central_ind])

###print "Central point : ",central_xc1,central_xc2,central_yc

# refine with a sigclipped mean
(xc0,yc0) = refine_2D_center(mag,mumax,sample_central_ind,nsig_center,nsig_nit)
###print "Refined Central point : ",xc0,yc0

mu__mag_center = yc0 - xc0
# -----------------------------------------------------------

# --- Get the width of the stellar branch at several mumax ---
width_ylimits_mumax = [mumax_mode - (mumax_mode-mumax_min)*x for x in width_ylimits]

res_width = {}
miny = width_ylimits_mumax[0]-width_ybin
maxy = width_ylimits_mumax[0]
last = 1
n = 0
###print "Get width in slices between %s and %s:" % (str(width_ylimits_mumax[0]),str(width_ylimits_mumax[1]))
starbranch_width_mu__mag_vs_mumax = {}
while last:
    n += 1
    miny +=     width_ybin
    maxy +=     width_ybin
    #print "    -- slice : ",miny,maxy

    # Last slice
    if maxy+width_ybin > width_ylimits_mumax[1]:
        last = 0
        maxy = width_ylimits_mumax[1]
    
    # Get the sample
    ind_slice = numpy.where((mumax<maxy) & (mumax>miny))
    #print "    -- nobjects: ",len(ind_slice[0])

    # Get the mag of the XX%th element
    data_slice = data[numpy.where((data['mumax']<maxy) & (data['mumax']>miny))]
    sname=options.catalog.split(".ldac")[0]+"_slice_"+str(n)
    #print "    -- slice name: ", sname

    file = open(sname,'w')
    trash.append(sname)
    for m1,m2 in zip(data_slice['mumax'],data_slice['mag']):
        file.write("%s %s %s \n" % (str(m1),str(m2),str(m1-m2)))
    file.close()

    # Get the quartile in mumax
    data_slice95 = numpy.sort(data_slice,order='mu__mag')[int(len(data_slice)*(1.0-quartile)):int(len(data_slice)*(quartile))]
    
    mumax_mean = numpy.mean(data_slice95['mumax'])
    mumax_disp = numpy.std(data_slice95['mumax'])
    
    miny -= mumax_disp
    maxy += mumax_disp
    data_sliceb = data[numpy.where((data['mumax']<maxy) & (data['mumax']>miny))]
    data_sliceb95 = numpy.sort(data_sliceb,order='mu__mag')[int(len(data_sliceb)*(1.0-quartile)):int(len(data_sliceb)*(quartile))]

    # store the width of the stellar branch at different magnitudes
    starbranch_width_mu__mag_vs_mumax[mumax_mean] = [data_sliceb95['mu__mag'][0],data_sliceb95['mu__mag'][-1]]

# -----------------------------------------------------------
# --------- Get the objects above the stellar branch --------

thresh = min([x[1] for x in starbranch_width_mu__mag_vs_mumax.values()]) * above_factor

data_tmp = numpy.sort(data,order='mu__mag')
data_above = data_tmp[numpy.where(data_tmp['mu__mag'] > thresh)]

# -----------------------------------------------------------

# --------- Get a weighted histogramm -----------------------
min = int(min(data_above['mumax'])) -1.0
max = int(max(data_above['mumax'])) +1.0
nbin = int((max-min)/abovehisto_binsize+1)

datax = numpy.array([min+x*abovehisto_binsize for x in range(nbin)])
datay = numpy.array([0]*nbin)
for mumax,mag,mumag in zip(data_above['mumax'],data_above['mag'],data_above['mu__mag']):

    for i,xx in enumerate(datax):
        if mumax >= xx and mumax < xx+abovehisto_binsize:
            datay[i] += mumag
# -----------------------------------------------------------

# ------------- Check plots ---------------
##if not options.noplot:
##    fig = plt.figure(1,figsize=(15,6))
##
##    # Mumax/mag
##    ax1 = fig.add_subplot(121)
##    ax1.plot(data['mag'],data['mumax'], 'r.')
##    ax1.plot(data_above['mag'],data_above['mumax'], 'b.')
##    plt.xlim(xmax=-6.0)
##    fig.show()
##    fig.savefig("test.png")
##    
##    # rh/mag
##    ax2 = fig.add_subplot(122)
##    ax2.plot(data['rh'],data['mag'], 'r.')
##    ax2.plot(data_above['rh'],data_above['mag'], 'b.')
##    ax2.set_xlim([0,10.0])
##    plt.xlim(xmax=-6.0)
##    fig.show()
##    fig.savefig("above.png")
##    plt.clf()
##    
##    # mumax histo
##    n = len(numpy.where(datax<yc0)[0])
##    max = numpy.max(datay[numpy.where(datax<yc0)])
##
##    fig1 = plt.figure(1,figsize=(15,6))
##    ax11 = fig1.add_subplot(111)
##    ax11.plot(datax,datay, 'b-o')
##    ax11.set_ylim([0,max*3.0])
##    fig1.savefig("histo_above.png")
##    plt.clf()

# -----------------------------------------

# create structured array
type_data = numpy.dtype([('mumax', numpy.float32),('val', numpy.float32)])
data_histo = numpy.zeros((len(datax),),dtype=type_data)
for i,x,y in zip(range(len(datax)),datax,datay):
    data_histo[i] = (x,y)

# --------- Get the mumax limit for saturated objects -----------
maxhist = numpy.max(datay[numpy.where(datax<yc0)])
maxhist_ind = numpy.where(datay == maxhist)[-1][0]
mumode_ind = numpy.where(datax<mumax_mode)[0][-1]

minhist = numpy.min(data_histo['val'][maxhist_ind:mumode_ind])
minhist_ind0 = numpy.where(data_histo['val'][maxhist_ind:mumode_ind]==minhist)
minhist_ind = minhist_ind0[0][0]+maxhist_ind

new_minhist_ind = minhist_ind
for i in range(minhist_ind-maxhist_ind):
    val = data_histo['val'][minhist_ind-i]
    if val < maxhist* maxhist_sweepthresh:
        new_minhist_ind = minhist_ind-i

final_thresh = data_histo['mumax'][new_minhist_ind] + final_thresh_expand


if not options.noplot:
    # ------------- Check plots ---------------
    fig = plt.figure(1,figsize=(15,6))
    
    data_sat = data[numpy.where(data['mumax']<final_thresh)]
    data_sat2 = data_sat[numpy.where(data_sat['mag'] < xc0 + 20.0)]
    
    
    data1 = data[numpy.where(data['mag'] < xc0 + 20.0)]
    
    # Mumax/mag
    ax1 = fig.add_subplot(121)
    ax1.plot(data1['mag'],data1['mumax'], 'r.')
    ax1.plot(data_sat2['mag'],data_sat2['mumax'], 'b.')
    plt.xlabel('MAG')
    plt.ylabel('MU_MAX')
    
    # rh/mag
    ax2 = fig.add_subplot(122)
    ax2.plot(data1['rh'],data1['mag'], 'r.')
    ax2.plot(data_sat2['rh'],data_sat2['mag'], 'b.')
    ax2.set_xlim([0,10.0])
    plt.xlabel('MAG')
    plt.ylabel('RH (px)')
    
    outplot = options.output.split(".ldac")[0]+"_checkplot.png"
    fig.savefig(outplot)
    plt.clf()
    
    # -----------------------------------------

pyim.close()

# ---- Flag the objects in the catalog ------
pyim= pyfits.open(options.catalog)
if ismumax:
    flag_ldac(pyim,options.output,"MU_MAX",final_thresh,256)
else:
    flag_ldac(pyim,options.output,"FLUX_MAX",10**(-final_thresh/2.5),256)


print ">> Done flag_saturation in "+options.catalog+"; threshold is MU_MAX: ", final_thresh

for f in trash:
    if os.path.isfile(f):
        os.system("rm -f "+f)

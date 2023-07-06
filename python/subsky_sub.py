#!/usr/bin/env python

import astropy.io.fits as pyfits
import os
import copy
import math
from TMASS_lib import *
import numpy
import numpy.random
from multiprocessing import Pool
numpy.set_printoptions(precision=2)

# General values / Configuration parameters ...

class bertin_param:
    # One pass

    # mask creation
    sex1param = {'CATALOG_TYPE': 'NONE', 'DEBLEND_MINCONT': 1, 'CLEAN': 'N', 'DETECT_MINAREA': '3', 'WEIGHT_TYPE': 'MAP_WEIGHT', 'CHECKIMAGE_TYPE': 'OBJECTS',  'BACK_SIZE': '64', 'BACK_FILTERSIZE': '1',    'MEMORY_PIXSTACK': '10000000'}
    sex2param = {'CATALOG_TYPE': 'NONE', 'DEBLEND_MINCONT': 1, 'CLEAN': 'N', 'WEIGHT_TYPE': 'MAP_WEIGHT', 'CHECKIMAGE_TYPE': 'BACKGROUND', 'BACK_SIZE': '4,2048', 'BACK_FILTERSIZE': '3',    'MEMORY_PIXSTACK': '10000000'}
    sex3param = {'CATALOG_TYPE': 'NONE', 'DEBLEND_MINCONT': 1, 'CLEAN': 'N', 'DETECT_MINAREA': '2048', 'WEIGHT_TYPE': 'NONE', 'CHECKIMAGE_TYPE': 'OBJECTS' ,  'BACK_SIZE': '64', 'BACK_FILTERSIZE': '1,11', 'DETECT_THRESH': '4.', 'ANALYSIS_THRESH': '4.', 'MEMORY_PIXSTACK': '10000000', 'FILTER': 'N'}

    # Weightwatcher
    ww0param  = {'FLAG_NAMES': '""', 'POLY_NAMES': '""', 'OUTFLAG_NAME': '""', 'WEIGHT_MIN': '0.5,-0.5', 'WEIGHT_MAX': '1.5,1.e-15', 'WEIGHT_OUTFLAGS': '1,1'}
    ww1param  = {'FLAG_NAMES': '""', 'POLY_NAMES': '""', 'OUTFLAG_NAME': '""', 'WEIGHT_MIN': '0.5,-0.5', 'WEIGHT_MAX': '1.5,1.e-15', 'WEIGHT_OUTFLAGS': '1,1'}
    ww12param = {'FLAG_NAMES': '""', 'POLY_NAMES': '""', 'OUTFLAG_NAME': '""', 'WEIGHT_MIN': '0.5,0.5',  'WEIGHT_MAX': '1.5,1.5',    'WEIGHT_OUTFLAGS': '1,1'}
    ww2param  = {'FLAG_NAMES': '""', 'POLY_NAMES': '""', 'OUTFLAG_NAME': '""', 'WEIGHT_MIN': '0.5,-0.5,-0.5','WEIGHT_MAX': '1.5,1.e-15,1.5', 'WEIGHT_OUTFLAGS': '1,1,1'}

    swarpproj1param = {'WEIGHTOUT_NAME': 'temp.weight.fits', 'WEIGHT_TYPE': 'NONE', 'GAIN_DEFAULT': '0.0', 'SUBTRACT_BACK': 'N', 'RESAMPLING_TYPE': 'NEAREST', 'FSCALASTRO_TYPE': 'NONE', 'FSCALE_KEYWORD': 'NONE', 'VMEM_MAX': '4095', 'MEM_MAX': '512'}

    # skysubtraction
    swarpsky1param = {'WEIGHTOUT_NAME': 'sky.weight.fits', 'RESAMPLING_TYPE': 'NEAREST', 'COMBINE_TYPE': 'MEDIAN','COPY_KEYWORDS': 'OBJECT,FILTER', 'WEIGHT_TYPE': 'MAP_WEIGHT', 'SUBTRACT_BACK': 'Y', 'BACK_SIZE': '4096'}
    swarpsky2param = {'RESAMPLING_TYPE': 'NEAREST', 'COMBINE_TYPE': 'SUM', 'WEIGHT_TYPE': 'NONE', 'GAIN_DEFAULT': '0.0','SUBTRACT_BACK': 'N', 'RESAMPLE': 'N', 'FSCALASTRO_TYPE': 'NONE', 'FSCALE_KEYWORD': 'NONE', 'FSCALE_DEFAULT': '-1,1'}

    # Background subtaction
    sexback1param = {'CATALOG_TYPE': 'NONE', 'CHECKIMAGE_TYPE': '-BACKGROUND,MINIBACKGROUND', 'BACK_SIZE': '2048', 'BACK_FILTERSIZE': '3', 'WRITE_XML': 'Y'}

    def get_verbose(self, par, options):
	try:
	    if options.verbose:
		par['VERBOSE_TYPE'] = "NORMAL"
	    else:
		par['VERBOSE_TYPE'] = "QUIET"
	except AttributeError:
	    try:
		if options.verbose_level in ["INFO","DEBUG"]:
		    par['VERBOSE_TYPE'] = "NORMAL"
		else:
		    par['VERBOSE_TYPE'] = "NORMAL"
	    except AttributeError:
		print "Incorrect verbose options"
		sys.exit(1)

    def get_verbose2(self, par, options):
	try:
	    if options.verbose:
		par['VERBOSE_TYPE'] = "NORMAL"
	    else:
		par['VERBOSE_TYPE'] = "QUIET"
	except AttributeError:
	    try:
		if options.verbose_level in ["INFO","DEBUG"]:
		    par['VERBOSE_TYPE'] = "NORMAL"
		else:
		    par['VERBOSE_TYPE'] = "NORMAL"
	    except AttributeError:
		print "Incorrect verbose options"
		sys.exit(1)

    def get_sex1param(self, options):
	sex1param0 = copy.copy(self.sex1param)

	sex1param0['c'] = options.cpath + "/sex.conf"
	sex1param0['DETECT_THRESH'] = options.thresh
	sex1param0['FILTER_NAME'] = options.cpath + "/gauss_3.0_7x7.conv"
	sex1param0['WEIGHT_IMAGE'] = options.mask_file
	sex1param0['PARAMETERS_NAME'] = options.cpath + "/tt.param"
	self.get_verbose(sex1param0, options)
	return sex1param0

    def get_sex2param(self, options):
	sex2param0 = copy.copy(self.sex2param)

	sex2param0['c'] = options.cpath + "/sex.conf"
	sex2param0['FILTER_NAME'] = options.cpath + "/gauss_3.0_7x7.conv"
	sex2param0['PARAMETERS_NAME'] = options.cpath + "/tt.param"
	self.get_verbose(sex2param0, options)
	return sex2param0

    def get_sex3param(self, options):
	sex3param0 = copy.copy(self.sex3param)

	sex3param0['c'] = options.cpath + "/sex.conf"
	sex3param0['PARAMETERS_NAME'] = options.cpath + "/tt.param"
	self.get_verbose(sex3param0, options)
	return sex3param0

    def get_ww1param(self, options):
	ww1param0 = copy.copy(self.ww1param)

	ww1param0['c'] = options.cpath + "/ww.conf"
	ww1param0['WEIGHT_NAMES'] = options.mask_file
	self.get_verbose(ww1param0, options)
	return ww1param0

    def get_ww12param(self, options):
	ww1param0 = copy.copy(self.ww12param)

	ww1param0['c'] = options.cpath + "/ww.conf"
	ww1param0['WEIGHT_NAMES'] = options.mask_file
	self.get_verbose(ww1param0, options)
	return ww1param0

    def get_ww12bparam(self, options):
	ww1param0 = copy.copy(self.ww12bparam)

	ww1param0['c'] = options.cpath + "/ww.conf"
	ww1param0['WEIGHT_NAMES'] = options.mask_file
	self.get_verbose(ww1param0, options)
	return ww1param0

    def get_ww2param(self, options):
	ww2param0 = copy.copy(self.ww2param)

	ww2param0['c'] = options.cpath + "/ww.conf"
	self.get_verbose(ww2param0, options)
	return ww2param0

    def get_swarpproj1param(self, options):
	swarpproj1param0 = copy.copy(self.swarpproj1param)

	swarpproj1param0['c'] = options.cpath + "/swarp.conf"
	self.get_verbose2(swarpproj1param0, options)
	return swarpproj1param0

    def get_swarpsky1param(self, options):
	swarpproj1param0 = copy.copy(self.swarpsky1param)

	swarpproj1param0['c'] = options.cpath + "/swarp.conf"
	swarpproj1param0['WEIGHT_SUFFIX'] = options.outweight_suf
	self.get_verbose2(swarpproj1param0, options)
	return swarpproj1param0

    def get_swarpsky2param(self, options):
	swarpproj1param0 = copy.copy(self.swarpsky2param)

	swarpproj1param0['c'] = options.cpath + "/swarp.conf"
	self.get_verbose2(swarpproj1param0, options)
	return swarpproj1param0

    def get_sexback1param(self, options):
	sexback1param0 = copy.copy(self.sexback1param)

	sexback1param0['c'] = options.cpath + "/sex.conf"
	sexback1param0['FILTER_NAME'] = options.cpath + "/gauss_3.0_7x7.conv"
	sexback1param0['PARAMETERS_NAME'] = options.cpath + "/tt.param"
	self.get_verbose(sexback1param0, options)
	return sexback1param0


# Get the number of extensions
def get_next(image):
    next = 0
    try:
	im = pyfits.open(image)
    except IOError():
	print 'Impossible to get extension number from ' + image
	sys.exit(1)
    for h in im:
	if h.__doc__.find("image") != -1:
	    next += 1

    im.close()
    return next


# Sextractor wrapper
def sextract(image, sexconf, cparam):
    cstr = ""
    for k in cparam:
	cstr += " -" + str(k) + " " + str(cparam[k]) + " "

    print "sex %s -c %s %s" % (image, sexconf, cstr)
    os.system("sex %s -c %s %s" % (image, sexconf, cstr));


# weightwatcher wrapper
def ww(wwconf, cparam):
    cstr = ""
    for k in cparam:
	cstr += " -" + str(k) + " " + str(cparam[k]) + " "

    print "ww -c %s %s \n" % (wwconf, cstr)
    os.system("ww -c %s %s" % (wwconf, cstr));


# Sextractor wrapper
def sextract2(image, cparam):
    cstr = ""
    for k in cparam:
	cstr += " -" + str(k) + " " + str(cparam[k]) + " "

    print "sex %s  %s " % (image, cstr)
    os.system("sex %s  %s " % (image, cstr));


# weightwatcher wrapper
def ww2(cparam):
    cstr = ""
    for k in cparam:
	cstr += " -" + str(k) + " " + str(cparam[k]) + " "

    print "ww  %s \n" % (cstr)
    os.system("ww  %s" % (cstr));

# swarp wrapper
def swarp(image, cparam):
    cstr = ""
    for k in cparam:
	cstr += " -" + str(k) + " " + str(cparam[k]) + " "

    print "swarp %s %s " % (image, cstr)
    os.system("swarp %s %s " % (image, cstr));


# Add value (concatenate with coma)
def add_val(par, key, val):
    if par.has_key(key):
	par[key] += "," + str(val)
    else:
	par[key] = str(val)


# Create the threads for the mask production (1pass)
def create_mask_pass1(file2, bertin_par, options):
    print " >>> Now starting subsky_sub.create_mask_pass1 ... \n "
    root = file2.split('.fits')[0]

    # First sextractor to build root of objects
    sexpar = bertin_par.get_sex1param(options)
    sexpar['CHECKIMAGE_NAME'] = root + "_ob.fits"
    sexpar['WEIGHT_IMAGE'] = root + options.inweight_suf
    sexpar['VERBOSE_TYPE'] = "QUIET"  #QUIET
    sexpar['BACK_SIZE'] = 64
    print root+'.fits',sexpar
    sextract2(root+'.fits',sexpar)

    # Then ww to convert it to a mask
    wwpar = bertin_par.get_ww1param(options)
    wwpar['OUTWEIGHT_NAME'] = root + options.outweight_suf
    wwpar['VERBOSE_TYPE'] = "QUIET"
    add_val(wwpar, 'WEIGHT_NAMES', root + '_ob.fits')

    if options.inweight_suf != "":
	add_val(wwpar, 'WEIGHT_NAMES', root + options.inweight_suf)
	add_val(wwpar, 'WEIGHT_MIN', '0.5')
	add_val(wwpar, 'WEIGHT_MAX', '1.5')
	add_val(wwpar, 'WEIGHT_OUTFLAGS', '1')
    ww2(wwpar)
    

# Copy the entire header but the keywords in the exclude list
def copy_header(inn, outt, exclude=[]):
    fitsim = pyfits.open(inn)
    file = open(outt, 'w')
    if len(exclude) == 0:
	file.write(fitsim[0].header)
	file.write('END')
    else:
	# lines = fitsim[0].header.ascardlist()
	lines = fitsim[0].header.cards
	for l in lines:
	    fex = 0
	    for ex in exclude:
		if l.keyword.split()[0] == ex:
		    fex = 1
		    break
	    file.write(l + '\n')
	file.write('END')
    file.close


# Copy the entire header but the keywords in the exclude list
def copy_header_MEF(inn, outt, extt, exclude=[]):
    fitsim = pyfits.open(inn)
    file = open(outt, 'w')
    if len(exclude) == 0:
	file.write(fitsim[extt].header + '\n')
	file.write('END')
    else:
	# lines = fitsim[extt].header.ascardlist()
	lines = fitsim[extt].header.cards
	for l in lines:
	    # print l
	    fex = 0
	    for ex in exclude:
		if l.keyword == ex:
		    fex = 1
		    break
	    if fex == 0:
		# print l.__repr__()
		file.write(l.__str__() + '\n')
		# file.write(l.__repr__()+'\n')
	file.write('END\n')
    file.close


# Project the input mask (maskin) at the position of the image (image)
def project(maskin, maskout, im, next, options):
    print " >>> Now starting subsky_sub.project ... \n "
    bertin_par = bertin_param()

    # If image is SEF
    if next == 1:
	# Get the header
	outhead = im.split('.fits')[0] + '.flag.miss.head'
	copy_header(im.split('.fits')[0] + '.temp.fits', outhead)

	# swarp it
	j = 1
	swarppar = bertin_par.get_swarpprojparam(options)
	swarppar['IMAGEOUT_NAME'] = im.split('.fits')[0] + '.flag.miss.fits'
	swarppar['RESAMPLE_SUFFIX'] = '.' + str(j) + '.resamp.fits'
	swarp2(maskin, swarppar)
	os.remove(outhead)

    else:      # image is a MEF - Loop on the extentions
	for j in range(1, next + 1, 1):
	    # copy header
	    infits = im.split('.fits')[0] + '.temp.fits'
	    outhead = im.split('.fits')[0] + '.flag.' + str(j) + '.head'
	    copy_header_MEF(infits, outhead, j, ['XTENSION', 'PCOUNT', 'GCOUNT'])
	    # Swarp image
	    swarppar = bertin_par.get_swarpproj1param(options)
	    swarppar['IMAGEOUT_NAME'] = im.split('.fits')[0] + '.flag.' + str(j) + '.fits'
	    swarppar['WEIGHTOUT_NAME'] = im.split('.fits')[0] + '.flag.' + str(j) + '.weight.fits'
	    swarppar['RESAMPLE_SUFFIX'] = '.' + str(j) + '.resamp.fits'
	    #swarp2(maskin, swarppar)
            cstr = ""
            for k in swarppar:
                cstr += " -" + str(k) + " " + str(swarppar[k]) + " "
            print "\nswarp %s %s " % (image, cstr)
            os.system("swarp %s %s " % (image, cstr));
	    os.remove(outhead)
	    os.remove(im.split('.fits')[0] + '.flag.' + str(j) + '.weight.fits')

	# join the extensions
	os.remove(im.split('.fits')[0] + '.temp.fits')
	os.system("missfits -c " + options.cpath + "/missfits.conf " + im.split('.fits')[0] + '.flag -OUTFILE_TYPE MULTI -SAVE_TYPE NEW -SPLIT_SUFFIX .%01d.fits')
	for j in range(1, next + 1, 1):
	    os.remove(im.split('.fits')[0] + '.flag.' + str(j) + '.fits')
	    continue

    # Build the final mask
    wwpar = bertin_par.get_ww12param(options)
    wwpar['OUTWEIGHT_NAME'] = im.split('.fits')[0] + options.outweight_suf
    add_val(wwpar, 'WEIGHT_NAMES', im.split('.fits')[0] + '.flag.miss.fits')
    if options.inweight_suf != "":
	add_val(wwpar, 'WEIGHT_NAMES', im.split('.fits')[0] + options.inweight_suf)
	add_val(wwpar, 'WEIGHT_MIN', '0.5')
	add_val(wwpar, 'WEIGHT_MAX', '1.5')
	add_val(wwpar, 'WEIGHT_OUTFLAGS', '1')
    ww2(wwpar)

#-----------------------------------------------------------------------------
# Project the input mask (maskin) at the position of the image (image)
#-----------------------------------------------------------------------------
def project_combine(maskin, maskout, im, next, options):
    print "\n >>> Now starting subsky_sub.project_combine ... "
    bertin_par = bertin_param()
    root = im.split('.fits')[0]

    for j in range(1, next + 1, 1):
#        print " --", root, j, "of ", next
        # copy header
        infits = root + '.temp.fits'
        outhead = root + '.flag.' + str(j) + '.head'
        copy_header_MEF(infits, outhead, j, ['XTENSION', 'PCOUNT', 'GCOUNT'])

        # Swarp image
        swarppar = bertin_par.get_swarpproj1param(options)
        swarppar['IMAGEOUT_NAME']  = root + '.flag.' + str(j) + '.fits'
        swarppar['WEIGHTOUT_NAME'] = root + '.flag.' + str(j) + '.weight.fits'
        swarppar['RESAMPLE_SUFFIX'] = '.' + str(j) + '.resamp.fits'
        swarppar['VERBOSE_TYPE'] = 'QUIET'
        #swarp2(maskin, swarppar)
        cstr = ""
        for k in swarppar:
            cstr += " -" + str(k) + " " + str(swarppar[k]) + " "
        print "swarp %s %s " % (maskin, cstr)
        os.system("swarp %s %s " % (maskin, cstr))

        os.remove(outhead)
        os.remove(root + '.flag.' + str(j) + '.weight.fits')

    print "\n >>> Join the extensions ..."
    print     "missfits -c " + options.cpath + "/missfits.conf " + root + '.flag  -OUTFILE_TYPE MULTI -SAVE_TYPE NEW -SPLIT_SUFFIX .%01d.fits'
    os.system("missfits -c " + options.cpath + "/missfits.conf " + root + '.flag  -OUTFILE_TYPE MULTI -SAVE_TYPE NEW -SPLIT_SUFFIX .%01d.fits')
    # rm intermediate (single extenstion) flag files
    for j in range(1, next + 1, 1):
        os.remove(root + '.flag.' + str(j) + '.fits')
    os.system('echo -n "## Built "; ls ' + root+'.flag.miss.fits')

    # Build the final mask
    print '\n >>> Merge into final mask ...'
    wwpar = bertin_par.get_ww12param(options)
    wwpar['OUTWEIGHT_NAME'] = root + options.outweight_suf + '_tmp' 
    add_val(wwpar, 'WEIGHT_NAMES', root + '.flag.miss.fits')
    if options.inweight_suf != "":
        print " >> add param values for %s"%(root + options.inweight_suf)
	add_val(wwpar, 'WEIGHT_NAMES', root + options.inweight_suf)
	add_val(wwpar, 'WEIGHT_MIN', '0.5')
	add_val(wwpar, 'WEIGHT_MAX', '1.5')
	add_val(wwpar, 'WEIGHT_OUTFLAGS', '1')
    # Add mask from single image masking
    print " >> add param values for single weight mask %s"%(root + options.outweight_suf)
    add_val(wwpar,'WEIGHT_NAMES', root +options.outweight_suf)
    add_val(wwpar,'WEIGHT_MIN','0.5')
    add_val(wwpar,'WEIGHT_MAX','1.5')
    add_val(wwpar,'WEIGHT_OUTFLAGS','1')
    wwpar['VERBOSE_TYPE'] = "QUIET"
    ww2(wwpar)

    os.remove(root + '.temp.fits')
    os.remove(root + '.flag.miss.fits')

    # Move output mask in the right filename
    print ' >>> Clean up and copy astro kwds form image'
    out = root + options.outweight_suf
    os.rename(root + options.outweight_suf + '_tmp',  out)

    # copy astro kwds of image to mask and convert mask to integer
    ihdus = pyfits.open(root+".fits")
    mhdus = pyfits.open(out,   mode="update")
    keys  =['CTYPE1','CTYPE2','CRVAL1','CRVAL2','CRPIX1','CRPIX2','CD1_1','CD1_2','CD2_1','CD2_2']
    nkeys = len(keys)
    for i in range(1,17):
        # convert mask data to byte
        mhdus[i].data = mhdus[i].data.astype('UInt8')
        # copy astro keywords
        hdi = ihdus[i].header
        hdm = mhdus[i].header
        for k in range(nkeys):
            key = keys[k]  
            val = hdi[key]
            hdm[key] = val
    mhdus.close(output_verify='silentfix+ignore')


#-----------------------------------------------------------------------------

# Read a list og keywords in a list of images
def read_header(list, keys):
    ''' Read a list of keywords in a list of images '''
    data = {}
    for k in keys:
	data[k] = []

    for im in list:
	pyim = pyfits.open(im)
	for k in keys:
	    try:
		val = pyim[0].header[k]
	    except KeyError():
		val = 'Nada'
	    data[k].append(val)

        pyim.close()
    return data


# Get the list of images to perform the skysubtaction
def get_skylist(ind0, imlist, data, options):
    ''' Get the list of images to perform the skysubtaction '''
    dtime = options.dtime2  # in minutes
    dist2 = options.dist2  # in arcsec

    # Filter by filter & position
    filter0 = data['FILTER'][ind0]
    ra0 = data['RA_DEG'][ind0]
    dec0 = data['DEC_DEG'][ind0]
    date0 = data['MJDATE'][ind0]

    if ra0 == 'Nada' or dec0 == 'Nada':
	return []

    good_im = []
    dtime_list = []
    for ind in range(len(imlist)):
	if ind != ind0 and filter0 == data['FILTER'][ind]:
	    d2 = (data['RA_DEG'][ind] - ra0) * (data['RA_DEG'][ind] - ra0) * (math.cos(dec0)) ** 2 + (data['DEC_DEG'][ind] - dec0) * (data['DEC_DEG'][ind] - dec0)
	    if d2 <= dist2:
		good_im.append(ind)
		dtime_list.append(abs(data['MJDATE'][ind0] - data['MJDATE'][ind]))

    # sort
    dtime_sorted = range(len(good_im))
    dtime_sorted.sort(lambda x, y: cmp(dtime_list[x], dtime_list[y]))

    # final list
    final = []
    count = 0
    for i in dtime_sorted:
	if dtime_list[i] <= options.dtime2 and count < options.numim:
	    final.append(imlist[good_im[i]])
	    count += 1
    return final


# Get the list of images to perform the skysubtaction (with sublist)
def get_skylist_sub(im, ind0, sublist, data, data_sub, options):
    ''' Get the list of images to perform the skysubtaction '''
    dtime = options.dtime2  # in minutes
    dist2 = options.dist2  # in arcsec

    # Filter by filter & position
    filter0 = data['FILTER'][ind0]
    ra0 = data['RA_DEG'][ind0]
    dec0 = data['DEC_DEG'][ind0]
    date0 = data['MJDATE'][ind0]

    cosdec2 = (math.cos(dec0)) ** 2

    if ra0 == 'Nada' or dec0 == 'Nada':
	return []

    good_im = []
    dtime_list = []
    filename_list = []
    for ind in range(len(sublist)):
	if sublist[ind] != im and filter0 == data_sub['FILTER'][ind]:
	    d2 = (data_sub['RA_DEG'][ind] - ra0) * (data_sub['RA_DEG'][ind] - ra0) * cosdec2 + (data_sub['DEC_DEG'][ind] - dec0) * (data_sub['DEC_DEG'][ind] - dec0)
	    if d2 <= dist2:
		good_im.append(ind)
		dtime_list.append(abs(data['MJDATE'][ind0] - data_sub['MJDATE'][ind]))
		filename_list.append(data_sub['FILENAME'][ind])

    # sort
    dtime_sorted = range(len(good_im))
    filename_sorted = range(len(good_im))
    dtime_sorted.sort(lambda x, y: cmp(dtime_list[x], dtime_list[y]))
    filename_sorted.sort(lambda x, y: cmp(dtime_list[x], dtime_list[y]))


    # final list
    # print options.numcube

    print "\n --------- \n"
    print options.dtime2

    if options.numcube == 0:
	final = []
	count = 0
	for i in dtime_sorted:
	    if dtime_list[i] <= options.dtime2 and count < options.numim:
		final.append(sublist[good_im[i]])
		count += 1
	return final
    else:
	final = []
	count_cube = 0
	list_cube = []
	cube_full = 0
	for i in dtime_sorted:
	    print "----- " + sublist[good_im[i]]
	    print list_cube
	    print final
	    if dtime_list[i] <= options.dtime2:
		if cube_full == 0 and not filename_list[i] in list_cube:
		    list_cube.append(filename_list[i])
		    if len(list_cube) >= options.numcube:
			cube_full = 1
		if filename_list[i] in list_cube:
		    final.append(sublist[good_im[i]])
	return final


# Get the list of images to perform the skysubtaction (with sublist) ######### TO FIX ##########


def get_skylist_sub_new(im, ind0, sublist, data, data_sub, options):
    ''' Get the list of images to perform the sky subtraction '''
    dtime = options.dtime2  # in minutes
    dist2 = options.dist2  # in arcsec

    # Filter by filter & position
    filter0 = data['FILTER'][ind0]
    ra0 = data['RA_DEG'][ind0]
    dec0 = data['DEC_DEG'][ind0]
    date0 = data['MJDATE'][ind0]

    cosdec2 = (math.cos(dec0)) ** 2

    if ra0 == 'Nada' or dec0 == 'Nada':
        return []

    good_im = []
    dtime_list = []
    filename_list = []
    for ind in range(len(sublist)):
        if sublist[ind] != im and filter0 == data_sub['FILTER'][ind]:
            d2 = (data_sub['RA_DEG'][ind] - ra0) * (data_sub['RA_DEG'][ind] - ra0) * cosdec2 + (data_sub['DEC_DEG'][ind] - dec0) * (data_sub['DEC_DEG'][ind] - dec0)
            if d2 <= dist2:
                good_im.append(ind)
                dtime_list.append(abs(data['MJDATE'][ind0] - data_sub['MJDATE'][ind]))
                filename_list.append(data_sub['FILENAME'][ind])

    # sort
    dtime_sorted = range(len(good_im))
    filename_sorted = range(len(good_im))
    dtime_sorted.sort(lambda x, y: cmp(dtime_list[x], dtime_list[y]))
    filename_sorted.sort(lambda x, y: cmp(dtime_list[x], dtime_list[y]))

#    if options.numcube == 0:
    final = []
    count = 0
    for i in dtime_sorted:
        if dtime_list[i] <= options.dtime2 and count < options.numim:
            final.append(sublist[good_im[i]])
            count += 1

    # Select random images from the cubes
    final2 = []

    final2 = [im for im in final]

    return final2
def get_skylist_dr6(im, ind0, sublist, data, data_sub, options):
    ''' Updates for DR6
    Get the list of images to perform the sky subtraction
    2023.apr:
    - delete some commeted lines;
    - check sky bgd values in subsilt and remove outliers
    '''

    numpy.set_printoptions(precision=4)
    dtime = options.dtime2  # in minutes of time
    dist2 = options.dist2   # in arcsec

    # Filter by filter & position
    filter0 = data['FILTER'][ind0]
    ra0 = data['RA_DEG'][ind0]
    dec0 = data['DEC_DEG'][ind0]
    date0 = data['MJDATE'][ind0]
    cosdec2 = (math.cos(dec0)) ** 2

    if ra0 == 'Nada' or dec0 == 'Nada':
	return []

    good_im = []
    dtime_list = []
    filename_list = []
    for ind in range(len(sublist)):
	if sublist[ind] != im and filter0 == data_sub['FILTER'][ind]:
	    d2 = (data_sub['RA_DEG'][ind] - ra0) * (data_sub['RA_DEG'][ind] - ra0) * cosdec2 + (data_sub['DEC_DEG'][ind] - dec0) * (data_sub['DEC_DEG'][ind] - dec0)
	    if d2 <= dist2:
		good_im.append(ind)
		dtime_list.append(abs(data['MJDATE'][ind0] - data_sub['MJDATE'][ind]))
		filename_list.append(data_sub['FILENAME'][ind])

    # sort
    dtime_sorted = range(len(good_im))   
    filename_sorted = range(len(good_im))

    dtime_sorted.sort(lambda x, y: cmp(dtime_list[x], dtime_list[y]))
    filename_sorted.sort(lambda x, y: cmp(dtime_list[x], dtime_list[y]))

    final = []
    count = 0
    for i in dtime_sorted:
        if dtime_list[i] <= options.dtime2 and count < options.numim:
            final.append(sublist[good_im[i]])
#            print "[]    - %s "%(sublist[good_im[i]])
            count += 1
    
    return final


def select_imcube(final, nim):
    """ Selecto a maximum of nim images from each cube """

    final2 = []

    pattern = re.compile(r'(\S+)_(\d+).fits')
    im_data = {}
    for im in final:
	match = pattern.search(im)
	if not match:
	    print "Incorrect image format for nimcube option !! " + im
	    sys.exit(1)
	else:
	    if not im_data.has_key(match.group(1)):
		im_data[match.group(1)] = []
		im_data[match.group(1)].append(match.group(2))
	    else:
		im_data[match.group(1)].append(match.group(2))

    for key, val in im_data.iteritems():
	nval = len(val)
	if nval <= nim:
	    for imm in val:
		final2.append(key + "_" + imm + ".fits")
	else:
	    added = []
	    for i in range(nim):
		while (1):
		    v = val[int(numpy.random.rand() * nval)]
		    if not v in added:
			added.append(v)
			final2.append(key + "_" + v + ".fits")
			break

    return final2


def cp_head(infits, outhead):
    """ Copy the header(s) of a fits image in a txt file """
    if os.path.isfile(outhead):
	os.remove(outhead)
    file = open(outhead, 'w')

    pi = pyfits.open(infits)
    if len(pi) == 1:  # SEF
	file.write(pi[0].header)
	file.write('\nEND')
    else:  # MEF
#	print " - Copy headers for %i extensions"%(len(pi)-1)	# AMo: to replace print in loop below
	for i in range(1, len(pi), 1):
            #print i			# AMo: - replaced by message before loop
	    file.write(pi[i].header.__str__())
	    file.write('\nEND\n')
    file.close()


def cp_skykeys(inn, outt):
    ''' Copy the list of images used to build the sky from one image to the other '''
    # Read keys
    pi = pyfits.open(inn)

    listk = []
    listv = []
    ind = 0
    hdin = pi[0].header
    while 'SKYIM' + str(ind) in hdin:
	listk.append('SKYIM' + str(ind))
	listv.append(pi[0].header['SKYIM' + str(ind)])
	ind += 1
    histkey = 1   # let's hope
    try:
        hist = hdin['history']
    except:
        histkey = 0
    pi.close()

    # Write keys
    with pyfits.open(outt, mode='update') as pi:
	for (key, val) in zip(listk, listv):
	    pi[0].header[key] = val
        if histkey:
            for i in range(len(hist)):
                pi[0].header['history'] = hist[i]


def check_same_sky(im_old, skylist):
    # get the keywords
    pi = pyfits.open(im_old)

    ind = 0
    list_k = []
    while pi[0].header.has_key('SKYIM' + str(ind)):
	list_k.append(pi[0].header['SKYIM' + str(ind)])
	ind += 1
    pi.close()

    print list_k
    print skylist

    # Check if same list of images
    if len(skylist) != len(list_k):
	return 0
    for key in list_k:
	if not key in skylist:
	    return 0
    return 1


def exclude_chips(skylist, image, ext, list_exclude):
    """ Exclude some chips from the list """

    skylist2 = []
    for im in skylist:
	if list_exclude.has_key(im) and str(ext) in list_exclude[im]:
	    continue
	skylist2.append(im)

    #print "  Final list of images used :"
    #for im in skylist2:
    #	 print "  - ", im

    return skylist2


#########################
def med_row(args):
    """ Compute median on a row """

    (mma, i) = args
    resu = []
    for j in range(mma.shape[0]):
	resu.append(numpy.median(mma[j, :].compressed()))
    return (i, resu)


def med_cube_multiproc(cube, options):
    pool = Pool(processes=options.nproc)
    imshape = cube[:, :, 0].shape

    # Get the final mask / number of images for each pixel
    cube_nval = numpy.ma.ones(cube.shape)
    ma_nval = numpy.ma.array(cube_nval, mask=numpy.ma.getmask(cube)).sum(axis=2)
    mask_fin = numpy.zeros(imshape)
    mask_filled = numpy.ma.filled(ma_nval, fill_value=0)
    ind = numpy.where(mask_filled < 1)
    mask_fin[ind] = 1

    # get the median
    res = numpy.zeros(imshape)
    list_args = []
    for i in range(imshape[0]):
	arg = [cube[i, :, :], i]
	list_args.append(arg)
    results = pool.map(med_row, list_args)
    pool.close()
    for (i, v) in results:
	res[i, :] = v

    # re-create the final mask
    res_mask = numpy.ma.array(res, mask=mask_fin)

    return res_mask, ma_nval


def med_im(arg):
    return (arg[1], numpy.median(arg[0].flatten().compressed()))


def get_median_fits(arg):
    """ Get the median of a fits image """
    (im, mask) = arg
    mlist = []

    pyim = pyfits.open(im)
    pymask = pyfits.open(mask)
    next = len(pyim)
    if next == 1:
	im_mask = numpy.ma.array(pyim[0].data, mask=1 - pymask[0].data)
	mlist = [im, numpy.median(im_mask.compressed())]
    else:
	mmlist = []
	for iext in range(next)[1:]:
	    im_mask = numpy.ma.array(pyim[iext].data, mask=1 - pymask[iext].data)
	    mmlist.append(numpy.median(im_mask.compressed()))
	mlist = [im, mmlist]
    pyim.close()
    pymask.close()

    return mlist


#########################

def compute_sky_linreg(im00, iext, SKY_median, data_y, skylist, sky_im):
    """ Compute the sky with a linear regression """

    ext = iext

    # Mean of each px (i,j) values (across the "sky image" axis)
    mean_data_y = numpy.ma.mean(data_y, axis=0)

    # Subtract mean
    dmean_y = numpy.ma.array(numpy.zeros(data_y.shape), mask=numpy.ma.getmask(data_y))
    for i in range(data_y.shape[0]):
	dmean_y[i] = data_y[i] - mean_data_y

    ########################
    # Get the median lists #
    ########################

    data_x = []
    for im in skylist:
	data_x.append(SKY_median[im][iext - 1])
    t_data_x = numpy.array(data_x)

    np_data_x = numpy.transpose(numpy.tile(t_data_x, (data_y.shape[1], 1)))  # Numpy version
    ma_data_x = numpy.ma.array(np_data_x, mask=numpy.ma.getmask(data_y))     # masked array version

    # Mean value of median_sky for each pixel (mean on the "sky images" axis)
    mean_data_x = numpy.ma.mean(ma_data_x, axis=0)

    # Subtract mean from the median values
    dmean_x0 = np_data_x - numpy.array([mean_data_x for i in range(data_y.shape[0])])
    dmean_x = numpy.ma.array(dmean_x0, mask=numpy.ma.getmask(data_y))

    ##########################
    # Compute the regression #
    ##########################

    ma_data_x2 = ma_data_x * ma_data_x
    dmean_x2 = dmean_x * dmean_x

    num = dmean_x * dmean_y
    snum = num.sum(axis=0)
    sdenom = dmean_x2.sum(axis=0)

    aa = snum / sdenom
    bb = mean_data_y - aa * mean_data_x

    np_ar = numpy.reshape(numpy.ma.filled(aa, 0), sky_im[skylist[0]][0].shape)
    np_br = numpy.reshape(numpy.ma.filled(bb, 0), sky_im[skylist[0]][0].shape)


    #####################
    # Get the sky image #
    #####################

    sky_im0 = (np_ar * SKY_median[im00][ext - 1] + np_br)

    return sky_im0


def create_skyim_SCALELVL2(im00, skylist, sub_im, sky_im, ext, outim, SKY_median, options):
    """ Create a sky image with pattern normalized to BACKLVL=1000 / median around 0 """
    iext = ext


    #############################
    # Open the images in pyfits #
    #############################

    pyim_list = {}
    for im in skylist:
	pyim_list[im] = sky_im[im][iext - 1]

    imshape = pyim_list[skylist[0]].shape

    #################################
    # Get the flattened pixel lists #
    #################################

    im_3d = []
    mask_3d = []
    for ii, im in enumerate(skylist):
	im_3d.append(numpy.ma.array(sky_im[im][ext - 1].flatten()))
	mask_3d.append(numpy.array(numpy.ma.getmask(sky_im[im][ext - 1]).flatten()))
    data_y = numpy.ma.array(numpy.array(im_3d), mask=numpy.array(mask_3d))

    # Mean of each px (i,j) values (across the "sky image" axis)
    mean_data_y = numpy.ma.mean(data_y, axis=0)

    # Subtract mean
    dmean_y = numpy.ma.array(numpy.zeros(data_y.shape), mask=numpy.ma.getmask(data_y))
    for i in range(data_y.shape[0]):
	dmean_y[i] = data_y[i] - mean_data_y

    ########################
    # Get the median lists #
    ########################

    data_x = []
    for im in skylist:
	data_x.append(SKY_median[im][iext - 1])
    t_data_x = numpy.array(data_x)

    np_data_x = numpy.transpose(numpy.tile(t_data_x, (data_y.shape[1], 1)))  # Numpy version
    ma_data_x = numpy.ma.array(np_data_x, mask=numpy.ma.getmask(data_y))  # masked array version

    # Mean value of median_sky for each pixel (mean on the "sky images" axis)
    mean_data_x = numpy.ma.mean(ma_data_x, axis=0)

    # Subtract mean from the median values
    dmean_x0 = np_data_x - numpy.array([mean_data_x for i in range(data_y.shape[0])])
    dmean_x = numpy.ma.array(dmean_x0, mask=numpy.ma.getmask(data_y))

    ##########################
    # Compute the regression #
    ##########################

    ma_data_x2 = ma_data_x * ma_data_x
    dmean_x2 = dmean_x * dmean_x

    num = dmean_x * dmean_y
    snum = num.sum(axis=0)
    sdenom = dmean_x2.sum(axis=0)

    aa = snum / sdenom
    bb = mean_data_y - aa * mean_data_x

    np_ar = numpy.reshape(numpy.ma.filled(aa, 0), sky_im[skylist[0]][0].shape)
    np_br = numpy.reshape(numpy.ma.filled(bb, 0), sky_im[skylist[0]][0].shape)


    #####################
    # Get the sky image #
    #####################

    sky_im0 = (np_ar * SKY_median[im00][ext - 1] + np_br)


    ##################
    # Sigma clipping #
    ##################

    if 1:
	print "SIze sky_im0 ", sky_im0.shape
	print "Size data_y", data_y.shape

	# sky = numpy.tile(aa * SKY_median[im00][ext-1] + bb,(data_y.shape[0],1))
	# print "Size sky ",sky.shape

	# Get the expected values (from a*MEDIAN+b) for each pixel for each sky image
	sky_medians = numpy.array([SKY_median[imm][ext - 1] for imm in skylist])
	print "Size medians ", sky_medians.shape

	sky_medians2 = numpy.transpose(numpy.tile(sky_medians, (data_y.shape[1], 1)))
	print "Size medians2 ", sky_medians2.shape

	a_s = numpy.tile(aa, (data_y.shape[0], 1))
	b_s = numpy.tile(bb, (data_y.shape[0], 1))

	print "a_s_shape ", a_s.shape
	print "b_s_shape ", b_s.shape

	# Get the expected values from the linear regression for each pixel
	expected = a_s * sky_medians2 + b_s

	print "expected shape ", expected.shape

	print "TEST", aa.shape
	print sky_medians

	print a_s[0, 10000]
	print b_s[0, 10000]

	print sky_medians
	print expected[:, 10000]
	print data_y[:, 10000]

	# Get the residual
	res0 = data_y - expected
	res = numpy.ma.array(res0, mask=numpy.ma.getmask(data_y))

	# Get the std for the clipping
	std_res = numpy.ma.std(res, axis=0)
	print "std res shape ", std_res.shape
	print "std res 10000", std_res[10000]
	print "res 10000", res[:, 10000]

	nsig = 2.0

	std_res2 = numpy.tile(std_res, (data_y.shape[0], 1))
	print "std_res2 shape", std_res2.shape

	# Get the sigma normalized residual
	limit = res / std_res2


	# Get the outliers
	ind = numpy.where(abs(limit) > nsig)
	print len(ind)
	print len(ind[0])
	print len(ind[0]) * 1.0 / (data_y.shape[1] * 1.0) / (data_y.shape[0] * 1.0)

	###############
	if 0:
	    j = 0
	    k = 336323
	    print j, k
	    j = ind[0][k]
	    for k0 in range(1000):
		k = k0 * 33
		print "\n\nTEST"
		j = ind[0][k]
		for i in range(9):
		    print sky_medians[i], expected[i, j], data_y[i, j], res[i, j], std_res2[i, j], limit[i, j]
	################

	# Mask the outliers
	data_y[ind] = numpy.ma.masked

	# Recompute the sky
	sky_new = compute_sky_linreg(im00, iext, SKY_median, data_y, skylist, sky_im)

    skyy = numpy.reshape(numpy.ma.filled(sky_im0, 0), sky_im[skylist[0]][0].shape)
    # fitslib.narray2fits(skyy,"sky.fits")
    # sys.exit(0)

    ######################
    # Get the nval image #
    ######################

    nval = numpy.ones(data_y.shape)
    ma_nval = numpy.ma.array(nval, mask=numpy.ma.getmask(data_y))
    nval_tot = ma_nval.sum(axis=0)
    nval = numpy.reshape(numpy.ma.filled(nval_tot, 0), sky_im[skylist[0]][0].shape)

    return sky_im0, nval


def create_skyim_SCALELVL(skylist, ext, outim, SKY_median, options):
    """ Create a sky image with pattern normalized to BACKLVL=1000 / median around 0 """
    iext = ext

    options.SKYLVL_method = "median"

    #############################
    # Open the images in pyfits #
    #############################

    # print "Read the im/mask"
    pyim_list = {}
    for im in skylist:
	pyim_list[im] = []
	pyim = pyfits.open(im)

	mask = im.split(".fits")[0] + options.inmask_suf
	pymask = pyfits.open(mask)

	pyim_list[im] = numpy.ma.array(pyim[iext].data, mask=1 - pymask[iext].data, dtype=numpy.float64)

	pyim.close()
	pymask.close()

    imshape = pyim_list[skylist[0]].shape

    #############################
    # Data cube with all images #
    #############################

    cube_array = numpy.ma.dstack([pyim_list[im] for im in skylist])

    ######################
    # List of sky levels #
    # Scale if needed    #
    ######################

    SKLVL = numpy.array([SKY_median[im][iext - 1] for im in skylist])

    for i, lvl in enumerate(SKLVL):
	if options.noscale:
	    cube_array[:, :, i] -= lvl
	else:
	    cube_array[:, :, i] = (cube_array[:, :, i] - lvl) / lvl * 1000.0

    ########################
    # For number of pixels #
    ########################

    sky_nn = numpy.ma.array(numpy.ones(cube_array.shape), mask=numpy.ma.getmask(cube_array))

    ###############
    # Get the sky #
    ###############
    if options.meansky:      # Use mean
	sky_lvl = cube_array.mean(axis=2)
	if options.npix:
	    sky_nval = numpy.ma.filled(sky_nn.sum(axis=2), fill_value=0)
	else:
	    sky_nval = 0
    else:  # Use median
	sky_lvl, sky_nval = med_cube_multiproc(cube_array, options)

    return sky_lvl, sky_nval

#########################

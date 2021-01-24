from math import sqrt
from math import cos
import os, re, sys
import stats


# from pylab import *


# Get the 2MASS catalog from CDS
def getcat_2MASS(ra, dec):
    minra = min(ra)
    maxra = max(ra)
    mindec = min(dec)
    maxdec = max(dec)

    rac = (minra + maxra) / 2.0
    decc = (mindec + maxdec) / 2.0

    dra = maxra - minra
    ddec = maxdec - mindec

    rad = sqrt(dra * dra + ddec * ddec) / 2.0 * 1.1 * 60.0

    tmasscat = "tmp2mass_" + str(os.getpid())
    # print "aclient cocat1.u-strasbg.fr 1660 find2m -c "+str(rac)+" "+str(decc)+" -r "+str(rad)+" -m | sed 's/|/ /g' > "+tmasscat
    # os.system("aclient cocat1.u-strasbg.fr 1660 find2m -c "+str(rac)+" "+str(decc)+" -r "+str(rad)+" -m 1000000 | sed 's/|/ /g' > "+tmasscat)
    print "find2mass -c " + str(rac) + " " + str(decc) + " -r " + str(rad) + " -m 1000000 | sed 's/|/ /g' > " + tmasscat
    os.system(
        "find2mass -c " + str(rac) + " " + str(decc) + " -r " + str(rad) + " -m 1000000 | sed 's/|/ /g' > " + tmasscat)

    # check the number of detections
    pattern = r'#---\s+(\d+)'
    regex = re.compile(pattern)

    file = open(tmasscat, 'r')
    lines = file.readlines()
    file.close()
    for line in lines:
        match = regex.search(line)
        if match:
            if match.group(1) == 0:
                print "No 2mass object found ..\n"
                sys.exit(1)
    return tmasscat


# Get the 2MASS catalog from CDS
def getcat_2MASS_center(ra, dec, rad):
    rac = ra
    decc = dec
    rad = rad * 60.0

    tmasscat = "tmp2mass_" + str(os.getpid())
    # print "aclient cocat1.u-strasbg.fr 1660 find2m -c "+str(rac)+" "+str(decc)+" -r "+str(rad)+" -m 1000000 | sed 's/|/ /g' > "+tmasscat
    # os.system("aclient cocat1.u-strasbg.fr 1660 find2m -c "+str(rac)+" "+str(decc)+" -r "+str(rad)+" -m 1000000 | sed 's/|/ /g' > "+tmasscat)
    print "find2mass -c " + str(rac) + " " + str(decc) + " -r " + str(rad) + " -m 1000000 | sed 's/|/ /g' > " + tmasscat
    os.system(
        "find2mass -c " + str(rac) + " " + str(decc) + " -r " + str(rad) + " -m 1000000 | sed 's/|/ /g' > " + tmasscat)

    # check the number of detections
    pattern = r'#---\s+(\d+)'
    regex = re.compile(pattern)

    file = open(tmasscat, 'r')
    lines = file.readlines()
    file.close()

    for line in lines:
        match = regex.search(line)
        if match:
            if match.group(1) == 0:
                print "No 2mass object found ..\n"
                sys.exit(1)
    return tmasscat


# Read the 2MASS catalog
def read_2MASS(tmasscat, filter, ab):
    lra = []
    ldec = []
    lmag = []

    # Which filter - column
    if filter == 'J':
        colmag = 7
    elif filter == 'H':
        colmag = 11
    elif filter == 'Ks':
        colmag = 15
    else:
        print "Filter " + options.filter + " not in 2MASS \n"
        sys.exit(1)

    # AB/VEGA correction
    magcorr = 0
    if ab:
        if colmag == 8:
            magcorr = 0.8894
        if colmag == 12:
            magcorr = 1.3642
        if colmag == 16:
            magcorr = 1.8402

    file = open(tmasscat, 'r')
    lines = file.readlines()
    file.close()
    for line in lines:
        if line[0] == "#":
            continue
        list = line.split()
        # print "list ",list

        if list[colmag - 1][0] == "-":
            # print 'BAD value --- '+list[colmag - 1][0]
            continue

        try:
            # lra.append(float(list[0]))
            # ldec.append(float(list[1]))
            # lmag.append(float(list[colmag - 1]))
            try:
                magg = float(list[colmag - 1])
                # os.system('echo magg = '+list[colmag - 1]+' >> log')
                # lmag.append(float(list[colmag - 1])+magcorr)
            except:
                # lmag.append(0)
                print "Error mag = " + str(list[colmag - 1])
                magg = 0

            if magg != 0:
                lra.append(float(list[0]))
                ldec.append(float(list[1]))
                lmag.append(magg + magcorr)

        except IndexError:
            print "Error in the 2MASS catalog ..."
            sys.exit(1)
    return (lra, ldec, lmag)

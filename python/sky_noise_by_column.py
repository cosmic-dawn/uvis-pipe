#!/softs/intel/intelpython3-2022.2.1.17274/intelpython/python3.9/bin/python
#-----------------------------------------------------------------------------
# v2 2023.oct.30

import os, sys
import numpy as np

import astropy.io.fits as fits
from scipy.stats import sigmaclip
from astropy.stats import sigma_clip
from astropy.stats import mad_std

import matplotlib as mpl
mpl.rcParams['xtick.direction'] = 'in'
mpl.rcParams['ytick.direction'] = 'in'
mpl.rcParams['xtick.top'] = "True"
mpl.rcParams['ytick.right'] = "True"
mpl.rcParams['xtick.labelsize'] = 14
mpl.rcParams['ytick.labelsize'] = 14
mpl.rcParams['xtick.minor.visible'] = True
import matplotlib.pyplot as plt

#-----------------------------------------------------------------------------
# Noise by column in DR6 stacks 
#
# Read the sky files, by vertical stripes width wide but avoiding offs pixels from top/bottom edges,
# every inter columns, starting at offs0 or soon thereafter, then compute one of the estimates of
# the dispersion of the nonzero values:
# - 1: astropy.stats.sigma_clip (hi=2.3, lo=2.5) and np.nanstd
# - 2: scipy.stat..sigmaclip(default params) and np.std
# - 3: mad_std
#-----------------------------------------------------------------------------

filters=("Y", "J", "H", "K", "N") ; res = 'lr'
reldir = "/n23data1/UltraVista/DR6_RC1-gamma/"
method=3
#doPNG=True; doPDF=True
doPNG=False; doPDF=False

res = sys.argv[1]
if res == 'lr':
    inter = 50 ; width = 50; offs0 = 400
elif res == 'hr':
    inter = 100 ; width = 100; offs0 = 800
else:
    print(" ..... Testing mode .....") 
    doPNG=False; doPDF=False
    res='lr' ; inter = 200 ; width = 3 ;  offs0 = 400    # min offset to skip around edges
#    res='hr' ; inter = 400 ; width = 2 ;  offs0 = 800    # min offset to skip around edges

nf = 0
for f in filters:
    file = "UVISTA_p2m-{:}_full_{:}_v10_sky.fits".format(f,res)
    pima = fits.open(reldir+file)
    naxis1 = pima[0].header['naxis1'] 
    data = pima[0].data                  #; print(np.shape(data))
    pima.close()
    
    # general setup: initialize the array to hold the noise measurements
    if nf == 0:
        num = []                # column numbers - for x-axis of plot
        ncols = int((naxis1-2*offs0)/inter)
        rest = naxis1 - ncols*inter      #    ; print(ncols, rest)
        offs = int(rest/2)
        
        for j in range(offs, naxis1-offs+1, inter):
            num.append(j)
        nn = len(num)
        print("Files have {:} columns".format(naxis1))
        print("Read {:} vertical stripes: one every {:}, from {:} to {:}".format(nn, inter,offs, naxis1-offs))
        noise = np.zeros((nn, len(filters)))  # initialise array
    
    print(">> Begin {:} - has {:} columns".format(file,naxis1)) 
    
    for i in range(len(num)):    #range(offs, naxis1 - offs, inter):
        col = data[offs:-offs , num[i]-width:num[i]+width+1]  ; 
        if method == 1:
            hi=2.3
            cln,low,upp = sigmaclip(col, low=2.5,high=hi)  #; print(i,np.std(col), np.std(cln))
            nn = np.nanstd(cln)
        if method == 2:
            nn = np.std(sigma_clip(col))
        if method == 3:
            nn = mad_std(col)
            
        noise[i,nf] = nn
        print("{:-4n}, col {:-6n}, noise {:0.2f} ".format(i, num[i], nn), end='\r')
    nf += 1 

print("DONE reading; build 'noise' array, of shape:", np.shape(noise))

#-----------------------------------------------------------------------------
fig,ax = plt.subplots(2,1, figsize=(12,12), sharex=True, gridspec_kw={'height_ratios': [1.7, 1]})
plt.subplots_adjust(wspace=0.0, hspace=0.0)

med_noi = []
filters=("Y", "J", "H", "K")

for i in range(len(filters)):
    f = filters[i]
    medi = np.median(noise[:,i]) ; med_noi.append(medi)
    #; mstd = mad_std(noise[:,i])     
    label = '{:s}_{:}: medi {:0.2f}'.format(f,res, medi) 
    ax[0].plot(num, noise[:,i], label=label)

ax[0].set_ylim(0.11,0.62)  # hi res
if res == 'lr': ax[0].set_ylim(0.45,2.4)  # lo res
ax[0].set_ylabel("Sky noise [counts]", fontsize=18)

ax[0].grid(color='grey', ls=':')
ax[0].legend(loc="upper left", fontsize=10, ncol=2)

# filter N - alone because of very different scale
i = 4  ; f = "N" 
nn = noise[:,i]
medi = np.median(nn[nn<np.mean(nn)]) ; med_noi.append(medi)
medih = np.median(nn[nn>np.mean(nn)])  ; med_noi.append(medih)
label = '{:s}_{:}: medi {:0.2f} / {:0.2f}'.format(f,res, medi, medih) 
ax[1].plot(num, noise[:,i], label=label, color="purple") #, label=suffs[i])
ax[1].set_xlabel("column number", fontsize=18)
ax[1].set_ylabel("Sky noise [counts]", fontsize=18)
ax[1].set_ylim(0.3,2.7)  # hi res
if res == 'lr': ax[1].set_ylim(1.7,11)
ax[1].grid(color='grey', ls=':')
ax[1].legend(loc="upper left", fontsize=10)

fig.suptitle("Column noise - {:} stacks".format(res), y=0.92, fontsize=20)
name = "{:}Column_noise_{:}_{:}-{:}".format(reldir,res,offs0,width) 
name = "{:}Test_{:}_{:}-{:}".format(reldir,res,offs0,width) 
print("Write figures: ", name)
if doPNG == True: fig.savefig(name+".png", bbox_inches="tight")
if doPDF == True: plt.savefig(name+'.pdf', bbox_inches="tight")

plt.show() ; plt.close()

#-----------------------------------------------------------------------------
# convert mean noise to 3-sigma mag limit
# confert flux to magnitude (with ZP = 30)
def f2m(s):
    return(-2.5*np.log10(s)+30)
#-----------------------------------------------------------------------------

filts=("Y", "J", "H", "K", "No","Ne")

if res == 'lr':
    area2 = np.pi * (2/0.30/2)**2
    area3 = np.pi * (3/0.30/2)**2 ; print('area 2"/3" apt: {:4.0f} {:4.0f} (pix)'.format(area2, area3))
else:
    area2 = np.pi * (2/0.15/2)**2
    area3 = np.pi * (3/0.15/2)**2 ; print('area 2"/3" apt: {:4.0f} {:4.0f} (pix)'.format(area2, area3))

fmt="  {:2s}  {:5.2f}  {:6.2f} {:6.2f}  {:6.2f} {:6.2f}"
fmt="  {:2s} & {:5.2f} & {:5.2f} & {:5.2f} & {:5.2f} & {:5.2f} \\\\"

nn=3
print("\n{:} stacks - 3-sigma noise limits".format(res))
print("filt     s       s2      s3      m2      m3")
for i in range(6):
    f = filts[i]
    apnoi2 = med_noi[i] * np.sqrt(area2)
    apnoi3 = med_noi[i] * np.sqrt(area3)
    print(fmt.format(f,med_noi[i], apnoi2, apnoi3, f2m(nn*apnoi3), f2m(nn*apnoi2)))

sys.exit()

#-----------------------------------------------------------------------------
'''
-----------------------------------------------------------------------------
RESULTS
-----------------------------------------------------------------------------
Method 3:
area 2"/3" apt:   35   79 (pix)
area 2"/3" apt:  140  314 (pix)

lr stacks - 3-sigma noise limits
filt     s       s2      s3      m2      m3
  Y  &  0.73 &  4.33 &  6.49 & 26.78 & 27.22 \\
  J  &  0.83 &  4.91 &  7.37 & 26.64 & 27.08 \\
  H  &  1.13 &  6.67 & 10.00 & 26.31 & 26.75 \\
  K  &  1.69 &  9.98 & 14.97 & 25.87 & 26.31 \\
  No &  2.55 & 15.09 & 22.63 & 25.42 & 25.86 \\
  Ne &  7.80 & 46.10 & 69.15 & 24.21 & 24.65 \\

hr stacks - 3-sigma noise limits
filt     s       s2      s3      m2      m3
  Y  &  0.18 &  2.16 &  3.24 & 27.53 & 27.97 \\
  J  &  0.21 &  2.45 &  3.68 & 27.39 & 27.83 \\
  H  &  0.28 &  3.33 &  5.00 & 27.06 & 27.50 \\
  K  &  0.42 &  4.98 &  7.46 & 26.62 & 27.07 \\
  No &  0.64 &  7.54 & 11.32 & 26.17 & 26.61 \\
  Ne &  1.95 & 23.02 & 34.54 & 24.96 & 25.40 \\

NB. in test mode the results are virtually identical

-----------------------------------------------------------------------------
EXEC TIMES
-----------------------------------------------------------------------------
on c03:
- lr:
- hr:

on n23
- lr
- hr
'''
#-----------------------------------------------------------------------------

mnoi = -2.5*np.log10(noise)+30

fig,ax = plt.subplots(2,1, figsize=(12,16), sharex=True)
plt.subplots_adjust(wspace=0.0, hspace=0.0)

filters=("Y", "J", "H", "K")
for i in range(len(filters)):
    f = filters[i]
#    ax[0].plot(num, noise[:,i], label=f) #, label=suffs[i])
    ax[0].plot(num, mnoi[:,i], label=f) #, label=suffs[i])
    mini = np.nanmin(noise[:,i])        #; print(mini)
    medi = np.nanmedian(noise[:,i])     #; print("mean: {:0.2f}, min: {:0.2f}".format(medi, mini))

#ax[0].set_ylim(0.1,0.8)  # hi res
#ax[0].set_ylim(0.6,3.6)  # lo res
ax[0].set_ylabel("Sky rms noise [mag]", fontsize=18)
ax[0].set_ylim(28,30.5)  # hi res

ax[0].grid(color='grey', ls=':')
ax[0].legend(loc="lower left", fontsize=16)

i = 4    # filter N - alone because of very different scale
f = "N"
ax[1].plot(num, mnoi[:,i], label=f, color="purple") #, label=suffs[i])
ax[1].set_xlabel("column number", fontsize=18)
ax[1].set_ylabel("Sky rms noise [mag]", fontsize=18)

ax[1].grid(color='grey', ls=':')
ax[1].legend(fontsize=16)

plt.suptitle("Column noise - low-res stacks", y=0.90, fontsize=20)
if "hr" in file: res = "hr"
else: res="lr"
name = "{:}test_{:}.png".format(reldir,res)
plt.savefig(name, bbox_inches="tight")

plt.show() ; plt.close()


# In[ ]:


"lr" in file


# In[ ]:





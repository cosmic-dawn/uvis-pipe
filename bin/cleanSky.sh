#!/bin/sh

#-----------------------------------------------------------------------------
set -u  
# paths
export PATH="~/bin:$PATH:/softs/dfits/bin:/softs/astromatic/bin"
export PYTHONPATH="/home/moneti/uvis/python:/home/moneti/uvis/python_lib"

# add python and UltraVista scripts and libs
module () {  eval $(/usr/bin/modulecmd bash $*); }
module purge; module load intelpython/2

uvis=/home/moneti/softs/uvis-pipe            # top UltraVista code dir
bindir=$uvis/bin
pydir=$uvis/python                # python scripts
confdir=$uvis/config              # config dir

ec() { echo "$(date "+[%d.%h.%y %T"]) $1 "; }    # echo with date
sdate=$(date "+%s")

#-----------------------------------------------------------------------------
# Setup
#-----------------------------------------------------------------------------

if [ $# -lt 4 ]; then
	echo "SYNTAX:  cleanSky.sh imlist back_size back_filter_size suffix {dry}"
	exit 1
fi

#echo "$0 $1 $2 $3 $4"
cp $confdir/config/bgsub.param $confdir/config/bgsub.conf .
cp $confdir/config/gauss_3.0_7x7.conv .

if [ $# -eq 5 ]; then dry=1; else dry=0; fi

comm="python $pydir/cleanSky.py -l $1 -s $2 -f $3 -o $4 "
echo $comm

if [ $dry -eq 1 ]; then
	$comm -D
else
	$comm
	if [ $? -ne 0 ]; then exit 1; fi
fi

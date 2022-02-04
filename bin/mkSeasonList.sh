#!/bin/sh

rm -f list_season

for f in $(cat list_images); do 
	yr=$(echo $f | cut -c2-5); mo=$(echo $f | cut -c6-7)
	for n in $(seq 2009 2020); do
		if [ $yr -eq $n ]; then
			if [ $mo -gt 8 ]; then 
				echo "$f $(($yr - 2008))" | awk '{printf "%-22s S%02i\n", $1,$2}' >> list_season
			else
				echo "$f $(($yr - 2009))" | awk '{printf "%-22s S%02i\n", $1,$2}' >> list_season
			fi
		fi
	done
done

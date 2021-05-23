# add.awk
BEGIN  { s = 0 }
{ s = s + $4*$5 }
END { print "Total: " s/3600.  }

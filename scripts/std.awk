{x[NR]=$0; s+=$0; n++} 
END{
    a=s/n; 
    for (i in x){ss += (x[i]-a)^2} sd = sqrt(ss/n)
    printf "==> for %0i values, mean = %0.2f, std.dev = %0.2f \n",n,a,sd
}

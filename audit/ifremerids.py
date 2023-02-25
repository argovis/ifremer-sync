# usage: bash whichprofiles.sh
# makes a list 'ifremerprofiles' that lists every <wmo>_<cycle> found in the ifremer mirror at /bulk/ifremer

find /bulk/ifremer -type f > x1
grep -Fri '/profiles/' x1 > x2
cut -d'/' -f7 x2 > x3
grep -Friv 'B' x3 > x4
sed 's/^S//' x4 > x5
sed 's/^R//' x5 > x6
sed 's/^D//' x6 > x7
sed 's/...$//' < x7 > x8
sort x8 | uniq > ifremerprofiles
rm x1 x2 x3 x4 x5 x6 x7 x8

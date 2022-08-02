# usage: bash freshrebuild.sh
# generates a file with lines appropriate for inputting to translateProfile, covering all profile data in /ifremer
# intended for performing a full database rebuild from an existing rsync.

find /ifremer -type f > x
grep '\/profiles\/.*\.nc' x > xx
grep -v 'profiles/B' xx > xxx
python freshrebuild.py xxx
sort /tmp/profileUpdates.txt | uniq > /tmp/profiles2translate
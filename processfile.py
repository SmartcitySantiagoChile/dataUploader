import os
import subprocess

# this works
#data = raw_input("Enter path to the data file: ")
#/home/osboxes/Downloads/logstash-py/test.od
#/home/osboxes/Downloads/logstash-py/2017-07-31.od
#conf = raw_input("Enter path to the conf file: ")
#/etc/logstash/conf.d/odbyroute-logstash.conf
#sincedb = raw_input("Enter path to sincedb file: ")
#/home/osboxes/Downloads/logstash-py/logstash_odbyroute.sincedb
# till here

data="/home/osboxes/Downloads/logstash-py/test.od"
conf="/etc/logstash/conf.d/odbyroute-logstash.conf"
sincedb="/home/osboxes/Downloads/logstash-py/logstash_odbyroute.sincedb"

# If sincedb doesn't exist, create it (logstash does this, but when?)

# Paths in conf file should be the ones given by the user

# Run logstash
#systemctl start logstash
cmd = ("sudo /usr/share/logstash/bin/logstash --path.settings /etc/logstash -f " + conf)
os.system(cmd)

# Current position in the file
#current = subprocess.check_output("cat syscall_list.txt | grep f89e7000 | awk '{print $2}'", shell=True)
current = os.system("tail -1 $SINCEDB | awk '{printf $4}'")
print(current)
#os.system("echo "CURRENT: $CURRENT"")
#os.system("FILESIZE="$(stat  -c '%s' $DATA)"")
#os.system("echo "FILESIZE: $FILESIZE"")

#print(CURRENT)

#while : ; do
#  echo "Working"
#  echo "Current: $CURRENT"
#  if [[ $CURRENT = $FILESIZE ]]; then
#      echo "Done"
#      exit 0
#  fi
#done

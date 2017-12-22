import os
import subprocess
import time

# Ask the user for the paths
data = raw_input("Enter path to the data file: ")
conf = raw_input("Enter path to the conf file: ")
sincedb = raw_input("Enter path to sincedb file: ")

# Paths in conf file should be the ones given by the user
#data="/home/osboxes/Downloads/profile-test/2017-07-31.perfiles"
#conf="/home/osboxes/Downloads/profile-test/profile-logstash.conf"
#sincedb="/home/osboxes/Downloads/profile-test/profile-logstash.sincedb"

# Current position in the file
current = 0
# Total size of the data file
filesize = int(filter(str.isdigit, subprocess.check_output(["stat", "-c", "%s'", data])))

# Run logstash
logstash = subprocess.Popen(['/usr/share/logstash/bin/logstash', '-f', conf])
print("Starting Logstash...")
time.sleep(15) # give it some time to start
print("Please wait...")
time.sleep(15)

while (current != filesize):
    try:
        current = int(filter(str.isdigit, subprocess.check_output("tail -1 " + sincedb + " | awk '{printf $4}'", shell=True)))
        print("current: " + str(current))
    except (OSError, ValueError):
	    pass
    time.sleep(15) #if you don't wait before rechecking, Logstash will take longer to run and may be unable to write to sincedb as the file is constantly being read

logstash.terminate()
#os.system("rm " + sincedb)
print("Success")

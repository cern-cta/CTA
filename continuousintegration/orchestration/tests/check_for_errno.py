#!/usr/bin/env python3
import subprocess
from subprocess import PIPE
import sys

failed_requests = []

print('Running check_for_errno.py')

with open('/var/log/eos/mgm/WFE.log', 'r') as f:
    # Look into the WFE the reported ''failed'' evict events
    for line in f:
        if line.find("Failed to issue evict for evict_prepare event") != -1:
          # Trim white spaces and split
          clean_line = ' '.join(line.split())
          clean_line = clean_line.split(' ')

          # Get field we want
          fxid=clean_line[5].split('=')[1]
          file_path=clean_line[6].split('=')[1]

          # Check if the file was actually evicted
          # eos ls -y
          #result = subprocess.run(['eos', 'root://ctaeos',
          #                           'ls', '-y', '{}/0/'.format(sys.argv[1]),
          #                           '|', 'grep','d1::t1',
          #                           '|', 'grep', '{}'.format()],
          #                        capture_output=True,
          #                        text=True)

          # Check if fxid exists under /fst/0000000/fxid
          cmd_str = 'ls /fst/00000000/ | grep {} | wc -l'.format(fxid)
          result = subprocess.run([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)

          if result.stdout.decode('ascii').strip().find("0") != -1:
              was_evicted=True
          else:
              was_evicted=False

          failed_requests.append([fxid, file_path, was_evicted])

if len(failed_requests):
    print("WFE logs reported failing to issue {} evict  requests".format(len(failed_requests)))

    for failed_req in failed_requests:
        print("fxid:{}\tfilepath:{}\twas_evicted:{}".format(failed_req[0], failed_req[1], failed_req[2]))

    print('Checking errno that generated the failure: ')
    for failed_req in failed_requests:
        with open('/var/log/eos/mgm/xrdlog.mgm', 'r') as f:
            for line in f:
                # Check if errno is set to something different than 0
                if (line.find(failed_req[1])  != -1
                     and line.find("errno=")  != -1
                     and line.find("errno=0") == -1):
                    # Clean log line
                    clean_log_split = ' '.join(line.split())
                    clean_log_split = clean_log_split(' ')
                    function = clean_log_split[3]
                    source = clean_log_split[8]
                    errno_setter = clean_log_split[15]
                    errno = clean_log_split[-1]

                    print('{}\tset_by={}\t{}\t{}\twas_evicted={}'.format(
                        errno, errno_setter[:-1], source, function, failed_req[2]
                    ))
                    # The first match of the conditions already
                    # tells us were errno was set so we can jump
                    # to the next error
                    break
    exit(1)
else:
    print('No errors found')
    exit(0)

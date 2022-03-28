#!/usr/bin/python3.6

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright(C) 2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.


import argparse
import subprocess
import os
import copy
import traceback
import re

# Instantiate the parser and parse command line
parser = argparse.ArgumentParser(
    description='Utility program to abort a retrieve on an EOS+CTA system.')
parser.add_argument('--eos-instance', required=True)
parser.add_argument('--eos-poweruser', required=True)
parser.add_argument('--eos-dir', required=True)
parser.add_argument('--subdir', required=True)
parser.add_argument('--file', required=True)
parser.add_argument('--error-dir', required=True)
options = parser.parse_args()

# Construct various parameters.
filepath = options.eos_dir + '/' + options.subdir + '/' + options.file
xattrgeterrorfilepath = options.error_dir + '/' + 'XATTRGET2_'
xattrgeterrorfilepath += options.subdir + '_' + options.file

aborterrorfilepath = options.error_dir + '/' + 'PREPAREABORT_'
aborterrorfilepath += options.subdir + '_' + options.file

# Get the xattr of the file
# Prepare the environment
env = copy.deepcopy(os.environ)
env['XRD_LOGLEVEL'] = 'Dump'
env['KRB5CCNAME'] = '/tmp/' + options.eos_poweruser + '/krb5cc_0'
env['XrdSecPROTOCOL'] = 'krb5'
try:
  xattrRes = subprocess.run(
      ['xrdfs', options.eos_instance, 'query', 'opaquefile',
          filepath+'?mgm.pcmd=xattr&mgm.subcmd=get&mgm.xattrname=sys.retrieve.req_id'],
      env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  xattrRes.check_returncode()
except subprocess.CalledProcessError as cpe:
  print('ERROR with xrdfs query for file ' + options.file + ': '+str(cpe.stderr)+' full logs in ' +
      xattrgeterrorfilepath)
  traceback.print_exc()
  print(cpe.stdout)
  errFile = open(xattrgeterrorfilepath, 'w')
  errFile.write(str(xattrRes.stderr))
  errFile.close()
except Exception as e:
  print('ERROR with xrdfs query for file ' + options.file + ': got exception of type: ' +
      str(type(e)) + '['.join(arg + ', ' for arg in e.args) +'] full logs in ' +  xattrgeterrorfilepath)
  traceback.print_exc()
  errFile = open(xattrgeterrorfilepath, 'w')
  errFile.write(str(xattrRes.stderr))
  errFile.close()
# OK, worked...
requestId=xattrRes.stdout.decode('utf8')
#print('requestId(pre-match)=' + requestId)
reComp=re.compile(r'.*value=(.*)')
reExec=reComp.match(requestId)
requestId = re.compile(r'.*value=(.*)').match(requestId).group(1)
#print('requestId(post-match)=' + requestId)

# We can now abort the prepare
try:
  # print('Will xrdfs ' + str(options.eos_instance).rstrip() + 'prepare -a ' + requestId + ' ' + filepath)
  abortRes = subprocess.run(
      ['xrdfs', str(options.eos_instance).rstrip(), 'prepare', '-a', requestId, filepath],
      env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  abortRes.check_returncode()
  # print('abortRes.stdout')
  # print(abortRes.stdout.decode('utf8'))
  # print('abortRes.stderr')
  # print(abortRes.stderr.decode('utf8'))
except subprocess.CalledProcessError as cpe:
  print('ERROR with xrdfs prepare -a ' + options.file + '(' + str(cpe.returncode) + ') full logs in ' +
      aborterrorfilepath)
  traceback.print_exc()
  print('cpe.stderr:')
  print(cpe.stderr.decode('utf8'))
  print('cpe.stdout')
  print(cpe.stdout.decode('utf8'))
  errFile = open(aborterrorfilepath, 'w')
  errFile.write(str(abortRes.stderr))
  errFile.close()
except Exception as e:
  print('ERROR with xrdfs prepare -a for file ' + options.file + ': got exception of type: ' +
        str(type(e)) + '['.join(arg + ', ' for arg in e.args) +'] full logs in '+ aborterrorfilepath)
  traceback.print_exc()
  errFile = open(aborterrorfilepath, 'w')
  errFile.write(str(e))
  errFile.close()

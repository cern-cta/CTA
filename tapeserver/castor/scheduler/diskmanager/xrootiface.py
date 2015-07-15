#!/usr/bin/python
#/******************************************************************************
# *                   xrootiface.py
# *
# * This file is part of the Castor project.
# * See http://castor.web.cern.ch/castor
# *
# * Copyright (C) 2003  CERN
# * This program is free software; you can redistribute it and/or
# * modify it under the terms of the GNU General Public License
# * as published by the Free Software Foundation; either version 2
# * of the License, or (at your option) any later version.
# * This program is distributed in the hope that it will be useful,
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# * You should have received a copy of the GNU General Public License
# * along with this program; if not, write to the Free Software
# * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# *
# * common functions to interface to xroot
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

"""common functions to interface to xroot, including building xroot URLs and parsing xroot tuples
   describing ongoing transfers as they come from querying an xroot server.
   Requires python-crypto to work."""

import os, time
import Crypto.Hash.SHA as SHA1
import Crypto.PublicKey.RSA as RSA
import Crypto.Signature.PKCS1_v1_5 as PKCS1
import base64
import castor_tools
from transfer import Transfer, TransferType, TapeTransfer, TapeTransferType, D2DTransfer, D2DTransferType, RunningTransfer

def signBase64(content, RSAKey):
  '''signs content with the given key and encode the result in base64'''
  contentHash = SHA1.new(content)
  signer = PKCS1.new(RSAKey)
  signature = signer.sign(contentHash)
  return base64.b64encode(signature)

def buildXrootURL(diskserver, path, transferId, transferType):
  '''Builds a valid xroot url for the given path on the given diskserver'''
  config = castor_tools.castorConf()
  # base url and key parameter
  url = 'root://'+diskserver+':1095//' + path + '?'
  opaque_dict = {'castor.pfn1' : path,
                 'castor.pfn2' : '' if transferId == None else '0:' + \
                                 str(config.getValue('DiskManager', 'MoverHandlerPort', 15511)) + ':' + transferId,
                 'castor.txtype' : transferType,
                 'castor.accessop' : '0',   # read from remote
                 'castor.exptime' : str(int(time.time()) + 3600)}

  # get Xroot RSA key
  keyFile = config.getValue('XROOT', 'PrivateKey', '/etc/castor/xrd_key.pem')
  key = RSA.importKey(open(keyFile, 'r').read())
  # sign opaque part obtained by concatenating the values
  opaque_token = ''.join([opaque_dict['castor.pfn1'],
                          opaque_dict['castor.pfn2'],
                          opaque_dict['castor.accessop'],
                          opaque_dict['castor.exptime'],
                          opaque_dict['castor.txtype']])
  signature = signBase64(opaque_token, key)
  opaque = ""

  # build the opaque info
  for key, val in opaque_dict.iteritems():
    opaque += key + '=' + val + '&'

  url += opaque + 'castor.signature=' + signature
  return url

def xrootTupleToTransfer(scheduler, xrootTuple):
  '''Parse an xroot tuple describing a transfer and returns a running or tape transfer object.
  The tuple must have the following format (cf. moverhandler.py):
  ('tident', 'physicalPath', 'transferType', 'isWriteFlag', 'transferId')
  '''
  tident, physicalPath, transferType, isWriteFlag, transferid = xrootTuple
  # for the time being xroot does not provide the start time of the transfer, therefore take current time
  startTime = time.time()
  # extract the relevant information
  clientHost = tident.split('@')[1]   # this is the host part
  fid, nshost = os.path.basename(physicalPath).split('@')
  nshost = nshost.split('.')[0]
  fileid = (nshost, int(fid))
  mountPoint = physicalPath.rsplit(os.sep, 2)[0] + os.sep
  if transferType == 'tape':
    return TapeTransfer(transferid,
	                      TapeTransferType.RECALL if isWriteFlag == '1' else TapeTransferType.MIGRATION,
	                      startTime, clientHost, fileid, mountPoint)
  elif transferType == 'user':
    t = Transfer(transferid, '-', fileid, -1, -1, 'unknownSvcClass', startTime, 'xroot', 0, 0, 'w' if isWriteFlag == '1' else 'r')
  elif transferType[0:3] == 'd2d':
    # any disk-to-disk copy goes here
    t = D2DTransfer(transferid, '-', fileid, -1, -1, 'unknownSvcClass', startTime,
                    TransferType.D2DDST if isWriteFlag == '1' else TransferType.D2DSRC,
                    D2DTransferType.toType(transferType[3:]), clientHost, mountPoint)
  else:
    # any other transferType is unknown
    raise ValueError('No valid transfer type %s' % transferType)
  return RunningTransfer(scheduler, None, startTime, t, physicalPath)

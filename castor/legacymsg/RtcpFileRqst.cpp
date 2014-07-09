/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 * 
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/legacymsg/RtcpFileRqst.hpp"

#include <string.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::legacymsg::RtcpFileRqst::RtcpFileRqst() throw():
  volReqId(0),
  jobId(0),
  stageSubReqId(0),
  umask(0),
  positionMethod(0),
  tapeFseq(0),
  diskFseq(0),
  blockSize(0),
  recordLength(0),
  retention(0),
  defAlloc(0),
  rtcpErrAction(0),
  tpErrAction(0),
  convert_noLongerUsed(0),
  checkFid(0),
  concat(0),
  procStatus(0),
  cprc(0),
  tStartPosition(0),
  tEndPosition(0),
  tStartTransferDisk(0),
  tEndTransferDisk(0),
  tStartTransferTape(0),
  tEndTransferTape(0),
  offset(0),
  bytesIn(0),
  bytesOut(0),
  hostBytes(0),
  nbRecs(0),
  maxNbRec(0),
  maxSize(0),
  startSize(0),
  stgReqId(nullCuuid) {
  memset(filePath, '\0', sizeof(filePath));
  memset(tapePath, '\0', sizeof(tapePath));
  memset(recfm_noLongerUsed, '\0', sizeof(recfm_noLongerUsed));
  memset(fid, '\0', sizeof(fid));
  memset(ifce, '\0', sizeof(ifce));
  memset(stageId, '\0', sizeof(stageId));
  memset(blockId,'\0', sizeof(blockId));
}

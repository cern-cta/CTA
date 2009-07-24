/******************************************************************************
 *                      castor/tape/aggregator/LogHelper.cpp
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
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/dlf/Dlf.hpp"
#include "castor/tape/aggregator/LogHelper.hpp"
#include "castor/tape/utils/utils.hpp"


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RcpJobRqstMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId"       , volReqId            ),
    castor::dlf::Param("socketFd"       , socketFd            ),
    castor::dlf::Param("tapeRequestId"  , body.tapeRequestId  ),
    castor::dlf::Param("clientPort"     , body.clientPort     ),
    castor::dlf::Param("clientEuid"     , body.clientEuid     ),
    castor::dlf::Param("clientEgid"     , body.clientEgid     ),
    castor::dlf::Param("clientHost"     , body.clientHost     ),
    castor::dlf::Param("deviceGroupName", body.deviceGroupName),
    castor::dlf::Param("driveName"      , body.driveName      ),
    castor::dlf::Param("clientUserName" , body.clientUserName )};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RcpJobReplyMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId"    , volReqId         ),
    castor::dlf::Param("socketFd"    , socketFd         ),
    castor::dlf::Param("status"      , body.status      ),
    castor::dlf::Param("errorMessage", body.errorMessage)};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RtcpTapeRqstErrMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId"      , volReqId           ),
    castor::dlf::Param("socketFd"      , socketFd           ),
    castor::dlf::Param("vid"           , body.vid           ),
    castor::dlf::Param("vsn"           , body.vsn           ),
    castor::dlf::Param("label"         , body.label         ),
    castor::dlf::Param("devtype"       , body.devtype       ),
    castor::dlf::Param("density"       , body.density       ),
    castor::dlf::Param("unit"          , body.unit          ),
    castor::dlf::Param("volReqId"      , body.volReqId      ),
    castor::dlf::Param("jobId"         , body.jobId         ),
    castor::dlf::Param("mode"          , body.mode          ),
    castor::dlf::Param("start_file"    , body.start_file    ),
    castor::dlf::Param("end_file"      , body.end_file      ),
    castor::dlf::Param("side"          , body.side          ),
    castor::dlf::Param("tprc"          , body.tprc          ),
    castor::dlf::Param("tStartRequest" , body.tStartRequest ),
    castor::dlf::Param("tEndRequest"   , body.tEndRequest   ),
    castor::dlf::Param("tStartRtcpd"   , body.tStartRtcpd   ),
    castor::dlf::Param("tStartMount"   , body.tStartMount   ),
    castor::dlf::Param("tEndMount"     , body.tEndMount     ),
    castor::dlf::Param("tStartUnmount" , body.tStartUnmount ),
    castor::dlf::Param("tEndUnmount"   , body.tEndUnmount   ),
    castor::dlf::Param("rtcpReqId"     , body.rtcpReqId     ),
    castor::dlf::Param("err.errorMsg"  , body.err.errorMsg  ),
    castor::dlf::Param("err.severity"  , body.err.severity  ),
    castor::dlf::Param("err.errorCode" , body.err.errorCode ),
    castor::dlf::Param("err.maxTpRetry", body.err.maxTpRetry),
    castor::dlf::Param("err.maxCpRetry", body.err.maxCpRetry)};

  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RtcpTapeRqstMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId"      , volReqId           ),
    castor::dlf::Param("socketFd"      , socketFd           ),
    castor::dlf::Param("vid"           , body.vid           ),
    castor::dlf::Param("vsn"           , body.vsn           ),
    castor::dlf::Param("label"         , body.label         ),
    castor::dlf::Param("devtype"       , body.devtype       ),
    castor::dlf::Param("density"       , body.density       ),
    castor::dlf::Param("unit"          , body.unit          ),
    castor::dlf::Param("volReqId"      , body.volReqId      ),
    castor::dlf::Param("jobId"         , body.jobId         ),
    castor::dlf::Param("mode"          , body.mode          ),
    castor::dlf::Param("start_file"    , body.start_file    ),
    castor::dlf::Param("end_file"      , body.end_file      ),
    castor::dlf::Param("side"          , body.side          ),
    castor::dlf::Param("tprc"          , body.tprc          ),
    castor::dlf::Param("tStartRequest" , body.tStartRequest ),
    castor::dlf::Param("tEndRequest"   , body.tEndRequest   ),
    castor::dlf::Param("tStartRtcpd"   , body.tStartRtcpd   ),
    castor::dlf::Param("tStartMount"   , body.tStartMount   ),
    castor::dlf::Param("tEndMount"     , body.tEndMount     ),
    castor::dlf::Param("tStartUnmount" , body.tStartUnmount ),
    castor::dlf::Param("tEndUnmount"   , body.tEndUnmount   ),
    castor::dlf::Param("rtcpReqId"     , body.rtcpReqId     )};

  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RtcpFileRqstErrMsgBody &body) throw() {

  // 32-bits = 1 x '0' + 1 x 'x' + 8 x hex + 1 x '/0' = 11 byte string
  char checksumHex[11];
  checksumHex[0] = '0';
  checksumHex[1] = 'x';
  utils::toHex(body.segAttr.segmCksum, &checksumHex[2], 9);

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId"            , volReqId                 ),
    castor::dlf::Param("socketFd"            , socketFd                 ),
    castor::dlf::Param("filePath"            , body.filePath            ),
    castor::dlf::Param("tapePath"            , body.tapePath            ),
    castor::dlf::Param("recfm"               , body.recfm               ),
    castor::dlf::Param("fid"                 , body.fid                 ),
    castor::dlf::Param("ifce"                , body.ifce                ),
    castor::dlf::Param("stageId"             , body.stageId             ),
    castor::dlf::Param("volReqId"            , body.volReqId            ),
    castor::dlf::Param("jobId"               , body.jobId               ),
    castor::dlf::Param("stageSubReqId"       , body.stageSubReqId       ),
    castor::dlf::Param("umask"               , body.umask               ),
    castor::dlf::Param("positionMethod"      , body.positionMethod      ),
    castor::dlf::Param("tapeFseq"            , body.tapeFseq            ),
    castor::dlf::Param("diskFseq"            , body.diskFseq            ),
    castor::dlf::Param("blockSize"           , body.blockSize           ),
    castor::dlf::Param("recordLength"        , body.recordLength        ),
    castor::dlf::Param("retention"           , body.retention           ),
    castor::dlf::Param("defAlloc"            , body.defAlloc            ),
    castor::dlf::Param("rtcpErrAction"       , body.rtcpErrAction       ),
    castor::dlf::Param("tpErrAction"         , body.tpErrAction         ),
    castor::dlf::Param("convert"             , body.convert             ),
    castor::dlf::Param("checkFid"            , body.checkFid            ),
    castor::dlf::Param("concat"              , body.concat              ),
    castor::dlf::Param("procStatus"          , body.procStatus          ),
    castor::dlf::Param("cprc"                , body.cprc                ),
    castor::dlf::Param("tStartPosition"      , body.tStartPosition      ),
    castor::dlf::Param("tEndPosition"        , body.tEndPosition        ),
    castor::dlf::Param("tStartTransferDisk"  , body.tStartTransferDisk  ),
    castor::dlf::Param("tEndTransferDisk"    , body.tEndTransferDisk    ),
    castor::dlf::Param("tStartTransferTape"  , body.tStartTransferTape  ),
    castor::dlf::Param("tEndTransferTape"    , body.tEndTransferTape    ),
    castor::dlf::Param("blockId[0]"          , body.blockId[0]          ),
    castor::dlf::Param("blockId[1]"          , body.blockId[1]          ),
    castor::dlf::Param("blockId[2]"          , body.blockId[2]          ),
    castor::dlf::Param("blockId[3]"          , body.blockId[3]          ),
    castor::dlf::Param("offset"              , body.offset              ),
    castor::dlf::Param("bytesIn"             , body.bytesIn             ),
    castor::dlf::Param("bytesOut"            , body.bytesOut            ),
    castor::dlf::Param("hostBytes"           , body.hostBytes           ),
    castor::dlf::Param("nbRecs"              , body.nbRecs              ),
    castor::dlf::Param("maxNbRec"            , body.maxNbRec            ),
    castor::dlf::Param("maxSize"             , body.maxSize             ),
    castor::dlf::Param("startSize"           , body.startSize           ),
    castor::dlf::Param("segAttr.nameServerHostName",
      body.segAttr.nameServerHostName),
    castor::dlf::Param("segAttr.segmCksumAlgorithm",
      body.segAttr.segmCksumAlgorithm),
    castor::dlf::Param("segAttr.segmCksum"   , checksumHex              ),
    castor::dlf::Param("segAttr.castorFileId", body.segAttr.castorFileId),
    castor::dlf::Param("stgReqId"            , body.stgReqId            ),
    castor::dlf::Param("err.errorMsg"        , body.err.errorMsg        ),
    castor::dlf::Param("err.severity"        , body.err.severity        ),
    castor::dlf::Param("err.errorCode"       , body.err.errorCode       ),
    castor::dlf::Param("err.maxTpRetry"      , body.err.maxTpRetry      ),
    castor::dlf::Param("err.maxCpRetry"      , body.err.maxCpRetry      )};

  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RtcpFileRqstMsgBody &body) throw() {

  // 32-bits = 1 x '0' + 1 x 'x' + 8 x hex + 1 x '/0' = 11 byte string
  char checksumHex[11];
  checksumHex[0] = '0';
  checksumHex[1] = 'x';
  utils::toHex(body.segAttr.segmCksum, &checksumHex[2], 9);

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId"            , volReqId                 ),
    castor::dlf::Param("socketFd"            , socketFd                 ),
    castor::dlf::Param("filePath"            , body.filePath            ),
    castor::dlf::Param("tapePath"            , body.tapePath            ),
    castor::dlf::Param("recfm"               , body.recfm               ),
    castor::dlf::Param("fid"                 , body.fid                 ),
    castor::dlf::Param("ifce"                , body.ifce                ),
    castor::dlf::Param("stageId"             , body.stageId             ),
    castor::dlf::Param("volReqId"            , body.volReqId            ),
    castor::dlf::Param("jobId"               , body.jobId               ),
    castor::dlf::Param("stageSubReqId"       , body.stageSubReqId       ),
    castor::dlf::Param("umask"               , body.umask               ),
    castor::dlf::Param("positionMethod"      , body.positionMethod      ),
    castor::dlf::Param("tapeFseq"            , body.tapeFseq            ),
    castor::dlf::Param("diskFseq"            , body.diskFseq            ),
    castor::dlf::Param("blockSize"           , body.blockSize           ),
    castor::dlf::Param("recordLength"        , body.recordLength        ),
    castor::dlf::Param("retention"           , body.retention           ),
    castor::dlf::Param("defAlloc"            , body.defAlloc            ),
    castor::dlf::Param("rtcpErrAction"       , body.rtcpErrAction       ),
    castor::dlf::Param("tpErrAction"         , body.tpErrAction         ),
    castor::dlf::Param("convert"             , body.convert             ),
    castor::dlf::Param("checkFid"            , body.checkFid            ),
    castor::dlf::Param("concat"              , body.concat              ),
    castor::dlf::Param("procStatus"          ,
      utils::procStatusToString(body.procStatus)),
    castor::dlf::Param("cprc"                , body.cprc                ),
    castor::dlf::Param("tStartPosition"      , body.tStartPosition      ),
    castor::dlf::Param("tEndPosition"        , body.tEndPosition        ),
    castor::dlf::Param("tStartTransferDisk"  , body.tStartTransferDisk  ),
    castor::dlf::Param("tEndTransferDisk"    , body.tEndTransferDisk    ),
    castor::dlf::Param("tStartTransferTape"  , body.tStartTransferTape  ),
    castor::dlf::Param("tEndTransferTape"    , body.tEndTransferTape    ),
    castor::dlf::Param("blockId[0]"          , body.blockId[0]          ),
    castor::dlf::Param("blockId[1]"          , body.blockId[1]          ),
    castor::dlf::Param("blockId[2]"          , body.blockId[2]          ),
    castor::dlf::Param("blockId[3]"          , body.blockId[3]          ),
    castor::dlf::Param("offset"              , body.offset              ),
    castor::dlf::Param("bytesIn"             , body.bytesIn             ),
    castor::dlf::Param("bytesOut"            , body.bytesOut            ),
    castor::dlf::Param("hostBytes"           , body.hostBytes           ),
    castor::dlf::Param("nbRecs"              , body.nbRecs              ),
    castor::dlf::Param("maxNbRec"            , body.maxNbRec            ),
    castor::dlf::Param("maxSize"             , body.maxSize             ),
    castor::dlf::Param("startSize"           , body.startSize           ),
    castor::dlf::Param("segAttr.nameServerHostName",
      body.segAttr.nameServerHostName),
    castor::dlf::Param("segAttr.segmCksumAlgorithm",
      body.segAttr.segmCksumAlgorithm),
    castor::dlf::Param("segAttr.segmCksum"   , checksumHex              ),
    castor::dlf::Param("segAttr.castorFileId", body.segAttr.castorFileId),
    castor::dlf::Param("stgReqId"            , body.stgReqId            )};

  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const GiveOutpMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId", volReqId    ),
    castor::dlf::Param("socketFd", socketFd    ),
    castor::dlf::Param("message" , body.message)};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RtcpNoMoreRequestsMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId", volReqId),
    castor::dlf::Param("socketFd", socketFd)};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RtcpAbortMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId", volReqId),
    castor::dlf::Param("socketFd", socketFd)};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RtcpDumpTapeRqstMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId"   , volReqId        ),
    castor::dlf::Param("socketFd"   , socketFd        ),
    castor::dlf::Param("maxBytes"   , body.maxBytes   ),
    castor::dlf::Param("blockSize"  , body.blockSize  ),
    castor::dlf::Param("convert"    , body.convert    ),
    castor::dlf::Param("tpErrAction", body.tpErrAction),
    castor::dlf::Param("startFile"  , body.startFile  ),
    castor::dlf::Param("maxFiles"   , body.maxFiles   ),
    castor::dlf::Param("fromBlock"  , body.fromBlock  ),
    castor::dlf::Param("toBlock"    , body.toBlock    )};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsg
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsg(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const tapegateway::Volume &msg) throw() {

 castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", msg.mountTransactionId()),
      castor::dlf::Param("vid"               , msg.vid()               ),
      castor::dlf::Param("density"           , msg.density()           ),
      castor::dlf::Param("label"             , msg.label()             ),
      castor::dlf::Param("dumpTapeMaxBytes"  , msg.dumpTapeMaxBytes()  ),
      castor::dlf::Param("dumpTapeBlockSize" , msg.dumpTapeBlockSize() ),
      castor::dlf::Param("dumpTapeConverter" , msg.dumpTapeConverter() ),
      castor::dlf::Param("dumpTapeErrAction" , msg.dumpTapeErrAction() ),
      castor::dlf::Param("dumpTapeStartFile" , msg.dumpTapeStartFile() ),
      castor::dlf::Param("dumpTapeMaxFile"   , msg.dumpTapeMaxFile()   ),
      castor::dlf::Param("dumpTapeFromBlock" , msg.dumpTapeFromBlock() ),
      castor::dlf::Param("dumpTapeToBlock"   , msg.dumpTapeToBlock()   ),
      castor::dlf::Param("id"                , msg.id()                ),
      castor::dlf::Param("mode"              ,
        utils::volumeModeToString(msg.mode()))};
    castor::dlf::dlf_writep(cuuid, severity, message_no, params);

}

/******************************************************************************
 *                      castor/tape/tapebridge/LogHelper.cpp
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
#include "castor/tape/tapebridge/LogHelper.hpp"
#include "castor/tape/utils/utils.hpp"


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const legacymsg::RtcpJobRqstMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId", volReqId            ),
    castor::dlf::Param("volReqId"          , volReqId            ),
    castor::dlf::Param("socketFd"          , socketFd            ),
    castor::dlf::Param("volReqId"          , body.volReqId       ),
    castor::dlf::Param("clientPort"        , body.clientPort     ),
    castor::dlf::Param("clientEuid"        , body.clientEuid     ),
    castor::dlf::Param("clientEgid"        , body.clientEgid     ),
    castor::dlf::Param("clientHost"        , body.clientHost     ),
    castor::dlf::Param("dgn"               , body.dgn            ),
    castor::dlf::Param("driveUnit"         , body.driveUnit      ),
    castor::dlf::Param("clientUserName"    , body.clientUserName )};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const legacymsg::RtcpJobReplyMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId", volReqId         ),
    castor::dlf::Param("volReqId"          , volReqId         ),
    castor::dlf::Param("socketFd"          , socketFd         ),
    castor::dlf::Param("status"            , body.status      ),
    castor::dlf::Param("errorMessage"      , body.errorMessage)};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const legacymsg::RtcpTapeRqstErrMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId", volReqId           ),
    castor::dlf::Param("volReqId"          , volReqId           ),
    castor::dlf::Param("socketFd"          , socketFd           ),
    castor::dlf::Param("TPVID"             , body.vid           ),
    castor::dlf::Param("vsn"               , body.vsn           ),
    castor::dlf::Param("label"             , body.label         ),
    castor::dlf::Param("devtype"           , body.devtype       ),
    castor::dlf::Param("density"           , body.density       ),
    castor::dlf::Param("unit"              , body.unit          ),
    castor::dlf::Param("volReqId"          , body.volReqId      ),
    castor::dlf::Param("jobId"             , body.jobId         ),
    castor::dlf::Param("mode"              , body.mode          ),
    castor::dlf::Param("start_file"        , body.start_file    ),
    castor::dlf::Param("end_file"          , body.end_file      ),
    castor::dlf::Param("side"              , body.side          ),
    castor::dlf::Param("tprc"              , body.tprc          ),
    castor::dlf::Param("tStartRequest"     , body.tStartRequest ),
    castor::dlf::Param("tEndRequest"       , body.tEndRequest   ),
    castor::dlf::Param("tStartRtcpd"       , body.tStartRtcpd   ),
    castor::dlf::Param("tStartMount"       , body.tStartMount   ),
    castor::dlf::Param("tEndMount"         , body.tEndMount     ),
    castor::dlf::Param("tStartUnmount"     , body.tStartUnmount ),
    castor::dlf::Param("tEndUnmount"       , body.tEndUnmount   ),
    castor::dlf::Param("rtcpReqId"         , body.rtcpReqId     ),
    castor::dlf::Param("err.errorMsg"      , body.err.errorMsg  ),
    castor::dlf::Param("err.severity"      , body.err.severity  ),
    castor::dlf::Param("err.errorCode"     , body.err.errorCode ),
    castor::dlf::Param("err.maxTpRetry"    , body.err.maxTpRetry),
    castor::dlf::Param("err.maxCpRetry"    , body.err.maxCpRetry)};

  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const legacymsg::RtcpTapeRqstMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId", volReqId           ),
    castor::dlf::Param("volReqId"          , volReqId           ),
    castor::dlf::Param("socketFd"          , socketFd           ),
    castor::dlf::Param("TPVID"             , body.vid           ),
    castor::dlf::Param("vsn"               , body.vsn           ),
    castor::dlf::Param("label"             , body.label         ),
    castor::dlf::Param("devtype"           , body.devtype       ),
    castor::dlf::Param("density"           , body.density       ),
    castor::dlf::Param("unit"              , body.unit          ),
    castor::dlf::Param("volReqId"          , body.volReqId      ),
    castor::dlf::Param("jobId"             , body.jobId         ),
    castor::dlf::Param("mode"              , body.mode          ),
    castor::dlf::Param("start_file"        , body.start_file    ),
    castor::dlf::Param("end_file"          , body.end_file      ),
    castor::dlf::Param("side"              , body.side          ),
    castor::dlf::Param("tprc"              , body.tprc          ),
    castor::dlf::Param("tStartRequest"     , body.tStartRequest ),
    castor::dlf::Param("tEndRequest"       , body.tEndRequest   ),
    castor::dlf::Param("tStartRtcpd"       , body.tStartRtcpd   ),
    castor::dlf::Param("tStartMount"       , body.tStartMount   ),
    castor::dlf::Param("tEndMount"         , body.tEndMount     ),
    castor::dlf::Param("tStartUnmount"     , body.tStartUnmount ),
    castor::dlf::Param("tEndUnmount"       , body.tEndUnmount   ),
    castor::dlf::Param("rtcpReqId"         , body.rtcpReqId     )};

  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const legacymsg::RtcpFileRqstErrMsgBody &body) throw() {

  std::ostringstream checksumHexStream;
  checksumHexStream << "0x" << std::hex << body.rqst.segAttr.segmCksum;

  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId"  , volReqId                      ),
    castor::dlf::Param("volReqId"            , volReqId                      ),
    castor::dlf::Param("socketFd"            , socketFd                      ),
    castor::dlf::Param("filePath"            , body.rqst.filePath            ),
    castor::dlf::Param("tapePath"            , body.rqst.tapePath            ),
    castor::dlf::Param("recfm"               , body.rqst.recfm               ),
    castor::dlf::Param("fid"                 , body.rqst.fid                 ),
    castor::dlf::Param("ifce"                , body.rqst.ifce                ),
    castor::dlf::Param("stageId"             , body.rqst.stageId             ),
    castor::dlf::Param("volReqId"            , body.rqst.volReqId            ),
    castor::dlf::Param("jobId"               , body.rqst.jobId               ),
    castor::dlf::Param("stageSubReqId"       , body.rqst.stageSubReqId       ),
    castor::dlf::Param("umask"               , body.rqst.umask               ),
    castor::dlf::Param("positionMethod"      , body.rqst.positionMethod      ),
    castor::dlf::Param("tapeFseq"            , body.rqst.tapeFseq            ),
    castor::dlf::Param("diskFseq"            , body.rqst.diskFseq            ),
    castor::dlf::Param("blockSize"           , body.rqst.blockSize           ),
    castor::dlf::Param("recordLength"        , body.rqst.recordLength        ),
    castor::dlf::Param("retention"           , body.rqst.retention           ),
    castor::dlf::Param("defAlloc"            , body.rqst.defAlloc            ),
    castor::dlf::Param("rtcpErrAction"       , body.rqst.rtcpErrAction       ),
    castor::dlf::Param("tpErrAction"         , body.rqst.tpErrAction         ),
    castor::dlf::Param("convert"             , body.rqst.convert             ),
    castor::dlf::Param("checkFid"            , body.rqst.checkFid            ),
    castor::dlf::Param("concat"              , body.rqst.concat              ),
    castor::dlf::Param("procStatus"          , body.rqst.procStatus          ),
    castor::dlf::Param("cprc"                , body.rqst.cprc                ),
    castor::dlf::Param("tStartPosition"      , body.rqst.tStartPosition      ),
    castor::dlf::Param("tEndPosition"        , body.rqst.tEndPosition        ),
    castor::dlf::Param("tStartTransferDisk"  , body.rqst.tStartTransferDisk  ),
    castor::dlf::Param("tEndTransferDisk"    , body.rqst.tEndTransferDisk    ),
    castor::dlf::Param("tStartTransferTape"  , body.rqst.tStartTransferTape  ),
    castor::dlf::Param("tEndTransferTape"    , body.rqst.tEndTransferTape    ),
    castor::dlf::Param("blockId[0]"          , body.rqst.blockId[0]          ),
    castor::dlf::Param("blockId[1]"          , body.rqst.blockId[1]          ),
    castor::dlf::Param("blockId[2]"          , body.rqst.blockId[2]          ),
    castor::dlf::Param("blockId[3]"          , body.rqst.blockId[3]          ),
    castor::dlf::Param("offset"              , body.rqst.offset              ),
    castor::dlf::Param("bytesIn"             , body.rqst.bytesIn             ),
    castor::dlf::Param("bytesOut"            , body.rqst.bytesOut            ),
    castor::dlf::Param("hostBytes"           , body.rqst.hostBytes           ),
    castor::dlf::Param("nbRecs"              , body.rqst.nbRecs              ),
    castor::dlf::Param("maxNbRec"            , body.rqst.maxNbRec            ),
    castor::dlf::Param("maxSize"             , body.rqst.maxSize             ),
    castor::dlf::Param("startSize"           , body.rqst.startSize           ),
    castor::dlf::Param("segAttr.nameServerHostName",
      body.rqst.segAttr.nameServerHostName),
    castor::dlf::Param("segAttr.segmCksumAlgorithm",
      body.rqst.segAttr.segmCksumAlgorithm),
    castor::dlf::Param("segAttr.segmCksum"   , checksumHexStream.str()       ),
    castor::dlf::Param("segAttr.castorFileId", body.rqst.segAttr.castorFileId),
    castor::dlf::Param("stgReqId"            , body.rqst.stgReqId            ),
    castor::dlf::Param("err.errorMsg"        , body.err.errorMsg             ),
    castor::dlf::Param("err.severity"        , body.err.severity             ),
    castor::dlf::Param("err.errorCode"       , body.err.errorCode            ),
    castor::dlf::Param("err.maxTpRetry"      , body.err.maxTpRetry           ),
    castor::dlf::Param("err.maxCpRetry"      , body.err.maxCpRetry           )};

  castor::dlf::dlf_writep(cuuid, severity, message_no,
    body.rqst.segAttr.castorFileId, body.rqst.segAttr.nameServerHostName,
    params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const legacymsg::RtcpFileRqstMsgBody &body) throw() {

  std::ostringstream checksumHexStream;
  checksumHexStream << "0x" << std::hex << body.rqst.segAttr.segmCksum;

  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId"  , volReqId                      ),
    castor::dlf::Param("volReqId"            , volReqId                      ),
    castor::dlf::Param("socketFd"            , socketFd                      ),
    castor::dlf::Param("filePath"            , body.rqst.filePath            ),
    castor::dlf::Param("tapePath"            , body.rqst.tapePath            ),
    castor::dlf::Param("recfm"               , body.rqst.recfm               ),
    castor::dlf::Param("fid"                 , body.rqst.fid                 ),
    castor::dlf::Param("ifce"                , body.rqst.ifce                ),
    castor::dlf::Param("stageId"             , body.rqst.stageId             ),
    castor::dlf::Param("volReqId"            , body.rqst.volReqId            ),
    castor::dlf::Param("jobId"               , body.rqst.jobId               ),
    castor::dlf::Param("stageSubReqId"       , body.rqst.stageSubReqId       ),
    castor::dlf::Param("umask"               , body.rqst.umask               ),
    castor::dlf::Param("positionMethod"      , body.rqst.positionMethod      ),
    castor::dlf::Param("tapeFseq"            , body.rqst.tapeFseq            ),
    castor::dlf::Param("diskFseq"            , body.rqst.diskFseq            ),
    castor::dlf::Param("blockSize"           , body.rqst.blockSize           ),
    castor::dlf::Param("recordLength"        , body.rqst.recordLength        ),
    castor::dlf::Param("retention"           , body.rqst.retention           ),
    castor::dlf::Param("defAlloc"            , body.rqst.defAlloc            ),
    castor::dlf::Param("rtcpErrAction"       , body.rqst.rtcpErrAction       ),
    castor::dlf::Param("tpErrAction"         , body.rqst.tpErrAction         ),
    castor::dlf::Param("convert"             , body.rqst.convert             ),
    castor::dlf::Param("checkFid"            , body.rqst.checkFid            ),
    castor::dlf::Param("concat"              , body.rqst.concat              ),
    castor::dlf::Param("procStatus"          ,
      utils::procStatusToString(body.rqst.procStatus)),
    castor::dlf::Param("cprc"                , body.rqst.cprc                ),
    castor::dlf::Param("tStartPosition"      , body.rqst.tStartPosition      ),
    castor::dlf::Param("tEndPosition"        , body.rqst.tEndPosition        ),
    castor::dlf::Param("tStartTransferDisk"  , body.rqst.tStartTransferDisk  ),
    castor::dlf::Param("tEndTransferDisk"    , body.rqst.tEndTransferDisk    ),
    castor::dlf::Param("tStartTransferTape"  , body.rqst.tStartTransferTape  ),
    castor::dlf::Param("tEndTransferTape"    , body.rqst.tEndTransferTape    ),
    castor::dlf::Param("blockId[0]"          , body.rqst.blockId[0]          ),
    castor::dlf::Param("blockId[1]"          , body.rqst.blockId[1]          ),
    castor::dlf::Param("blockId[2]"          , body.rqst.blockId[2]          ),
    castor::dlf::Param("blockId[3]"          , body.rqst.blockId[3]          ),
    castor::dlf::Param("offset"              , body.rqst.offset              ),
    castor::dlf::Param("bytesIn"             , body.rqst.bytesIn             ),
    castor::dlf::Param("bytesOut"            , body.rqst.bytesOut            ),
    castor::dlf::Param("hostBytes"           , body.rqst.hostBytes           ),
    castor::dlf::Param("nbRecs"              , body.rqst.nbRecs              ),
    castor::dlf::Param("maxNbRec"            , body.rqst.maxNbRec            ),
    castor::dlf::Param("maxSize"             , body.rqst.maxSize             ),
    castor::dlf::Param("startSize"           , body.rqst.startSize           ),
    castor::dlf::Param("segAttr.nameServerHostName",
      body.rqst.segAttr.nameServerHostName),
    castor::dlf::Param("segAttr.segmCksumAlgorithm",
      body.rqst.segAttr.segmCksumAlgorithm),
    castor::dlf::Param("segAttr.segmCksum"   , checksumHexStream.str()       ),
    castor::dlf::Param("segAttr.castorFileId", body.rqst.segAttr.castorFileId),
    castor::dlf::Param("stgReqId"            , body.rqst.stgReqId            )};

  castor::dlf::dlf_writep(cuuid, severity, message_no,
    body.rqst.segAttr.castorFileId, body.rqst.segAttr.nameServerHostName,
    params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const legacymsg::GiveOutpMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId", volReqId    ),
    castor::dlf::Param("volReqId"          , volReqId    ),
    castor::dlf::Param("socketFd"          , socketFd    ),
    castor::dlf::Param("message"           , body.message)};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const legacymsg::RtcpNoMoreRequestsMsgBody&)
  throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId", volReqId),
    castor::dlf::Param("volReqId"          , volReqId),
    castor::dlf::Param("socketFd"          , socketFd)};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const legacymsg::RtcpAbortMsgBody&) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId", volReqId),
    castor::dlf::Param("volReqId"          , volReqId),
    castor::dlf::Param("socketFd"          , socketFd)};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const legacymsg::RtcpDumpTapeRqstMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId", volReqId          ),
    castor::dlf::Param("volReqId"          , volReqId          ),
    castor::dlf::Param("socketFd"          , socketFd          ),
    castor::dlf::Param("maxBytes"          , body.maxBytes     ),
    castor::dlf::Param("blockSize"         , body.blockSize    ),
    castor::dlf::Param("convert"           , body.convert      ),
    castor::dlf::Param("tapeErrAction"     , body.tapeErrAction),
    castor::dlf::Param("startFile"         , body.startFile    ),
    castor::dlf::Param("maxFiles"          , body.maxFiles     ),
    castor::dlf::Param("fromBlock"         , body.fromBlock    ),
    castor::dlf::Param("toBlock"           , body.toBlock      )};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const legacymsg::VmgrTapeInfoMsgBody &body,
  const timeval &connectDuration, const timeval &sendRecvDuration) throw() {

  const double connectDurationDouble = utils::timevalToDouble(connectDuration);
  const double sendRecvDurationDouble =
    utils::timevalToDouble(sendRecvDuration);

  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId", volReqId               ),
    castor::dlf::Param("connectDuration"   , connectDurationDouble  ),
    castor::dlf::Param("sendRecvDuration"  , sendRecvDurationDouble ),
    castor::dlf::Param("volReqId"          , volReqId               ),
    castor::dlf::Param("socketFd"          , socketFd               ),
    castor::dlf::Param("vsn"               , body.vsn               ),
    castor::dlf::Param("library"           , body.library           ),
    castor::dlf::Param("dgn"               , body.dgn               ),
    castor::dlf::Param("density"           , body.density           ),
    castor::dlf::Param("labelType"         , body.labelType         ),
    castor::dlf::Param("model"             , body.model             ),
    castor::dlf::Param("mediaLetter"       , body.mediaLetter       ),
    castor::dlf::Param("manufacturer"      , body.manufacturer      ),
    castor::dlf::Param("serialNumber"      , body.serialNumber      ),
    castor::dlf::Param("nbSides"           , body.nbSides           ),
    castor::dlf::Param("eTime"             , body.eTime             ),
    castor::dlf::Param("side"              , body.side              ),
    castor::dlf::Param("poolName"          , body.poolName          ),
    castor::dlf::Param("estimatedFreeSpace", body.estimatedFreeSpace),
    castor::dlf::Param("nbFiles"           , body.nbFiles           ),
    castor::dlf::Param("rCount"            , body.rCount            ),
    castor::dlf::Param("wCount"            , body.wCount            ),
    castor::dlf::Param("rHost"             , body.rHost             ),
    castor::dlf::Param("wHost"             , body.wHost             ),
    castor::dlf::Param("rJid"              , body.rJid              ),
    castor::dlf::Param("wJid"              , body.wJid              ),
    castor::dlf::Param("rTime"             , body.rTime             ),
    castor::dlf::Param("wTime"             , body.wTime             ),
    castor::dlf::Param("status"            , body.status            )};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const tapeBridgeFlushedToTapeMsgBody_t &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId", volReqId     ),
    castor::dlf::Param("volReqId"          , volReqId     ),
    castor::dlf::Param("socketFd"          , socketFd     ),
    castor::dlf::Param("volReqId"          , body.volReqId),
    castor::dlf::Param("tapeFseq"          , body.tapeFseq)};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsg
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LogHelper::logMsg(const Cuuid_t &cuuid,
  const int severity, const int message_no,
  const tapegateway::Volume &msg, const timeval &connectDuration,
  const timeval &sendRecvDuration) throw() {

  const double connectDurationDouble = utils::timevalToDouble(connectDuration);
  const double sendRecvDurationDouble =
    utils::timevalToDouble(sendRecvDuration);

  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId", msg.mountTransactionId()),
    castor::dlf::Param("connectDuration"   , connectDurationDouble   ),
    castor::dlf::Param("sendRecvDuration"  , sendRecvDurationDouble  ),
    castor::dlf::Param("TPVID"             , msg.vid()               ),
    castor::dlf::Param("density"           , msg.density()           ),
    castor::dlf::Param("label"             , msg.label()             ),
    castor::dlf::Param("id"                , msg.id()                ),
    castor::dlf::Param("clientType"        ,
      utils::volumeClientTypeToString(msg.clientType())),
    castor::dlf::Param("mode"              ,
      utils::volumeModeToString(msg.mode()))};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsg
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LogHelper::logMsg(const Cuuid_t &cuuid,
  const int severity, const int message_no,
  const tapegateway::DumpParameters &msg, const timeval &connectDuration,
  const timeval &sendRecvDuration) throw() {

  const double connectDurationDouble = utils::timevalToDouble(connectDuration);
  const double sendRecvDurationDouble =
    utils::timevalToDouble(sendRecvDuration);

  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId", msg.mountTransactionId()),
    castor::dlf::Param("connectDuration"   , connectDurationDouble   ),
    castor::dlf::Param("sendRecvDuration"  , sendRecvDurationDouble  ),
    castor::dlf::Param("maxBytes"          , msg.maxBytes()          ),
    castor::dlf::Param("blockSize"         , msg.blockSize()         ),
    castor::dlf::Param("converter"         , msg.converter()         ),
    castor::dlf::Param("errAction"         , msg.errAction()         ),
    castor::dlf::Param("startFile"         , msg.startFile()         ),
    castor::dlf::Param("maxFile"           , msg.maxFile()           ),
    castor::dlf::Param("fromBlock"         , msg.fromBlock()         ),
    castor::dlf::Param("toBlock"           , msg.toBlock()           )};
  castor::dlf::dlf_writep(cuuid, severity, message_no, params);
}

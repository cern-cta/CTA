/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/TestingBridgeProtocolEngine.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_TESTINGBRIDGEPROTOCOLENGINE_HPP
#define TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_TESTINGBRIDGEPROTOCOLENGINE_HPP 1

#include "castor/tape/tapebridge/BridgeProtocolEngine.hpp"
#include "test/unittest/test_exception.hpp"

#include <exception>

namespace castor     {
namespace tape       {
namespace tapebridge {

class TestingBridgeProtocolEngine: public BridgeProtocolEngine {
public:

  TestingBridgeProtocolEngine(
    IFileCloser                         &fileCloser,
    const BulkRequestConfigParams       &bulkRequestConfigParams,
    const TapeFlushConfigParams         &tapeFlushConfigParams,
    const Cuuid_t                       &cuuid,
    const int                           rtcpdListenSock,
    const int                           initialRtcpdSock,
    const legacymsg::RtcpJobRqstMsgBody &jobRequest,
    const tapegateway::Volume           &volume,
    const uint32_t                      nbFilesOnDestinationTape,
    utils::BoolFunctor                  &stoppingGracefully,
    Counter<uint64_t>                   &tapebridgeTransactionCounter,
    const bool                          logPeerOfCallbackConnectionsFromRtcpd,
    const bool                          checkRtcpdIsConnectingFromLocalHost,
    IClientProxy                        &clientProxy,
    ILegacyTxRx                         &legacyTxRx)
    throw():
    BridgeProtocolEngine(
      fileCloser,
      bulkRequestConfigParams,
      tapeFlushConfigParams,
      cuuid,
      rtcpdListenSock,
      initialRtcpdSock,
      jobRequest,
      volume,
      nbFilesOnDestinationTape,
      stoppingGracefully,
      tapebridgeTransactionCounter,
      logPeerOfCallbackConnectionsFromRtcpd,
      checkRtcpdIsConnectingFromLocalHost,
      clientProxy,
      legacyTxRx) {
    // Do nothing
  }

  using BridgeProtocolEngine::startRtcpdSession;
  using BridgeProtocolEngine::endRtcpdSession;
  using BridgeProtocolEngine::runRtcpdSession;
  using BridgeProtocolEngine::processSocksInALoop;

  void handleSelectEvents(struct timeval selectTimeout)
    throw(std::exception) {
    try {
      BridgeProtocolEngine::handleSelectEvents(selectTimeout);
    } catch(castor::exception::Exception &ex) {
      test_exception te(ex.getMessage().str());
      throw te;
    } catch(std::exception &se) {
      throw se;
    } catch(...) {
      test_exception te("Caught an unknown exception");
      throw te;
    }
  }

  using BridgeProtocolEngine::acceptRtcpdConnection;
  using BridgeProtocolEngine::processAPendingSocket;
  using BridgeProtocolEngine::processPendingListenSocket;
  using BridgeProtocolEngine::processPendingInitialRtcpdSocket;
  using BridgeProtocolEngine::processPendingRtcpdDiskTapeIOControlSocket;
  using BridgeProtocolEngine::processPendingClientGetMoreWorkSocket;
  using BridgeProtocolEngine::dispatchCallbackForReplyToGetMoreWork;
  using BridgeProtocolEngine::processPendingClientMigrationReportSocket;
  using BridgeProtocolEngine::processRtcpdRequest;
  using BridgeProtocolEngine::dispatchRtcpdRequestHandler;
  using BridgeProtocolEngine::rtcpFileReqRtcpdCallback;
  using BridgeProtocolEngine::rtcpFileErrReqRtcpdCallback;
  using BridgeProtocolEngine::dispatchRtcpFileErrReq;
  using BridgeProtocolEngine::processRtcpFileErrReqDump;
  using BridgeProtocolEngine::processRtcpWaiting;
  using BridgeProtocolEngine::processRtcpRequestMoreWork;
  using BridgeProtocolEngine::processRtcpRequestMoreMigrationWork;
  using BridgeProtocolEngine::sendFilesToMigrateListRequestToClient;
  using BridgeProtocolEngine::sendCachedFileToMigrateToRtcpd;
  using BridgeProtocolEngine::processRtcpRequestMoreRecallWork;
  using BridgeProtocolEngine::sendFilesToRecallListRequestToClient;
  using BridgeProtocolEngine::sendCachedFileToRecallToRtcpd;
  using BridgeProtocolEngine::sendFileToRecallToRtcpd;
  using BridgeProtocolEngine::processRtcpFilePositionedRequest;
  using BridgeProtocolEngine::processRtcpFileFinishedRequest;
  using BridgeProtocolEngine::processSuccessfullRtcpFileFinishedRequest;
  using BridgeProtocolEngine::rtcpTapeReqRtcpdCallback;
  using BridgeProtocolEngine::rtcpTapeErrReqRtcpdCallback;
  using BridgeProtocolEngine::rtcpEndOfReqRtcpdCallback;
  using BridgeProtocolEngine::tapeBridgeFlushedToTapeCallback;
  using BridgeProtocolEngine::setMigrationCompressedFileSize;
  using BridgeProtocolEngine::calcMigrationCompressionRatio;
  using BridgeProtocolEngine::calcSumOfFileSizes;
  using BridgeProtocolEngine::sendFlushedMigrationsToClient;
  using BridgeProtocolEngine::giveOutpRtcpdCallback;
  using BridgeProtocolEngine::startMigrationSession;
  using BridgeProtocolEngine::startRecallSession;
  using BridgeProtocolEngine::startDumpSession;
  using BridgeProtocolEngine::checkPeerIsLocalhost;
  using BridgeProtocolEngine::filesToMigrateListClientCallback;
  using BridgeProtocolEngine::addFilesToMigrateToCache;
  using BridgeProtocolEngine::addFileToMigrateToCache;
  using BridgeProtocolEngine::sendFileToMigrateToRtcpd;
  using BridgeProtocolEngine::filesToRecallListClientCallback;
  using BridgeProtocolEngine::addFilesToRecallToCache;
  using BridgeProtocolEngine::addFileToRecallToCache;
  using BridgeProtocolEngine::noMoreFilesClientCallback;
  using BridgeProtocolEngine::tellRtcpdThereAreNoMoreFiles;
  using BridgeProtocolEngine::endNotificationErrorReportForGetMoreWorkClientCallback;
  using BridgeProtocolEngine::notifyClientEndOfSession;
  using BridgeProtocolEngine::notifyRtcpdEndOfSession;
  using BridgeProtocolEngine::notifyClientOfAnyFileSpecificErrors;
  using BridgeProtocolEngine::notifyClientOfFailedMigrations;
  using BridgeProtocolEngine::notifyClientOfFailedRecalls;
  using BridgeProtocolEngine::shuttingDownRtcpdSession;
  using BridgeProtocolEngine::sessionWithRtcpdIsFinished;
  using BridgeProtocolEngine::getNbDiskTapeIOControlConns;
  using BridgeProtocolEngine::getNbReceivedENDOF_REQs;
  using BridgeProtocolEngine::continueProcessingSocks;
  using BridgeProtocolEngine::generateMigrationTapeFileId;
}; // class TestingBridgeProtocolEngine

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_TESTINGBRIDGEPROTOCOLENGINE_HPP

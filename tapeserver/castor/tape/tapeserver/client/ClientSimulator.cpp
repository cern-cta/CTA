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

#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/FileMigrationReportList.hpp"
#include "castor/tape/tapegateway/FilesToMigrateListRequest.hpp"
#include "castor/tape/tapegateway/FilesToRecallListRequest.hpp"
#include "castor/tape/tapegateway/FilesToRecallList.hpp"
#include "castor/tape/tapegateway/GatewayMessage.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapeserver/client/ClientSimulator.hpp"
#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"

#include <errno.h>
#include <memory>
#include <queue>
#include <typeinfo>

namespace castor {
namespace tape {
namespace tapeserver {
namespace client {
  
using namespace castor::tape;
//------------------------------------------------------------------------------
//constructor
//------------------------------------------------------------------------------
ClientSimulator::ClientSimulator(uint32_t volReqId, const std::string & vid, 
    const std::string & density, tapegateway::ClientType clientType,
    tapegateway::VolumeMode volumeMode, EmptyMount_t emptyMount):
  TpcpCommand("clientSimulator::clientSimulator"), m_sessionErrorCode(0),
    m_vid(vid), m_density(density), m_clientType(clientType),
    m_volumeMode(volumeMode), m_emptyMount(emptyMount)
{
  m_volReqId = volReqId;
  setupCallbackSock();
}

//------------------------------------------------------------------------------
//processFirstRequest
//------------------------------------------------------------------------------
void ClientSimulator::processFirstRequest()
 {
  // Accept the next connection
  std::unique_ptr<castor::io::ServerSocket> clientConnection(m_callbackSock.accept());
  // Read in the message sent by the tapebridge
  std::unique_ptr<castor::IObject> obj(clientConnection->readObject());
  // Cast the message to a GatewayMessage or die
  tapegateway::GatewayMessage & msg(
      dynamic_cast<tapegateway::GatewayMessage &> (*obj.get()));
  // Check the mount transaction ID
  if (msg.mountTransactionId() != (uint64_t) m_volReqId) {
    std::stringstream oss;
    oss <<
        "Mount transaction ID mismatch" <<
        ": actual=" << msg.mountTransactionId() <<
        " expected=" << m_volReqId;
    sendEndNotificationErrorReport(msg.aggregatorTransactionId(), EBADMSG,
        oss.str(), *clientConnection);
    castor::exception::Exception ex(EBADMSG);
    ex.getMessage() << oss.str();
    throw ex;
  }
  // We strictly expect getVolId for this test for the moment
  if (msg.type() != tapegateway::VolumeRequest::TYPE()) {
    std::stringstream oss;
    oss <<
        "Expected a tapegateway::VolumeRequest, got " << typeid (msg).name();
    sendEndNotificationErrorReport(msg.aggregatorTransactionId(), EBADMSG,
        oss.str(), *clientConnection);
    castor::exception::Exception ex(EBADMSG);
    ex.getMessage() << oss.str();
    throw ex;
  }

  // This should always succeed
  tapegateway::VolumeRequest & vReq =
      dynamic_cast<tapegateway::VolumeRequest &> (msg);

  if (EmptyOnVolReq != m_emptyMount) {
    tapegateway::Volume vol;
    vol.setAggregatorTransactionId(vReq.aggregatorTransactionId());
    vol.setVid(m_vid);
    vol.setClientType(m_clientType);
    vol.setMode(m_volumeMode);
    vol.setLabel(m_volLabel);
    vol.setMountTransactionId(m_volReqId);
    vol.setDensity(m_density);
    clientConnection->sendObject(vol);
  } else {
    tapegateway::NoMoreFiles noMore;
    noMore.setAggregatorTransactionId(vReq.aggregatorTransactionId());
    noMore.setMountTransactionId(m_volReqId);
    clientConnection->sendObject(noMore);
  }
}
//------------------------------------------------------------------------------
//processOneRequest
//------------------------------------------------------------------------------
bool ClientSimulator::processOneRequest()
 {
  // Accept the next connection
  std::unique_ptr<castor::io::ServerSocket> clientConnection(m_callbackSock.accept());
  // Read in the message sent by the tapebridge
  std::unique_ptr<castor::IObject> obj(clientConnection->readObject());
  // Cast the message to a GatewayMessage or die
  tapegateway::GatewayMessage & msg(
      dynamic_cast<tapegateway::GatewayMessage &> (*obj.get()));
  // Check the mount transaction ID
  if (msg.mountTransactionId() != (uint64_t) m_volReqId) {
    std::stringstream oss;
    oss <<
        "Mount transaction ID mismatch" <<
        ": actual=" << msg.mountTransactionId() <<
        " expected=" << m_volReqId;
    sendEndNotificationErrorReport(msg.aggregatorTransactionId(), EBADMSG,
        oss.str(), *clientConnection);
    castor::exception::Exception ex(EBADMSG);
    ex.getMessage() << oss.str();
    throw ex;
  }
  // Process all the requests we can receive
  // Handle request for more work (Recall)
  try {
    tapegateway::FilesToRecallListRequest & req = 
        dynamic_cast<tapegateway::FilesToRecallListRequest &> (*obj);
    uint32_t files = 0;
    uint64_t bytes = 0;
    tapegateway::FilesToRecallList reply;
    while (files < req.maxFiles() && bytes < req.maxBytes()) {
      if (m_filesToRecall.size()) {
        files ++;
        bytes += m_recallSizes.front();
        std::unique_ptr<tapegateway::FileToRecallStruct> ftr(new tapegateway::FileToRecallStruct);
        *ftr = m_filesToRecall.front();
        reply.filesToRecall().push_back(ftr.release());
        m_filesToRecall.pop();
        m_recallSizes.pop();
      } else {
        break;
      }
    }
    // At this point, the reply should be populated with all the files asked for
    // (or as much as we could provide).
    // If not, we should send a NoMoreFiles instead of if.
    if (reply.filesToRecall().size()) {
      // All the information about the transaction still has to be filled up
      reply.setAggregatorTransactionId(msg.aggregatorTransactionId());
      reply.setMountTransactionId(m_volReqId);
      clientConnection->sendObject(reply);
      return true; // The end of session is not signalled here
    } else {
      tapegateway::NoMoreFiles noMore;
      noMore.setAggregatorTransactionId(msg.aggregatorTransactionId());
      noMore.setMountTransactionId(m_volReqId);
      clientConnection->sendObject(noMore);
      return true; // The end of session is not signalled here
    }
  } catch (std::bad_cast&) {}
  
  // Process the recall reports
  try {
    // Check that we get a recall report and simply acknowledge
    tapegateway::FileRecallReportList & req =
        dynamic_cast<tapegateway::FileRecallReportList &> (*obj);
    tapegateway::NotificationAcknowledge reply;
    reply.setMountTransactionId(m_volReqId);
    reply.setAggregatorTransactionId(req.aggregatorTransactionId());
    clientConnection->sendObject(reply);
    return true; // The end of session is not signalled here
  } catch (std::bad_cast&) {}
  
  // Handle request for more work (migration)
  try {
    tapegateway::FilesToMigrateListRequest & req =
        dynamic_cast<tapegateway::FilesToMigrateListRequest &> (*obj);
    uint32_t files = 0;
    uint64_t bytes = 0;
    tapegateway::FilesToMigrateList reply;
    while (files < req.maxFiles() && bytes < req.maxBytes()) {
      if (m_filesToMigrate.size()) {
        files++;
        std::unique_ptr<tapegateway::FileToMigrateStruct> ftm(new tapegateway::FileToMigrateStruct);
        *ftm = m_filesToMigrate.front();
        bytes += ftm->fileSize();
        reply.filesToMigrate().push_back(ftm.release());
        m_filesToMigrate.pop();
      } else {
        break;
      }
    }
    // At this point, the reply should be populated with all the files asked for
    // (or as much as we could provide).
    // If not, we should send a NoMoreFiles instead of if.
    if(reply.filesToMigrate().size()) {
      reply.setAggregatorTransactionId(msg.aggregatorTransactionId());
      reply.setMountTransactionId(m_volReqId);
      clientConnection->sendObject(reply);
      return true; // The end of session is not signalled here
    } else {
      tapegateway::NoMoreFiles noMore;
      noMore.setAggregatorTransactionId(msg.aggregatorTransactionId());
      noMore.setMountTransactionId(m_volReqId);
      clientConnection->sendObject(noMore);
      return true; // The end of session is not signalled here
    }
  } catch(std::bad_cast&) {}
  
  // Process the migration reports
  try {
    tapegateway::FileMigrationReportList & req =
        dynamic_cast<tapegateway::FileMigrationReportList &> (*obj);
    tapegateway::NotificationAcknowledge reply;
    reply.setMountTransactionId(m_volReqId);
    reply.setAggregatorTransactionId(req.aggregatorTransactionId());
    clientConnection->sendObject(reply);
    // We will now record the fseqs and checksums reported:
    for(std::vector<tapegateway::FileMigratedNotificationStruct*>::iterator i =
        req.successfulMigrations().begin(); i!=req.successfulMigrations().end();
        i++) {
      m_receivedChecksums[(*i)->fseq()] = (*i)->checksum();
    }
    // We also record the error codes the the failed migrations
    for(std::vector<tapegateway::FileErrorReportStruct*>::iterator i =
        req.failedMigrations().begin(); i!=req.failedMigrations().end();
        i++) {
      m_receivedErrorCodes[(*i)->fseq()] = (*i)->errorCode();
    }
    return true; // The end of session is not signalled here
  } catch (std::bad_cast&) {}
  
  // Final case: end of session.
  // We expect with tape EndNotification or EndNotificationErrorReport
  try {
    (void)dynamic_cast<tapegateway::EndNotification &> (*obj);
  } catch (std::bad_cast&) {
    try  {
      tapegateway::EndNotificationErrorReport & enr = 
        dynamic_cast<tapegateway::EndNotificationErrorReport &> (*obj);
      m_sessionErrorCode = enr.errorCode();
    } catch (std::bad_cast&) {
      std::stringstream oss;
      oss <<
        "Expected a tapegateway::EndNotification or tapegateway::EndNotificationErrorReport, got " << typeid (msg).name();
        sendEndNotificationErrorReport(msg.aggregatorTransactionId(), EBADMSG,
          oss.str(), *clientConnection);
      castor::exception::Exception ex(EBADMSG);
      ex.getMessage() << oss.str();
      throw ex;
    }
  }
  tapegateway::NotificationAcknowledge ack;
  ack.setAggregatorTransactionId(msg.aggregatorTransactionId());
  ack.setMountTransactionId(m_volReqId);
  clientConnection->sendObject(ack);
  return false; // We are at the end of the session
}
//------------------------------------------------------------------------------
//sendEndNotificationErrorReport
//------------------------------------------------------------------------------
void ClientSimulator::sendEndNotificationErrorReport(
    const uint64_t tapebridgeTransactionId,
    const int errorCode,
    const std::string &errorMessage,
    castor::io::AbstractSocket &sock)
throw() {
  tapegateway::EndNotificationErrorReport err;
  err.setAggregatorTransactionId(tapebridgeTransactionId);
  err.setErrorCode(errorCode);
  err.setErrorMessage(errorMessage);
  sock.sendObject(err);
}

}
}
}
}

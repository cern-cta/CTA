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

#include "castor/io/ClientSocket.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/FilesToMigrateListRequest.hpp"
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"
#include "castor/tape/tapegateway/FileMigrationReportList.hpp"
#include "castor/tape/tapegateway/FilesToRecallListRequest.hpp"
#include "castor/tape/tapegateway/FilesToRecallList.hpp"
#include "castor/tape/tapegateway/FileRecallReportList.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/utils/Timer.hpp"

#include <cxxabi.h>
#include <memory>
#include <stdlib.h>
#include <typeinfo>

namespace castor {
namespace tape {
namespace tapeserver {
namespace client {
  
//------------------------------------------------------------------------------
//ClientProxy constructor
//------------------------------------------------------------------------------
ClientProxy::ClientProxy(): m_transactionId(0) {}

//------------------------------------------------------------------------------
//UnexpectedResponse::UnexpectedResponse
//------------------------------------------------------------------------------
ClientProxy::UnexpectedResponse::
    UnexpectedResponse(const castor::IObject* resp, const std::string & w):
castor::exception::Exception(w) {
  std::string responseType = typeid(*resp).name();
  int status = -1;
  char * demangled = abi::__cxa_demangle(responseType.c_str(), NULL, NULL, &status);
  if (!status)
    responseType = demangled;
  free(demangled);
  getMessage() << " Response type was: " << responseType;
}
//------------------------------------------------------------------------------
//requestResponseSession
//------------------------------------------------------------------------------
tapegateway::GatewayMessage *
  ClientProxy::requestResponseSession(
    const tapegateway::GatewayMessage &req,
    RequestReport & report,
    int timeout)
{
  std::ostringstream msg;
  msg << __FUNCTION__ << ": Gatway communication is not supported in the CTA"
    " version of tapeserverd";
  throw castor::exception::Exception(msg.str());
}

//------------------------------------------------------------------------------
//fetchVolumeId
//------------------------------------------------------------------------------
void ClientProxy::fetchVolumeId(
  VolumeInfo & volInfo, RequestReport &report) 
{
  std::ostringstream msg;
  msg << __FUNCTION__ << ": Gatway communication is not supported in the CTA"
    " version of tapeserverd";
  throw castor::exception::Exception(msg.str());
}

//------------------------------------------------------------------------------
//reportEndOfSession
//------------------------------------------------------------------------------
void ClientProxy::reportEndOfSession(
RequestReport &transactionReport) 
{
  std::ostringstream msg;
  msg << __FUNCTION__ << ": Gatway communication is not supported in the CTA"
    " version of tapeserverd";
  throw castor::exception::Exception(msg.str());
}

//------------------------------------------------------------------------------
//reportEndOfSessionWithError
//------------------------------------------------------------------------------
void ClientProxy::reportEndOfSessionWithError(
const std::string & errorMsg, int errorCode, RequestReport &transactionReport) 
{
  std::ostringstream msg;
  msg << __FUNCTION__ << ": Gatway communication is not supported in the CTA"
    " version of tapeserverd";
  throw castor::exception::Exception(msg.str());
}

//------------------------------------------------------------------------------
//getFilesToMigrate
//------------------------------------------------------------------------------
tapegateway::FilesToMigrateList * 
    ClientProxy::getFilesToMigrate(
uint64_t files, uint64_t bytes, RequestReport& report) 
{
  std::ostringstream msg;
  msg << __FUNCTION__ << ": Gatway communication is not supported in the CTA"
    " version of tapeserverd";
  throw castor::exception::Exception(msg.str());
}

//------------------------------------------------------------------------------
//reportMigrationResults
//------------------------------------------------------------------------------
void ClientProxy::reportMigrationResults(
tapegateway::FileMigrationReportList & migrationReport,
    RequestReport& report) {
  std::ostringstream msg;
  msg << __FUNCTION__ << ": Gatway communication is not supported in the CTA"
    " version of tapeserverd";
  throw castor::exception::Exception(msg.str());
}

//------------------------------------------------------------------------------
//getFilesToRecall
//------------------------------------------------------------------------------
tapegateway::FilesToRecallList * 
    ClientProxy::getFilesToRecall(
uint64_t files, uint64_t bytes, RequestReport& report) 
{
  std::ostringstream msg;
  msg << __FUNCTION__ << ": Gatway communication is not supported in the CTA"
    " version of tapeserverd";
  throw castor::exception::Exception(msg.str());
}
//------------------------------------------------------------------------------
//reportRecallResults
//------------------------------------------------------------------------------
void ClientProxy::reportRecallResults(
tapegateway::FileRecallReportList & recallReport,
RequestReport& report) {
  std::ostringstream msg;
  msg << __FUNCTION__ << ": Gatway communication is not supported in the CTA"
    " version of tapeserverd";
  throw castor::exception::Exception(msg.str());
}

}}}}

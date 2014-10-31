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

#include "castor/common/CastorConfiguration.hpp"
#include "castor/tape/tapeserver/Constants.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferConfig.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DataTransferConfig::DataTransferConfig():
  bufsz(0),
  nbBufs(0),
  bulkRequestMigrationMaxBytes(0),
  bulkRequestMigrationMaxFiles(0),
  bulkRequestRecallMaxBytes(0),
  bulkRequestRecallMaxFiles(0),
  maxBytesBeforeFlush(0),
  maxFilesBeforeFlush(0),
  nbDiskThreads(0) {
}

//------------------------------------------------------------------------------
// createFromCastorConf
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DataTransferConfig::DataTransferConfig
  castor::tape::tapeserver::daemon::DataTransferConfig::createFromCastorConf(
    log::Logger *const log) {
  common::CastorConfiguration &castorConf =
    common::CastorConfiguration::getConfig();
  DataTransferConfig config;

  config.bufsz = castorConf.getConfEntInt(
    "TAPESERVERD", "BUFSZ",
    castor::tape::tapeserver::daemon::TAPESERVER_BUFSZ_DEFAULT, log);
  config.nbBufs = castorConf.getConfEntInt<uint32_t>(
    "TAPESERVERD", "NB_BUFS", log);
  config.tapeBadMIRHandlingRepair = castorConf.getConfEntString(
    "TAPESERVERD", "BADMIR_HANDLING", "CANCEL", log);
  config.bulkRequestMigrationMaxBytes = castorConf.getConfEntInt(
    "TAPESERVERD", "BULKREQUESTMIGRATIONMAXBYTES",
    (uint64_t)TAPEBRIDGE_BULKREQUESTMIGRATIONMAXBYTES, log);
  config.bulkRequestMigrationMaxFiles = castorConf.getConfEntInt(
    "TAPESERVERD", "BULKREQUESTMIGRATIONMAXFILES",
    (uint64_t)TAPEBRIDGE_BULKREQUESTMIGRATIONMAXFILES, log);
  config.bulkRequestRecallMaxBytes = castorConf.getConfEntInt(
    "TAPESERVERD", "BULKREQUESTRECALLMAXBYTES",
    (uint64_t)TAPEBRIDGE_BULKREQUESTRECALLMAXBYTES, log);
  config.bulkRequestRecallMaxFiles = castorConf.getConfEntInt(
    "TAPESERVERD", "BULKREQUESTRECALLMAXFILES",
    (uint64_t)TAPEBRIDGE_BULKREQUESTRECALLMAXFILES, log);
  config.maxBytesBeforeFlush = castorConf.getConfEntInt(
    "TAPESERVERD", "MAXBYTESBEFOREFLUSH",
    (uint64_t)TAPEBRIDGE_MAXBYTESBEFOREFLUSH, log);
  config.maxFilesBeforeFlush = castorConf.getConfEntInt(
    "TAPESERVERD", "MAXFILESBEFOREFLUSH",
    (uint64_t)TAPEBRIDGE_MAXFILESBEFOREFLUSH, log);
  config.nbDiskThreads = castorConf.getConfEntInt(
    "TAPESERVERD", "THREAD_POOL", 
    castor::tape::tapeserver::daemon::TAPESERVER_NB_DISK_THREAD_DEFAULT, log);
  config.remoteFileProtocol = castorConf.getConfEntString(
    "TAPESERVERD", "REMOTEFILEPROTOCOL", "RFIO", log);
  config.xrootPrivateKey = castorConf.getConfEntString(
    "XROOT", "PrivateKey", "/opt/xrootd/keys/key.pem", log);

  return config;
}

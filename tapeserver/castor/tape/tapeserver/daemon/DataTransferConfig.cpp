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
#include "castor/tape/tapeserver/daemon/DataTransferConfig.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/tapeserver/TapeBridgeConstants.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DataTransferConfig::DataTransferConfig()
  throw():
  bufsz(0),
  nbBufs(0),
  bulkRequestMigrationMaxBytes(0),
  bulkRequestMigrationMaxFiles(0),
  bulkRequestRecallMaxBytes(0),
  bulkRequestRecallMaxFiles(0),
  maxBytesBeforeFlush(0),
  maxFilesBeforeFlush(0),
  nbDiskThreads(0),
  useLbp(false),
  useRAO(false),
  externalEncryptionKeyScript("") {}

//------------------------------------------------------------------------------
// createFromCastorConf
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DataTransferConfig
  castor::tape::tapeserver::daemon::DataTransferConfig::createFromCastorConf(
    cta::log::Logger *const log) {
  common::CastorConfiguration &castorConf =
    common::CastorConfiguration::getConfig();
  DataTransferConfig config;

  config.bufsz = castorConf.getConfEntInt(
    "TapeServer", "BufSize",
    castor::tape::tapeserver::daemon::TAPESERVER_BUFSZ, log);
  config.nbBufs = castorConf.getConfEntInt<uint32_t>(
    "TapeServer", "NbBufs", log);
  config.bulkRequestMigrationMaxBytes = castorConf.getConfEntInt(
    "TapeServer", "BulkRequestMigrationMaxBytes",
    (uint64_t)TAPEBRIDGE_BULKREQUESTMIGRATIONMAXBYTES, log);
  config.bulkRequestMigrationMaxFiles = castorConf.getConfEntInt(
    "TapeServer", "BulkRequestMigrationMaxFiles",
    (uint64_t)TAPEBRIDGE_BULKREQUESTMIGRATIONMAXFILES, log);
  config.bulkRequestRecallMaxBytes = castorConf.getConfEntInt(
    "TapeServer", "BulkRequestRecallMaxBytes",
    (uint64_t)TAPEBRIDGE_BULKREQUESTRECALLMAXBYTES, log);
  config.bulkRequestRecallMaxFiles = castorConf.getConfEntInt(
    "TapeServer", "BulkRequestRecallMaxFiles",
    (uint64_t)TAPEBRIDGE_BULKREQUESTRECALLMAXFILES, log);
  config.maxBytesBeforeFlush = castorConf.getConfEntInt(
    "TapeServer", "MaxBytesBeforeFlush",
    (uint64_t)TAPEBRIDGE_MAXBYTESBEFOREFLUSH, log);
  config.maxFilesBeforeFlush = castorConf.getConfEntInt(
    "TapeServer", "MaxFilesBeforeFlush",
    (uint64_t)TAPEBRIDGE_MAXFILESBEFOREFLUSH, log);
  config.nbDiskThreads = castorConf.getConfEntInt(
    "TapeServer", "NbDiskThreads", 
    castor::tape::tapeserver::daemon::TAPESERVER_NB_DISK_THREAD, log);
  config.xrootPrivateKey = castorConf.getConfEntString(
    "TapeServer", "XrootPrivateKey", "/etc/castor/xrd_tape_key.pem", log);
  config.xrootTimeout = castorConf.getConfEntInt(
    "TapeServer", "XrootTimeout", 0, log);
  const std::string useLBP = castorConf.getConfEntString(
    "TapeServer", "UseLogicalBlockProtection", "no", log);
  const std::string useRAO = castorConf.getConfEntString(
    "TapeServer", "UseRecommendedAccessOrder", "no", log);

  if (!strcasecmp(useLBP.c_str(), "yes") || !strcmp(useLBP.c_str(), "1")) {
    config.useLbp = true;
  } else {
    config.useLbp = false;
  }

  if (!strcasecmp(useRAO.c_str(), "yes") || !strcmp(useRAO.c_str(), "1")) {
    config.useRAO = true;
  } else {
    config.useRAO = false;
  }

  config.externalEncryptionKeyScript = castorConf.getConfEntString("TapeServer",
    "ExternalEncryptionKeyScript", "");

  return config;
}

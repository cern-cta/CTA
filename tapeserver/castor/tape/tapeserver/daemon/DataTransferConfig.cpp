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


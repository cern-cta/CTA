/******************************************************************************
 *                castor/tape/tapebridge/BulkRequestConfigParams.cpp
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

#include "castor/tape/tapebridge/BulkRequestConfigParams.hpp"
#include "castor/tape/tapebridge/Constants.hpp"


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::BulkRequestConfigParams::BulkRequestConfigParams():
  m_bulkRequestMigrationMaxBytes("TAPEBRIDGE", "BULKREQUESTMIGRATIONMAXBYTES",
    (uint64_t)0, "UNKNOWN"),
  m_bulkRequestMigrationMaxFiles("TAPEBRIDGE", "BULKREQUESTMIGRATIONMAXFILES",
    (uint64_t)0, "UNKNOWN"),
  m_bulkRequestRecallMaxBytes("TAPEBRIDGE", "BULKREQUESTRECALLMAXBYTES",
    (uint64_t)0, "UNKNOWN"),
  m_bulkRequestRecallMaxFiles("TAPEBRIDGE", "BULKREQUESTRECALLMAXFILES",
    (uint64_t)0, "UNKNOWN") {
  // Do nothing
}


//-----------------------------------------------------------------------------
// getBulkRequestMigrationMaxBytes
//-----------------------------------------------------------------------------
const castor::tape::tapebridge::ConfigParamAndSource<uint64_t> &
  castor::tape::tapebridge::BulkRequestConfigParams::
  getBulkRequestMigrationMaxBytes() const {
  return m_bulkRequestMigrationMaxBytes;
}


//-----------------------------------------------------------------------------
// getBulkRequestMigrationMaxFiles
//-----------------------------------------------------------------------------
const castor::tape::tapebridge::ConfigParamAndSource<uint64_t> &
  castor::tape::tapebridge::BulkRequestConfigParams::
  getBulkRequestMigrationMaxFiles() const {
  return m_bulkRequestMigrationMaxFiles;
}


//-----------------------------------------------------------------------------
// getBulkRequestRecallMaxBytes
//-----------------------------------------------------------------------------
const castor::tape::tapebridge::ConfigParamAndSource<uint64_t> &
  castor::tape::tapebridge::BulkRequestConfigParams::
  getBulkRequestRecallMaxBytes() const {
  return m_bulkRequestRecallMaxBytes;
}


//-----------------------------------------------------------------------------
// getBulkRequestRecallMaxFiles
//-----------------------------------------------------------------------------
const castor::tape::tapebridge::ConfigParamAndSource<uint64_t> &
  castor::tape::tapebridge::BulkRequestConfigParams::
  getBulkRequestRecallMaxFiles() const {
  return m_bulkRequestRecallMaxFiles;
}


//-----------------------------------------------------------------------------
// determineConfigParams
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BulkRequestConfigParams::determineConfigParams()
  throw(castor::exception::Exception) {
  determineBulkRequestMigrationMaxBytes();
  determineBulkRequestMigrationMaxFiles();
  determineBulkRequestRecallMaxBytes();
  determineBulkRequestRecallMaxFiles();
}


//-----------------------------------------------------------------------------
// determineBulkRequestMigrationMaxBytes
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BulkRequestConfigParams::
  determineBulkRequestMigrationMaxBytes()
  throw(castor::exception::Exception) {
  determineUint64ConfigParam(m_bulkRequestMigrationMaxBytes,
    TAPEBRIDGE_BULKREQUESTMIGRATIONMAXBYTES);
}


//-----------------------------------------------------------------------------
// determineBulkRequestMigrationMaxFiles
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BulkRequestConfigParams::
  determineBulkRequestMigrationMaxFiles()
  throw(castor::exception::Exception) {
  determineUint64ConfigParam(m_bulkRequestMigrationMaxFiles,
    TAPEBRIDGE_BULKREQUESTMIGRATIONMAXFILES);
}


//-----------------------------------------------------------------------------
// determineBulkRequestRecallMaxBytes
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BulkRequestConfigParams::
  determineBulkRequestRecallMaxBytes()
  throw(castor::exception::Exception) {
  determineUint64ConfigParam(m_bulkRequestRecallMaxBytes,
    TAPEBRIDGE_BULKREQUESTRECALLMAXBYTES);
}


//-----------------------------------------------------------------------------
// determineBulkRequestRecallMaxFiles
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BulkRequestConfigParams::
  determineBulkRequestRecallMaxFiles()
  throw(castor::exception::Exception) {
  determineUint64ConfigParam(m_bulkRequestRecallMaxFiles,
    TAPEBRIDGE_BULKREQUESTRECALLMAXFILES);
}

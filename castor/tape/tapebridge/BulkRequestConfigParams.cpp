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
  m_bulkRequestMigrationMaxBytes("TAPEBRIDGE", "BULKREQUESTMIGRATIONMAXBYTES"),
  m_bulkRequestMigrationMaxFiles("TAPEBRIDGE", "BULKREQUESTMIGRATIONMAXFILES"),
  m_bulkRequestRecallMaxBytes("TAPEBRIDGE", "BULKREQUESTRECALLMAXBYTES"),
  m_bulkRequestRecallMaxFiles("TAPEBRIDGE", "BULKREQUESTRECALLMAXFILES") {
  // Do nothing
}


//-----------------------------------------------------------------------------
// getBulkRequestMigrationMaxBytes
//-----------------------------------------------------------------------------
const castor::tape::tapebridge::ConfigParam<uint64_t> &
  castor::tape::tapebridge::BulkRequestConfigParams::
  getBulkRequestMigrationMaxBytes() const {
  return m_bulkRequestMigrationMaxBytes;
}


//-----------------------------------------------------------------------------
// getBulkRequestMigrationMaxFiles
//-----------------------------------------------------------------------------
const castor::tape::tapebridge::ConfigParam<uint64_t> &
  castor::tape::tapebridge::BulkRequestConfigParams::
  getBulkRequestMigrationMaxFiles() const {
  return m_bulkRequestMigrationMaxFiles;
}


//-----------------------------------------------------------------------------
// getBulkRequestRecallMaxBytes
//-----------------------------------------------------------------------------
const castor::tape::tapebridge::ConfigParam<uint64_t> &
  castor::tape::tapebridge::BulkRequestConfigParams::
  getBulkRequestRecallMaxBytes() const {
  return m_bulkRequestRecallMaxBytes;
}


//-----------------------------------------------------------------------------
// getBulkRequestRecallMaxFiles
//-----------------------------------------------------------------------------
const castor::tape::tapebridge::ConfigParam<uint64_t> &
  castor::tape::tapebridge::BulkRequestConfigParams::
  getBulkRequestRecallMaxFiles() const {
  return m_bulkRequestRecallMaxFiles;
}


//-----------------------------------------------------------------------------
// determineConfigParams
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BulkRequestConfigParams::determineConfigParams()
  throw(castor::exception::InvalidArgument, castor::exception::Exception) {
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
  throw(castor::exception::InvalidArgument, castor::exception::Exception) {
  determineUint64ConfigParam(m_bulkRequestMigrationMaxBytes,
    TAPEBRIDGE_BULKREQUESTMIGRATIONMAXBYTES);
}


//-----------------------------------------------------------------------------
// determineBulkRequestMigrationMaxFiles
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BulkRequestConfigParams::
  determineBulkRequestMigrationMaxFiles()
  throw(castor::exception::InvalidArgument, castor::exception::Exception) {
  determineUint64ConfigParam(m_bulkRequestMigrationMaxFiles,
    TAPEBRIDGE_BULKREQUESTMIGRATIONMAXFILES);
}


//-----------------------------------------------------------------------------
// determineBulkRequestRecallMaxBytes
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BulkRequestConfigParams::
  determineBulkRequestRecallMaxBytes()
  throw(castor::exception::InvalidArgument, castor::exception::Exception) {
  determineUint64ConfigParam(m_bulkRequestRecallMaxBytes,
    TAPEBRIDGE_BULKREQUESTRECALLMAXBYTES);
}


//-----------------------------------------------------------------------------
// determineBulkRequestRecallMaxFiles
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BulkRequestConfigParams::
  determineBulkRequestRecallMaxFiles()
  throw(castor::exception::InvalidArgument, castor::exception::Exception) {
  determineUint64ConfigParam(m_bulkRequestRecallMaxFiles,
    TAPEBRIDGE_BULKREQUESTRECALLMAXFILES);
}


//-----------------------------------------------------------------------------
// setBulkRequestMigrationMaxBytes
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BulkRequestConfigParams::
  setBulkRequestMigrationMaxBytes(
  const uint64_t                value,
  const ConfigParamSource::Enum source) {
  m_bulkRequestMigrationMaxBytes.setValueAndSource(value, source);
}


//-----------------------------------------------------------------------------
// setBulkRequestMigrationMaxFiles
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BulkRequestConfigParams::
  setBulkRequestMigrationMaxFiles(
  const uint64_t                value,
  const ConfigParamSource::Enum source) {
  m_bulkRequestMigrationMaxFiles.setValueAndSource(value, source);
}


//-----------------------------------------------------------------------------
// setBulkRequestRecallMaxBytes
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BulkRequestConfigParams::
  setBulkRequestRecallMaxBytes(
  const uint64_t                value,
  const ConfigParamSource::Enum source) {
  m_bulkRequestRecallMaxBytes.setValueAndSource(value, source);
}


//-----------------------------------------------------------------------------
// setBulkRequestRecallMaxFiles
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BulkRequestConfigParams::
  setBulkRequestRecallMaxFiles(
  const uint64_t                value,
  const ConfigParamSource::Enum source) {
  m_bulkRequestRecallMaxFiles.setValueAndSource(value, source);
}

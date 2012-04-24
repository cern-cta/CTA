/******************************************************************************
 *                castor/tape/tapebridge/TapeFlushConfigParams.cpp
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

#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/tapebridge/TapeFlushConfigParams.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/getconfent.h"
#include "h/rtcpd_constants.h"
#include "h/tapebridge_constants.h"
#include "h/u64subr.h"

#include <string>


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::TapeFlushConfigParams::TapeFlushConfigParams():
  m_tapeFlushMode("TAPEBRIDGE", "TAPEFLUSHMODE"),
  m_maxBytesBeforeFlush("TAPEBRIDGE", "MAXBYTESBEFOREFLUSH"),
  m_maxFilesBeforeFlush("TAPEBRIDGE", "MAXFILESBEFOREFLUSH") {
  // Do nothing
}


//------------------------------------------------------------------------------
// determineConfigParams
//------------------------------------------------------------------------------
void castor::tape::tapebridge::TapeFlushConfigParams::determineConfigParams()
  throw(castor::exception::InvalidArgument, castor::exception::Exception) {

  determineTapeFlushMode();

  // Only try to determine the maximum number of bytes and files before a flush
  // if the flush mode requires them
  if(TAPEBRIDGE_ONE_FLUSH_PER_N_FILES == m_tapeFlushMode.getValue()) {
    determineMaxBytesBeforeFlush();
    determineMaxFilesBeforeFlush();
  }
}


//------------------------------------------------------------------------------
// determineTapeFlushMode
//------------------------------------------------------------------------------
void castor::tape::tapebridge::TapeFlushConfigParams::determineTapeFlushMode()
  throw(castor::exception::InvalidArgument, castor::exception::Exception) {
  const std::string envVarName = m_tapeFlushMode.getCategory() + "_" +
    m_tapeFlushMode.getName();
  const char *paramCStr  = NULL;

  // Try to get the value from the environment variables, else try to get it
  // from castor.conf, else use the compile-time default
  if(NULL != (paramCStr = getenv(envVarName.c_str()))) {
    const uint32_t value = stringToTapeFlushMode(paramCStr);
    const ConfigParamSource::Enum source =
      ConfigParamSource::ENVIRONMENT_VARIABLE;
    m_tapeFlushMode.setValueAndSource(value, source);
  } else if(NULL != (paramCStr =
      getconfent(m_tapeFlushMode.getCategory().c_str(),
      m_tapeFlushMode.getName().c_str(), 0))) {
    const uint32_t value = stringToTapeFlushMode(paramCStr);
    const ConfigParamSource::Enum source = ConfigParamSource::CASTOR_CONF;
    m_tapeFlushMode.setValueAndSource(value, source);
  } else {
    const uint32_t value = TAPEBRIDGE_N_FLUSHES_PER_FILE;
    const ConfigParamSource::Enum source =
      ConfigParamSource::COMPILE_TIME_DEFAULT;
    m_tapeFlushMode.setValueAndSource(value, source);
  }
}


//------------------------------------------------------------------------------
// stringToTapeFlushMode
//------------------------------------------------------------------------------
uint32_t castor::tape::tapebridge::TapeFlushConfigParams::
  stringToTapeFlushMode(const std::string s)
  throw(castor::exception::InvalidArgument) {
  if(s == "N_FLUSHES_PER_FILE") {
    return TAPEBRIDGE_N_FLUSHES_PER_FILE;
  } else if(s == "ONE_FLUSH_PER_N_FILES") {
    return TAPEBRIDGE_ONE_FLUSH_PER_N_FILES;
  } else {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid tape-flush mode string"
      ": expected N_FLUSHES_PER_FILE or ONE_FLUSH_PER_N_FILES"
      ": actual=" << s);
  }
}


//------------------------------------------------------------------------------
// determineMaxBytesBeforeFlush
//------------------------------------------------------------------------------
void castor::tape::tapebridge::TapeFlushConfigParams::
  determineMaxBytesBeforeFlush()
  throw(castor::exception::InvalidArgument, castor::exception::Exception) {
  determineUint64ConfigParam(m_maxBytesBeforeFlush,
    TAPEBRIDGE_MAXBYTESBEFOREFLUSH);
}


//------------------------------------------------------------------------------
// determineMaxFilesBeforeFlush
//------------------------------------------------------------------------------
void castor::tape::tapebridge::TapeFlushConfigParams::
  determineMaxFilesBeforeFlush()
  throw(castor::exception::InvalidArgument, castor::exception::Exception) {
  determineUint64ConfigParam(m_maxFilesBeforeFlush,
    TAPEBRIDGE_MAXFILESBEFOREFLUSH);
}


//-----------------------------------------------------------------------------
// getTapeFlushMode
//-----------------------------------------------------------------------------
const castor::tape::tapebridge::ConfigParam<uint32_t>
  &castor::tape::tapebridge::TapeFlushConfigParams::getTapeFlushMode() const {
  return m_tapeFlushMode;
}


//-----------------------------------------------------------------------------
// getMaxBytesBeforeFlush
//-----------------------------------------------------------------------------
const castor::tape::tapebridge::ConfigParam<uint64_t>
  &castor::tape::tapebridge::TapeFlushConfigParams::
  getMaxBytesBeforeFlush() const {
  return m_maxBytesBeforeFlush;
}


//-----------------------------------------------------------------------------
// getMaxFilesBeforeFlush
//-----------------------------------------------------------------------------
const castor::tape::tapebridge::ConfigParam<uint64_t>
  &castor::tape::tapebridge::TapeFlushConfigParams::
  getMaxFilesBeforeFlush() const {
  return m_maxFilesBeforeFlush;
}


//-----------------------------------------------------------------------------
// setTapeFlushMode
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::TapeFlushConfigParams::setTapeFlushMode(
  const uint32_t                value,
  const ConfigParamSource::Enum source) {
  m_tapeFlushMode.setValueAndSource(value, source);
}


//-----------------------------------------------------------------------------
// setMaxBytesBeforeFlush
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::TapeFlushConfigParams::setMaxBytesBeforeFlush(
  const uint64_t                value,
  const ConfigParamSource::Enum source) {
  m_maxBytesBeforeFlush.setValueAndSource(value, source);
}


//-----------------------------------------------------------------------------
// setMaxFilesBeforeFlush
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::TapeFlushConfigParams::setMaxFilesBeforeFlush(
  const uint64_t                value,
  const ConfigParamSource::Enum source) {
  m_maxFilesBeforeFlush.setValueAndSource(value, source);
}

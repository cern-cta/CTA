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


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::TapeFlushConfigParams::TapeFlushConfigParams():
  m_tapeFlushMode("TAPEBRIDGE", "TAPEFLUSHMODE", TAPEBRIDGE_N_FLUSHES_PER_FILE,
    "UNKNOWN"),
  m_maxBytesBeforeFlush("TAPEBRIDGE", "MAXBYTESBEFOREFLUSH", (uint64_t)0,
    "UNKNOWN"),
  m_maxFilesBeforeFlush("TAPEBRIDGE", "MAXFILESBEFOREFLUSH", (uint64_t)0,
    "UNKNOWN") {
  // Do nothing
}


//------------------------------------------------------------------------------
// determineConfigParams
//------------------------------------------------------------------------------
void castor::tape::tapebridge::TapeFlushConfigParams::determineConfigParams()
  throw(castor::exception::Exception) {

  determineTapeFlushMode();

  // Only try to determine the maximum number of bytes and files before a flush
  // if the flush mode requires them
  if(TAPEBRIDGE_ONE_FLUSH_PER_N_FILES == m_tapeFlushMode.value) {
    determineMaxBytesBeforeFlush();
    determineMaxFilesBeforeFlush();
  }
}


//------------------------------------------------------------------------------
// determineTapeFlushMode
//------------------------------------------------------------------------------
void castor::tape::tapebridge::TapeFlushConfigParams::determineTapeFlushMode()
  throw(castor::exception::Exception) {
  const std::string envVarName = m_tapeFlushMode.category + "_" +
    m_tapeFlushMode.name;
  const char *paramCStr  = NULL;

  // Try to get the value from the environment variables, else try to get it
  // from castor.conf
  if(NULL != (paramCStr = getenv(envVarName.c_str()))) {
    m_tapeFlushMode.source = "environment variable";
  } else if(NULL != (paramCStr = getconfent(m_tapeFlushMode.category.c_str(),
    m_tapeFlushMode.name.c_str(), 0))) {
    m_tapeFlushMode.source = "castor.conf";
  }

  // If we got the value, then try to convert it to a uint32_t, else use the
  // compile-time default
  if(NULL != paramCStr) {
    if(0 == strcmp("N_FLUSHES_PER_FILE", paramCStr)) {
      m_tapeFlushMode.value = TAPEBRIDGE_N_FLUSHES_PER_FILE;
    } else if(0 == strcmp("ONE_FLUSH_PER_N_FILES", paramCStr)) {
      m_tapeFlushMode.value = TAPEBRIDGE_ONE_FLUSH_PER_N_FILES;
    } else {
      castor::exception::InvalidArgument ex;
      ex.getMessage() <<
        "Value of configuration parameter is not valid"
        ": expected N_FLUSHES_PER_FILE or ONE_FLUSH_PER_N_FILES"
        ": category=" << m_tapeFlushMode.category <<
        " name=" << m_tapeFlushMode.name <<
        " value=" << paramCStr <<
        " source=" << m_tapeFlushMode.source;
      throw(ex);
    }
  } else {
    m_tapeFlushMode.source = "compile-time default";
    m_tapeFlushMode.value = TAPEBRIDGE_N_FLUSHES_PER_FILE;
  }
}


//------------------------------------------------------------------------------
// determineMaxBytesBeforeFlush
//------------------------------------------------------------------------------
void castor::tape::tapebridge::TapeFlushConfigParams::
  determineMaxBytesBeforeFlush() throw(castor::exception::Exception) {
  determineUint64ConfigParam(m_maxBytesBeforeFlush,
    TAPEBRIDGE_MAXBYTESBEFOREFLUSH);
}


//------------------------------------------------------------------------------
// determineMaxFilesBeforeFlush
//------------------------------------------------------------------------------
void castor::tape::tapebridge::TapeFlushConfigParams::
  determineMaxFilesBeforeFlush() throw(castor::exception::Exception) {
  determineUint64ConfigParam(m_maxFilesBeforeFlush,
    TAPEBRIDGE_MAXFILESBEFOREFLUSH);
}


//-----------------------------------------------------------------------------
// getTapeFlushMode
//-----------------------------------------------------------------------------
const castor::tape::tapebridge::ConfigParamAndSource<uint32_t>
  &castor::tape::tapebridge::TapeFlushConfigParams::getTapeFlushMode() const {
  return m_tapeFlushMode;
}


//-----------------------------------------------------------------------------
//
//-----------------------------------------------------------------------------
const castor::tape::tapebridge::ConfigParamAndSource<uint64_t>
  &castor::tape::tapebridge::TapeFlushConfigParams::
  getMaxBytesBeforeFlush() const {
  return m_maxBytesBeforeFlush;
}


//-----------------------------------------------------------------------------
//
//-----------------------------------------------------------------------------
const castor::tape::tapebridge::ConfigParamAndSource<uint64_t>
  &castor::tape::tapebridge::TapeFlushConfigParams::
  getMaxFilesBeforeFlush() const {
  return m_maxFilesBeforeFlush;
}

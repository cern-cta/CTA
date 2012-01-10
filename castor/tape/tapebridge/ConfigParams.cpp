/******************************************************************************
 *                castor/tape/tapebridge/ConfigParams.cpp
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
#include "castor/tape/tapebridge/ConfigParams.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/getconfent.h"
#include "h/u64subr.h"


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapebridge::ConfigParams::~ConfigParams() {
  // Do nothing
}


//------------------------------------------------------------------------------
// determineUint64ConfigParam
//------------------------------------------------------------------------------
void
  castor::tape::tapebridge::ConfigParams::determineUint64ConfigParam(
  ConfigParamAndSource<uint64_t> &param,
  const uint64_t                 compileTimeDefault)
  throw(castor::exception::Exception) {
  const std::string envVarName = param.category + "_" + param.name;
  const char *valueCStr = NULL;

  // Try to get the value from the environment variables, else try to get it
  // from castor.conf
  if(NULL != (valueCStr = getenv(envVarName.c_str()))) {
    param.source = "environment variable";
  } else if(NULL != (valueCStr = getconfent(param.category.c_str(),
    param.name.c_str(), 0))) {
    param.source = "castor.conf";
  }

  // If we got the value, then try to convert it to a uint64_t, else use the
  // compile-time default
  if(NULL != valueCStr) {
    if(!utils::isValidUInt(valueCStr)) {
      castor::exception::InvalidArgument ex;
      ex.getMessage() <<
        "Configuration parameter is not a valid unsigned integer"
        ": category=" << param.category <<
        " name=" << param.name <<
        " value=" << valueCStr <<
        " source=" << param.source;
      throw(ex);
    }
    param.value = strtou64(valueCStr);
  } else {
    param.source = "compile-time default";
    param.value  = compileTimeDefault;
  }
}

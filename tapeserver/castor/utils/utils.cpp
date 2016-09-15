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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/utils/utils.hpp"
#include "common/utils/strerror_r_wrapper.hpp"
#include "common/utils/utils.hpp" 
#include "h/Castor_limits.h"

#include <algorithm>
#include <errno.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <locale>
#include <sstream>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/prctl.h>
#include <sys/socket.h>

//-----------------------------------------------------------------------------
// copyString
//-----------------------------------------------------------------------------
void castor::utils::copyString(char *const dst, const size_t dstSize,
  const std::string &src) {

  if(dst == NULL) {
    cta::exception::Exception ex;

    ex.getMessage() << __FUNCTION__
      << ": Pointer to destination string is NULL";

    throw ex;
  }

  if(src.length() >= dstSize) {
    cta::exception::Exception ex;

    ex.getMessage() << __FUNCTION__
      << ": Source string is longer than destination.  Source length: "
      << src.length() << " Max destination length: " << (dstSize - 1);

    throw ex;
  }

  strncpy(dst, src.c_str(), dstSize);
  *(dst + dstSize -1) = '\0'; // Ensure destination string is null terminated
}

//------------------------------------------------------------------------------
// setProcessNameAndCmdLine
//------------------------------------------------------------------------------
void castor::utils::setProcessNameAndCmdLine(char *const argv0,
  const std::string &name) {
  try {
    setProcessName(name);
    setCmdLine(argv0, name);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to set process name and command-line"
      ": " << ne.getMessage().str();
  }
}

//------------------------------------------------------------------------------
// setProcessName
//------------------------------------------------------------------------------
void castor::utils::setProcessName(const std::string &name) {
  char buf[16];
  strncpy(buf, name.c_str(), sizeof(buf));
  buf[sizeof(buf)-1] = '\0';

  if(prctl(PR_SET_NAME, buf)) {
    const std::string errMsg = cta::utils::errnoToString(errno);
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to set process name: " << errMsg;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setCmdLine
//------------------------------------------------------------------------------
void castor::utils::setCmdLine(char *const argv0, const std::string &cmdLine)
  throw() {
  const size_t argv0Len = strlen(argv0);
  strncpy(argv0, cmdLine.c_str(), argv0Len);
  argv0[argv0Len] = '\0';
}


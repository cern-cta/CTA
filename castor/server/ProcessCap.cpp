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

#include "castor/exception/Exception.hpp"
#include "castor/server/ProcessCap.hpp"
#include "castor/server/SmartCap.hpp"
#include "serrno.h"

#include <errno.h>

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::ProcessCap::~ProcessCap()
  throw() {
}

//------------------------------------------------------------------------------
// getProcText
//------------------------------------------------------------------------------
std::string castor::server::ProcessCap::getProcText() {
  try {
    SmartCap cap(getProc());
    return toText((cap_t)cap.get());
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to get text representation of the capabilities of the process: "
      << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getProc
//------------------------------------------------------------------------------
cap_t castor::server::ProcessCap::getProc() {
  cap_t cap = cap_get_proc();
  if(NULL == cap) {
    char errBuf[81];
    sstrerror_r(errno, errBuf, sizeof(errBuf));
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to get the capabilities of the process: " << errBuf;
    throw ex;
  }
  return cap;
}

//------------------------------------------------------------------------------
// toText
//------------------------------------------------------------------------------
std::string castor::server::ProcessCap::toText(
  const cap_t cap) {
  // Create a C++ string with the result of calling cap_to_text()
  char *const text = cap_to_text(cap, NULL);
  if(NULL == text) {
    char errBuf[81];
    sstrerror_r(errno, errBuf, sizeof(errBuf));
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to create string representation of capability state: " << errBuf;
    throw ex;
  }
  std::string result(text);

  // Free the memory allocated by cap_to_text()
  if(cap_free(text)) {
    char errBuf[81];
    sstrerror_r(errno, errBuf, sizeof(errBuf));
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to free string representation of capability state: " << errBuf;
    throw ex;
  }

  // Return the C++ string
  return result;
}

//------------------------------------------------------------------------------
// setProcText
//------------------------------------------------------------------------------
void castor::server::ProcessCap::setProcText(const std::string &text) {
  try {
    SmartCap cap(fromText(text));
    setProc(cap.get());
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to set capabilities of process: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// fromText
//------------------------------------------------------------------------------
cap_t castor::server::ProcessCap::fromText(const std::string &text) {
  const cap_t cap = cap_from_text(text.c_str());
  if(NULL == cap) {
    char errBuf[81];
    sstrerror_r(errno, errBuf, sizeof(errBuf));
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to create capability state from string representation"
      ": text='" << text << "': " << errBuf;
    throw ex;
  }

  return cap;
}

//------------------------------------------------------------------------------
// setProc
//------------------------------------------------------------------------------
void castor::server::ProcessCap::setProc(const cap_t cap) {
  if(cap_set_proc(cap)) {
    char errBuf[81];
    sstrerror_r(errno, errBuf, sizeof(errBuf));
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to set the capabilities of the process: " << errBuf;
    throw ex;
  }
}

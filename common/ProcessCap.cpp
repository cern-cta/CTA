/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/exception/Exception.hpp"
#include "common/ProcessCap.hpp"
#include "castor/server/SmartCap.hpp"
#include "common/Utils.hpp"

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
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to get the capabilities of the process: " 
        << cta::utils::errnoToString(errno);
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
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to create string representation of capability state: " 
        << cta::utils::errnoToString(errno);
    throw ex;
  }
  std::string result(text);

  // Free the memory allocated by cap_to_text()
  if(cap_free(text)) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to free string representation of capability state: " 
        << cta::utils::errnoToString(errno);
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
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to create capability state from string representation"
      ": text='" << text << "': " << cta::utils::errnoToString(errno);
    throw ex;
  }

  return cap;
}

//------------------------------------------------------------------------------
// setProc
//------------------------------------------------------------------------------
void castor::server::ProcessCap::setProc(const cap_t cap) {
  if(cap_set_proc(cap)) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to set the capabilities of the process: " 
        << cta::utils::errnoToString(errno);
    throw ex;
  }
}

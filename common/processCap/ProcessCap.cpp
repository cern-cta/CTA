/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/exception/Exception.hpp"
#include "common/processCap/ProcessCap.hpp"
#include "common/processCap/SmartCap.hpp"
#include "common/utils/utils.hpp"

#include <errno.h>

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::server::ProcessCap::~ProcessCap() {}

//------------------------------------------------------------------------------
// getProcText
//------------------------------------------------------------------------------
std::string cta::server::ProcessCap::getProcText() {
  try {
    SmartCap cap(getProc());
    return toText((cap_t)cap.get());
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to get text representation of the capabilities of the process: "
      << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getProc
//------------------------------------------------------------------------------
cap_t cta::server::ProcessCap::getProc() {
  cap_t cap = cap_get_proc();
  if(NULL == cap) {
    cta::exception::Exception ex;
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
std::string cta::server::ProcessCap::toText(
  const cap_t cap) {
  // Create a C++ string with the result of calling cap_to_text()
  char *const text = cap_to_text(cap, NULL);
  if(NULL == text) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to create string representation of capability state: " 
        << cta::utils::errnoToString(errno);
    throw ex;
  }
  std::string result(text);

  // Free the memory allocated by cap_to_text()
  if(cap_free(text)) {
    cta::exception::Exception ex;
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
void cta::server::ProcessCap::setProcText(const std::string &text) {
  try {
    SmartCap cap(fromText(text));
    setProc(cap.get());
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to set capabilities of process: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// fromText
//------------------------------------------------------------------------------
cap_t cta::server::ProcessCap::fromText(const std::string &text) {
  const cap_t cap = cap_from_text(text.c_str());
  if(NULL == cap) {
    cta::exception::Exception ex;
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
void cta::server::ProcessCap::setProc(const cap_t cap) {
  if(cap_set_proc(cap)) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to set the capabilities of the process: " 
        << cta::utils::errnoToString(errno);
    throw ex;
  }
}

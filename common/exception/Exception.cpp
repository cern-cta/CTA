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

#include "Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::exception::Exception::Exception(const std::string& context, const bool embedBacktrace) :
m_backtrace(!embedBacktrace) {
  m_message << context;
}

//------------------------------------------------------------------------------
// copy constructor
//------------------------------------------------------------------------------
cta::exception::Exception::Exception(const cta::exception::Exception& rhs) : std::exception() {
  m_message << rhs.m_message.str();
  m_backtrace = rhs.m_backtrace;
}

//------------------------------------------------------------------------------
// assignment operator
//------------------------------------------------------------------------------
cta::exception::Exception& cta::exception::Exception::operator=(const cta::exception::Exception& rhs) {
  m_message << rhs.m_message.str();
  return *this;
}

//------------------------------------------------------------------------------
// what operator
//------------------------------------------------------------------------------
const char* cta::exception::Exception::what() const noexcept {
  m_what = getMessageValue();
  m_what += "\n";
  m_what += (std::string) m_backtrace;
  return m_what.c_str();
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::exception::Exception::~Exception() {}

//------------------------------------------------------------------------------
// setWhat
//------------------------------------------------------------------------------
void cta::exception::Exception::setWhat(const std::string& what) {
  getMessage() << what;
}

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

namespace cta::exception {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Exception::Exception(std::string_view context, const bool embedBacktrace) : 
  m_backtrace(!embedBacktrace) {
  m_message << context;
}

//------------------------------------------------------------------------------
// copy constructor
//------------------------------------------------------------------------------
Exception::Exception(const Exception& rhs) : std::exception() {
  m_message << rhs.m_message.str();
  m_backtrace = rhs.m_backtrace;
}

//------------------------------------------------------------------------------
// assignment constructor
//------------------------------------------------------------------------------
Exception& Exception::operator=(const Exception& rhs) {
  m_message << rhs.m_message.str();
  m_backtrace = rhs.m_backtrace;
  return *this;
}

//------------------------------------------------------------------------------
// what operator
//------------------------------------------------------------------------------
const char* Exception::what() const noexcept {
  m_what = getMessageValue();
  m_what += "\n";
  m_what += m_backtrace.str();
  return m_what.c_str();
}

//------------------------------------------------------------------------------
// setWhat
//------------------------------------------------------------------------------
void Exception::setWhat(std::string_view what) {
  getMessage() << what;
}

} // namespace cta::exception::Exception

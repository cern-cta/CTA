/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Exception.hpp"

namespace cta::exception {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Exception::Exception(std::string_view context, const bool embedBacktrace) : m_backtrace(!embedBacktrace) {
  m_message << context;
}

//------------------------------------------------------------------------------
// copy constructor
//------------------------------------------------------------------------------
Exception::Exception(const Exception& rhs)
    : std::exception(),
      m_message(rhs.m_message.str()),
      m_backtrace(rhs.m_backtrace) {}

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

}  // namespace cta::exception

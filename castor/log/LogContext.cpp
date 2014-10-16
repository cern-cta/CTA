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
 * Interface to the CASTOR logging system
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <list>
#include <algorithm>
#include <bfd.h>

#include "LogContext.hpp"
#include "Param.hpp"
#include "Logger.hpp"
#include "Ctape_constants.h"

castor::log::LogContext::LogContext(castor::log::Logger& logger) throw():
m_log(logger) {}

void castor::log::LogContext::pushOrReplace(const Param& param) throw() {
  ParamNameMatcher match(param.getName());
  std::list<Param>::iterator i = 
      std::find_if(m_params.begin(), m_params.end(), match);
  if (i != m_params.end()) {
    i->setValue(param.getValue());
  } else {
    m_params.push_back(param);
  }
}

void castor::log::LogContext::erase(const std::string& paramName) throw() {
  ParamNameMatcher match(paramName);
  m_params.erase(std::remove_if(m_params.begin(), m_params.end(), match), m_params.end());
}

void castor::log::LogContext::log(const int priority, const std::string& msg) throw() {
  m_log(priority, msg, m_params);
}

void castor::log::LogContext::log(const int priority, const std::string& msg, 
    const timeval& timeStamp) throw() {
  m_log(priority, msg, m_params, timeStamp);
}

void castor::log::LogContext::logBacktrace(const int priority, 
    const std::string& backtrace) throw() {
  // Sanity check to prevent substr from throwing exceptions
  if (!backtrace.size())
    return;
  size_t position = 0;
  int lineNumber = 0;
  bool stillGoing = true;
  while(stillGoing) {
    size_t next = backtrace.find_first_of("\n", position);
    std::string line;
    if(next != std::string::npos) { 
      line = backtrace.substr(position, next - position);
      // If our position is out of range, substr would throw an exception
      // so we check here if we would get out of range.
      position = next + 1;
      if (position >= backtrace.size())
        stillGoing = false;
    } else {
      stillGoing=false;
      line = backtrace.substr(position);
    }
    if (line.size()) {
      ScopedParam sp1 (*this, castor::log::Param("traceFrameNumber", lineNumber++));
      ScopedParam sp2 (*this, castor::log::Param("traceFrame", line));
      log(priority, "Stack trace");
    }
  }
}

castor::log::LogContext::ScopedParam::ScopedParam(
    LogContext& context, 
    const Param& param) throw(): 
    m_context(context), m_name(param.getName()) {
  m_context.pushOrReplace(param);
}

castor::log::LogContext::ScopedParam::~ScopedParam() throw() {
   m_context.erase(m_name);
}

std::ostream & castor::log::operator << (std::ostream & os, 
    const castor::log::LogContext & lc) {
  bool first=true;
  for (std::list<Param>::const_iterator p = lc.m_params.begin(); 
      p != lc.m_params.end(); ++p) {
    if (!first) {
      os << " ";
    } else {
      first = false;
    }
    os << p->getName() << "=" << p->getValue();
  }
  return os;
}

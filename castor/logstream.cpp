/******************************************************************************
 *                      logstream.cpp
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
 * @(#)logstream.cpp,v 1.7 $Release$ 2005/06/27 13:33:45 sponcec3
 *
 * A generic logstream for castor, handling IP addresses
 * and timestamps
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "logstream.h"
#if !defined(_WIN32)
#include <execinfo.h> /* for stackframe tracing */
#endif
#define MANIPULATORIMPL(T)                              \
  castor::logstream& castor::T(castor::logstream& s) {  \
    s.setLevel(DLF_LVL_##T);                            \
    return s;                                           \
  }

MANIPULATORIMPL(EMERGENCY)
MANIPULATORIMPL(ALERT)
MANIPULATORIMPL(ERROR)
MANIPULATORIMPL(WARNING)
MANIPULATORIMPL(AUTH)
MANIPULATORIMPL(SECURITY)
MANIPULATORIMPL(USAGE)
MANIPULATORIMPL(SYSTEM)
MANIPULATORIMPL(IMPORTANT)
MANIPULATORIMPL(DEBUG)

// Added VERBOSE manually in case DLF supports it in the future
castor::logstream& castor::VERBOSE(castor::logstream& s) {
  s.setLevel(DLF_LVL_DEBUG);
  return s;
}

castor::logstream& castor::ip(castor::logstream& s) {
  s.setIsIP(true);
  return s;
}

castor::logstream& castor::trace(castor::logstream& s) {
#if !defined(_WIN32)
  void* trace[20];
  int trace_size = backtrace(trace,20);
  char **symbols = backtrace_symbols(trace, trace_size);
  for (int i = 0; i < trace_size; i++) {
    s << symbols[i] << "\n";
  }
  s << std::flush;
  free(symbols);
#endif
  return s;
}

castor::logstream& castor::timeStamp(castor::logstream& s) {
  s.setIsTimeStamp(true);
  return s;
}

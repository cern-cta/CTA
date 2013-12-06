/******************************************************************************
 *                      Backtrace.cpp
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
 *
 * @author Eric Cano
 *****************************************************************************/

#include <execinfo.h>
#include <cxxabi.h>
#include <stdlib.h>
#include "Backtrace.hpp"

castor::exception::Backtrace::Backtrace(): m_trace() {
  void * array[200];
  size_t depth = ::backtrace(array, sizeof(array)/sizeof(void*));
  char ** strings = ::backtrace_symbols(array, depth);
  if (!strings)
    m_trace = "";
  else {
    for (size_t i=0; i<depth; i++) {
      std::string line(strings[i]);
      /* Demangle the c++, if possible. We expect the c++ function name's to live
       * between a '(' and a +
       * line format: /usr/lib/somelib.so.1(_Mangle2Mangle3Ev+0x123) [0x12345] */
      if ((std::string::npos != line.find("(")) && (std::string::npos != line.find("+"))) {
        std::string before, theFunc, after;
        before = line.substr(0, line.find("(")+1);
        theFunc = line.substr(line.find("(")+1, line.find("+") - (line.find("(") + 1));
        after = line.substr(line.find("+"), std::string::npos);
        int status(-1);
        char demangled[200];
        size_t length(sizeof(demangled));
        abi::__cxa_demangle(theFunc.c_str(), demangled, &length, &status);
        if (0 == status) {
          m_trace += before;
          m_trace += demangled;
          m_trace += after;
          m_trace += "(C++ demangled)\n";
        } else {
          m_trace += strings[i];
          m_trace += "\n";
        }
      } else {
        m_trace += strings[i];
        m_trace += "\n";
      }  
    }
    free (strings);
  }
}


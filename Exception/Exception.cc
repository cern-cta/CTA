// ----------------------------------------------------------------------
// File: Exception/Exception.cc
// Author: Eric Cano - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/
#define _XOPEN_SOURCE 600
#include "Exception.hh"
#include <stdlib.h>
#include <errno.h>
/* We want the thread safe (and portable) version of strerror */
#include <string.h>
#include <sstream>
#include <iosfwd>
#include <sstream>
#include <execinfo.h>
#include <cxxabi.h>


/* TODO remove me: it should be temporary */
#include <iostream>

const char * Tape::Exception::what() const throw () {
  return m_what.c_str();
}

const char * Tape::Exception::shortWhat() const throw () {
  return m_shortWhat.c_str();
}

void Tape::Exception::setWhat(const std::string& what) {
  std::stringstream w;
  w << what << std::endl << std::string(backtrace);
  m_what = w.str();
  m_shortWhat = what;
}

Tape::Exceptions::Errnum::Errnum(std::string what):Exception("") {
  m_errnum = errno;
  char s[1000];
  /* _XOPEN_SOURCE seems not to work.  */
  char * errorStr = ::strerror_r(m_errnum, s, sizeof(s));
  if (!errorStr) {
    int new_errno = errno;
    std::stringstream w;
    w << "Errno=" << m_errnum << ". In addition, failed to read the corresponding error string (strerror gave errno="
            << new_errno << ")";
    m_strerror = w.str();
  } else {
    m_strerror = errorStr;
  }
  std::stringstream w2;
  if (what.size())
    w2 << what << " ";
  w2 << "Errno=" << m_errnum << ": " << m_strerror;
  setWhat(w2.str());
}

Tape::Exceptions::Backtrace::Backtrace() {
  void * array[200];
  size_t depth = ::backtrace(array, sizeof(array)/sizeof(void*));
  char ** strings = ::backtrace_symbols(array, depth);
  if (!strings)
    m_trace = "";
  else {
    std::stringstream trc;
    for (int i=0; i<depth; i++) {
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
        if (0 == status)
          trc << before << demangled << after << " (C++ demangled)" << std::endl;
        else
          trc << strings[i] << std::endl;
      } else {
        trc << strings[i] << std::endl;
      }  
    }
    free (strings);
    m_trace = trc.str();
  }
}



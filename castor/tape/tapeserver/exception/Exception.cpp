/******************************************************************************
 *                      Exception.cpp
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

#define _XOPEN_SOURCE 600
#include "Exception.hpp"

#include <stdlib.h>
#include <errno.h>
/* We want the thread safe (and portable) version of strerror */
#include <string.h>
#include <sstream>
#include <iosfwd>


/* TODO remove me: it should be temporary */
#include <iostream>

using namespace castor::tape;

void Exception::setWhat(const std::string& what) {
  getMessage() << what;
}

Exception::Exception(const Exception &ex): castor::exception::Exception(0) {
  getMessage() << ex.getMessageValue();
  m_backtrace = ex.m_backtrace;
}

exceptions::Errnum::Errnum(std::string what):Exception("") {
  m_errnum = errno;
  ErrnumConstructorBottomHalf(what);
}

exceptions::Errnum::Errnum(int err, std::string what):Exception("") {
  m_errnum = err;
  ErrnumConstructorBottomHalf(what);
}

void exceptions::Errnum::ErrnumConstructorBottomHalf(const std::string & what) {
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

void exceptions::throwOnReturnedErrno (int err, std::string context) {
  if (err) throw Errnum(err, context);
}

void exceptions::throwOnNonZeroWithErrno (int status, std::string context) {
  if (status) throw Errnum(context);
}




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

#include "Exception.hh"
#include <errno.h>
/* We want the thread safe (and portable) version of strerror */
#define _XOPEN_SOURCE 600
#include <string.h>
#include <sstream>
#include <iosfwd>
#include <sstream>

Tape::Exceptions::Errnum::Errnum(std::string what):Exception(what) {
  m_errnum = errno;
  char s[1000];
  if (::strerror_r(m_errnum, s, sizeof(s))) {
    int new_errno = errno;
    std::stringstream w;
    w << "Errno=" << m_errnum << ". In addition, failed to read the corresponding error string (with errno="
            << new_errno << ")";
    m_strerror = w.str();
  } else {
    m_strerror = std::string(s);
  }
  std::stringstream w2;
  w2 << "Errno=" << m_errnum << ": " << m_strerror;
  if (m_what.size())
    m_what += " ";
  m_what += w2.str();
}

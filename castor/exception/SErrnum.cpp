/******************************************************************************
 *                      SErrnum.cpp
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
 * @author castor dev team
 *****************************************************************************/
#include "SErrnum.hpp"
#include <serrno.h>
#include <errno.h>
#include <string.h>

using namespace castor::exception;

SErrnum::SErrnum(std::string what):Exception("") {
  m_serrnum = serrno;
  SErrnumConstructorBottomHalf(what);
}

SErrnum::SErrnum(int err, std::string what):Exception("") {
  m_serrnum = err;
  SErrnumConstructorBottomHalf(what);
}

void SErrnum::SErrnumConstructorBottomHalf(const std::string & what) {
  char s[1000];
  /* _XOPEN_SOURCE seems not to work.  */
  char * errorStr = ::sstrerror_r(m_serrnum, s, sizeof(s));
  if (!errorStr) {
    int new_errno = errno;
    std::stringstream w;
    w << "SErrno=" << m_serrnum << ". In addition, failed to read the corresponding error string (sstrerror gave errno="
            << new_errno << ")";
    m_sstrerror = w.str();
  } else {
    m_sstrerror = errorStr;
  }
  std::stringstream w2;
  if (what.size())
    w2 << what << " ";
  w2 << "SErrno=" << m_serrnum << ": " << m_sstrerror;
  getMessage().str(w2.str());
}

void SErrnum::throwOnReturnedErrno (int err, std::string context) {
  if (err) throw SErrnum(err, context);
}

void SErrnum::throwOnNonZero(int status, std::string context) {
  if (status) throw SErrnum(context);
}

void SErrnum::throwOnZero(int status, std::string context) {
  if (!status) throw SErrnum(context);
}

void SErrnum::throwOnNull(void * f, std::string context) {
  if (NULL == f) throw SErrnum(context);
}

void SErrnum::throwOnNegative(int ret, std::string context) {
  if (ret < 0) throw SErrnum(context);
}

void SErrnum::throwOnMinusOne(int ret, std::string context) {
  if (-1 == ret) throw SErrnum(context);
}

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
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/exception/SErrnum.hpp"
#include "serrno.h"

#include <errno.h>
#include <string.h>

using namespace castor::exception;

SErrnum::SErrnum(std::string what):Exception("") {
  m_serrnum = serrno;
  m_errnum = errno;
  SErrnumConstructorBottomHalf(what);
}

SErrnum::SErrnum(int err, std::string what):Exception("") {
  m_serrnum = err;
  m_errnum = errno;
  SErrnumConstructorBottomHalf(what);
}

void SErrnum::SErrnumConstructorBottomHalf(const std::string & what) {
  char buf[100];

  // Castor-style error reporting is not for the faint of the heart.
  // If we get serrno = 0, we should rely on the errnum instead
  if(m_serrnum) {
    // Normal case, serrnum is not zero.
    if(sstrerror_r(m_serrnum, buf, sizeof(buf))) {
      // sstrerror_r() failed
      const int new_errno = errno;
      std::stringstream w;
      w << "SErrno=" << m_serrnum << ". In addition, failed to read the corresponding error string (sstrerror_r gave errno="
              << new_errno << ")";
      m_sstrerror = w.str();
    } else {
      // sstrerror_r() succeeded
      m_sstrerror = buf;
    }
    std::stringstream w2;
    if (what.size())
      w2 << what << " ";
    w2 << "SErrno=" << m_serrnum << ": " << m_sstrerror;
    getMessage().str(w2.str());
  } else {
    // degraded case, serrnum is indeed 0, we fall back on the normal error reporting.
    std::stringstream w;
    w << "Serrno=0. Failling back to errno=" << m_errnum;
    char buf[100];

    if(sstrerror_r(m_errnum, buf, sizeof(buf))) {
      // sstrerror_r() failed
      const int new_errno = errno;
      w << ". In addition, failed to read the corresponding error string (sstrerror_r gave errno="
              << new_errno << ")";
    } else {
      // sstrerror_r() succeeded
      w << ". "<< buf;
    }
    std::stringstream w2;
    if (what.size())
      w2 << what << " ";
    w2 << w.str();
    getMessage().str(w2.str());
  }
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

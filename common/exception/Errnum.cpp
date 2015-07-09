/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/exception/Errnum.hpp"
#include "common/Utils.hpp"

#include <errno.h>
#include <string.h>

using namespace cta::exception;

Errnum::Errnum(std::string what):Exception("") {
  m_errnum = errno;
  ErrnumConstructorBottomHalf(what);
}

Errnum::Errnum(int err, std::string what):Exception("") {
  m_errnum = err;
  ErrnumConstructorBottomHalf(what);
}

void Errnum::ErrnumConstructorBottomHalf(const std::string & what) {
  m_strerror = Utils::errnoToString(m_errnum);
  std::stringstream w2;
  if (what.size())
    w2 << what << " ";
  w2 << "Errno=" << m_errnum << ": " << m_strerror;
  getMessage().str(w2.str());
}

void Errnum::throwOnReturnedErrno (int err, std::string context) {
  if (err) throw Errnum(err, context);
}

void Errnum::throwOnNonZero(int status, std::string context) {
  if (status) throw Errnum(context);
}

void Errnum::throwOnZero(int status, std::string context) {
  if (!status) throw Errnum(context);
}

void Errnum::throwOnNull(void * f, std::string context) {
  if (NULL == f) throw Errnum(context);
}

void Errnum::throwOnNegative(int ret, std::string context) {
  if (ret < 0) throw Errnum(context);
}

void Errnum::throwOnMinusOne(int ret, std::string context) {
  if (-1 == ret) throw Errnum(context);
}

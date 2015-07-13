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

#include "common/exception/Serrnum.hpp"
#include "common/Utils.hpp"

#include <errno.h>
#include <string.h>

using namespace cta::exception;

Serrnum::Serrnum(std::string what):Exception("") {
  m_serrnum = errno;
  SerrnumConstructorBottomHalf(what);
}

Serrnum::Serrnum(int err, std::string what):Exception("") {
  m_serrnum = err;
  SerrnumConstructorBottomHalf(what);
}

void Serrnum::SerrnumConstructorBottomHalf(const std::string & what) {
  m_strerror = Utils::serrnoToString(m_serrnum);
  std::stringstream w2;
  if (what.size())
    w2 << what << " ";
  w2 << "Errno=" << m_serrnum << ": " << m_strerror;
  getMessage().str(w2.str());
}

void Serrnum::throwOnReturnedErrno (const int err, const std::string &context) {
  if (err) throw Serrnum(err, context);
}

void Serrnum::throwOnNonZero(const int status, const std::string &context) {
  if (status) throw Serrnum(context);
}

void Serrnum::throwOnZero(const int status, const std::string &context) {
  if (!status) throw Serrnum(context);
}

void Serrnum::throwOnNull(const void *const f, const std::string &context) {
  if (NULL == f) throw Serrnum(context);
}

void Serrnum::throwOnNegative(const int ret, const std::string &context) {
  if (ret < 0) throw Serrnum(context);
}

void Serrnum::throwOnMinusOne(const int ret, const std::string &context) {
  if (-1 == ret) throw Serrnum(context);
}

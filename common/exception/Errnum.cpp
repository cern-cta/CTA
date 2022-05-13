/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/exception/Errnum.hpp"
#include "common/utils/utils.hpp"

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
  m_strerror = utils::errnoToString(m_errnum);
  std::stringstream w2;
  if (what.size())
    w2 << what << " ";
  w2 << "Errno=" << m_errnum << ": " << m_strerror;
  getMessage() << w2.str();
}

void Errnum::throwOnReturnedErrno (const int err, const std::string &context) {
  if (err) throw Errnum(err, context);
}

void Errnum::throwOnNonZero(const int status, const std::string &context) {
  if (status) throw Errnum(context);
}

void Errnum::throwOnZero(const int status, const std::string &context) {
  if (!status) throw Errnum(context);
}

void Errnum::throwOnNull(const void *const f, const std::string &context) {
  if (nullptr == f) throw Errnum(context);
}

void Errnum::throwOnNegative(const int ret, const std::string &context) {
  if (ret < 0) throw Errnum(context);
}

void Errnum::throwOnMinusOne(const int ret, const std::string &context) {
  if (-1 == ret) throw Errnum(context);
}

void Errnum::throwOnNegativeErrnoIfNegative(const int ret, const std::string& context) {
  if (ret < 0) throw Errnum(-ret, context);
}

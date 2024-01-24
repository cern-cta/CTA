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

namespace cta::exception {

Errnum::Errnum(int err, std::string_view what) : Exception(),
  m_errnum(err),
  m_strerror(utils::errnoToString(err)) {
  std::stringstream what2;
  what2 << what << (what.empty() ? "" : " ")
        << "Errno=" << m_errnum << ": " << m_strerror;
  getMessage() << what2.str();
}

Errnum::Errnum(std::string_view what) : Errnum(errno, what) { }

void Errnum::throwOnReturnedErrno(int err, std::string_view context) {
  if(err) throw Errnum(err, context);
}

void Errnum::throwOnNonZero(int status, std::string_view context) {
  if(status) throw Errnum(context);
}

void Errnum::throwOnNull(const void* const ptr, std::string_view context) {
  if(nullptr == ptr) throw Errnum(context);
}

void Errnum::throwOnMinusOne(ssize_t ret, std::string_view context) {
  if(ret == -1) throw Errnum(context);
}

} // namespace cta::exception

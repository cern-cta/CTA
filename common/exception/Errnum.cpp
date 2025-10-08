/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Errnum.hpp"
#include "common/utils/utils.hpp"

namespace cta::exception {

Errnum::Errnum(int err, std::string_view what) : Exception(),
  m_errnum(err),
  m_strerror(utils::errnoToString(err)) {
  std::stringstream whatWithErrnum;
  whatWithErrnum << what << (what.empty() ? "" : " ")
                 << "Errno=" << m_errnum << ": " << m_strerror;
  getMessage() << whatWithErrnum.str();
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

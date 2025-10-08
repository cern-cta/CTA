/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/exception/Exception.hpp"

namespace cta::exception {

class Errnum : public Exception {
public:
  explicit Errnum(std::string_view what = "");
  Errnum(int err, std::string_view what = "");
  virtual ~Errnum() = default;

  int errorNumber() const { return m_errnum; }

  static void throwOnReturnedErrno(int err, std::string_view context = "");
  static void throwOnNonZero(int status, std::string_view context = "");
  static void throwOnNull(const void *const f, std::string_view context = "");
  static void throwOnMinusOne(ssize_t ret, const std::string_view context = "");

private:
  int m_errnum;
  std::string m_strerror;
};

} // namespace cta::exception

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

#pragma once

#include "common/exception/Exception.hpp"

namespace cta::exception {

class Errnum : public Exception {
public:
  explicit Errnum(std::string_view what = "");
  Errnum(int err, std::string_view what = "");
  virtual ~Errnum() = default;

  int errorNumber() const { return m_errnum; }
  std::string strError() const { return m_strerror; }

  static void throwOnReturnedErrno(int err, std::string_view context = "");
  static void throwOnNonZero(int status, std::string_view context = "");
  static void throwOnZero(int status, std::string_view context = "");
  static void throwOnNull(const void *const f, std::string_view context = "");
  static void throwOnNegative(int ret, const std::string_view context = "");
  static void throwOnMinusOne(int ret, const std::string_view context = "");
  static void throwOnMinusOne(ssize_t ret, const std::string_view context = "");
  static void throwOnNegativeErrnoIfNegative(int ret, std::string_view context = "");

  template <typename F>
  static void throwOnReturnedErrnoOrThrownStdException(F f, std::string_view context = "") {
    try {
      throwOnReturnedErrno(f(), context);
    } catch(Errnum&) {
      throw; // Let the exception of throwOnReturnedErrno pass through
    } catch(std::error_code& ec) {
      throw Errnum(ec.value(), std::string(context) + " Got a std::error_code: " + ec.message());
    } catch(std::exception& ex) {
      throw Exception(std::string(context) + " Got a std::exception: " + ex.what());
    }
  }

private:
  void ErrnumConstructorBottomHalf(std::string_view what);

  int m_errnum;
  std::string m_strerror;
};

} // namespace cta::exception

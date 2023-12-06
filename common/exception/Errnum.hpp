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

#include "Exception.hpp"
#include <functional>
#include <system_error>

namespace cta::exception {

  class Errnum: public cta::exception::Exception {
  public:
    explicit Errnum(std::string what = "");
    Errnum(int err, std::string what = "");
    virtual ~Errnum() = default;
    int errorNumber() const { return m_errnum; }
    std::string strError() const { return m_strerror; }
    static void throwOnReturnedErrno(const int err, const std::string &context = "");
    template <typename F>
    static void throwOnReturnedErrnoOrThrownStdException (F f, const std::string &context = "") {
      try {
        throwOnReturnedErrno(f(), context);
      } catch (Errnum &) {
        throw; // Let the exception of throwOnReturnedErrno pass through.
      } catch (std::error_code & ec) {
        throw Errnum(ec.value(), context + " Got an std::error_code: " + ec.message());
      } catch (std::exception & ex) {
        throw Exception(context + " Got a standard exception: " + ex.what());
      }
    }
    static void throwOnNonZero(const int status, const std::string &context = "");
    static void throwOnZero(const int status, const std::string &context = "");
    static void throwOnNull(const void *const f, const std::string &context = "");
    static void throwOnNegative(const int ret, const std::string &context = "");
    static void throwOnMinusOne(const int ret, const std::string &context = "");
    static void throwOnNegativeErrnoIfNegative(const int ret, const std::string &context = "");
  protected:
    void ErrnumConstructorBottomHalf(const std::string & what);
    int m_errnum;
    std::string m_strerror;
  };

} // namespace cta::exception

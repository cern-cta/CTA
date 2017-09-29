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

#pragma once

#include "Exception.hpp"

namespace cta {
namespace exception {
  class Errnum: public cta::exception::Exception {
  public:
    Errnum(std::string what = "");
    Errnum (int err, std::string what = "");
    virtual ~Errnum() throw() {};
    int errorNumber() const { return m_errnum; }
    std::string strError() const { return m_strerror; }
    static void throwOnReturnedErrno(const int err, const std::string &context = "");
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
}
}

/******************************************************************************
 *                      Errnum.hpp
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
 * @author castor dev team
 *****************************************************************************/

#pragma once

#include "Exception.hpp"

namespace castor {
namespace exception {
  class Errnum: public castor::exception::Exception {
  public:
    Errnum(std::string what = "");
	  Errnum (int err, std::string what = "");
    virtual ~Errnum() throw() {};
    int ErrorNumber() { return m_errnum; }
    std::string strError() { return m_strerror; }
    static void throwOnReturnedErrno(int err, std::string context = "");
    static void throwOnNonZero(int status, std::string context = "");
    static void throwOnZero(int status, std::string context = "");
    static void throwOnNull(void * f, std::string context = "");
    static void throwOnNegative(int ret, std::string context = "");
    static void throwOnMinusOne(int ret, std::string context = "");
  protected:
	  void ErrnumConstructorBottomHalf(const std::string & what);
    int m_errnum;
    std::string m_strerror;
  };
}
}

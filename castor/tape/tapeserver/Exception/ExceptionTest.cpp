/******************************************************************************
 *                      ExceptionTest.cpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "Exception.hpp"
#include <errno.h>

#include <gtest/gtest.h>
#include <gmock/gmock-cardinalities.h>

namespace UnitTests {
  class Nested {
  public:
    void f1();
    void f2();
    Nested();
  };
  
  /* Prevent inlining: it makes this test fail! */
  void __attribute__((noinline)) Nested::f1() {
    throw castor::tape::Exception("");
  }
  
  /* Prevent inlining: it makes this test fail!
   * Even with that, f2 does not show up in the trace */
  void __attribute__((noinline)) Nested::f2() {
    f1();
  }
  
  /* Prevent inlining: it makes this test fail! */
  __attribute__((noinline))  Nested::Nested() {
    f2();
  }

  TEST(Exceptions, stacktrace_with_demangling) {
    try {
      Nested x;
    } catch (castor::tape::Exception & e) {
      std::string bt = e.backtrace();
      ASSERT_NE(std::string::npos, bt.find("Nested::f1"));
      ASSERT_NE(std::string::npos, bt.find("castor::exception::Backtrace::Backtrace"));
      ASSERT_EQ("", std::string(e.getMessageValue()));
      std::string fullWhat(e.what());
      ASSERT_NE(std::string::npos, fullWhat.find("Nested::f1"));
    }
  }
  
  TEST(Exceptions, errnum_throwing) {
    /* Mickey Mouse test as we had trouble which throwing Errnum (with errno=ENOENT)*/
    errno = ENOENT;
    try {
      throw castor::tape::Exceptions::Errnum("Test ENOENT");
    } catch (std::exception & e) {
      std::string temp = e.what();
      temp += " ";
    }
  }
}

// ----------------------------------------------------------------------
// File: Exception/ExceptionTest.cc
// Author: Eric Cano - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "Exception.hh"
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
    throw Tape::Exception("");
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
    } catch (Tape::Exception & e) {
      std::string bt = e.backtrace;
      ASSERT_NE(std::string::npos, bt.find("Nested::f1"));
      ASSERT_NE(std::string::npos, bt.find("Tape::Exceptions::Backtrace::Backtrace"));
    }
  }
}

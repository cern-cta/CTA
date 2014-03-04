/******************************************************************************
 *                castor/utils/SmartArrayPtrTest.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/utils/SmartArrayPtr.hpp"

#include <gtest/gtest.h>
#include <list>
#include <stdlib.h>
#include <string>
#include <sys/time.h>
#include <unistd.h>
#include <vector>

namespace unitTests {

class castor_utils_SmartArrayPtrTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_utils_SmartArrayPtrTest, constructor) {
  char *ptr = new char[10];
  castor::utils::SmartArrayPtr<char> smartPtr(ptr);

  ASSERT_EQ(ptr, smartPtr.get());
}

TEST_F(castor_utils_SmartArrayPtrTest, reset) {
  char *ptr = new char[10];
  castor::utils::SmartArrayPtr<char> smartPtr;

  ASSERT_EQ((char *)0, smartPtr.get());
  smartPtr.reset(ptr);
  ASSERT_EQ(ptr, smartPtr.get());
}

TEST_F(castor_utils_SmartArrayPtrTest, assignment) {
  castor::utils::SmartArrayPtr<char> smartPtr1;
  castor::utils::SmartArrayPtr<char> smartPtr2;

  ASSERT_EQ((char *)0, smartPtr1.get());
  ASSERT_EQ((char *)0, smartPtr2.get());

  char *ptr = new char[10];
  smartPtr1.reset(ptr);
  ASSERT_EQ(ptr, smartPtr1.get());

  smartPtr2 = smartPtr1;
  ASSERT_EQ((char *)0, smartPtr1.get());
  ASSERT_EQ(ptr, smartPtr2.get());
}

TEST_F(castor_utils_SmartArrayPtrTest, releaseNull) {
  castor::utils::SmartArrayPtr<char> smartPtr;
  ASSERT_THROW(smartPtr.release(), castor::exception::NotAnOwner);
}

TEST_F(castor_utils_SmartArrayPtrTest, subscript) {
  char *ptr = new char[4];
  ptr[0] = 'T';
  ptr[1] = 'e';
  ptr[2] = 's';
  ptr[3] = 't';

  castor::utils::SmartArrayPtr<char> smartPtr(ptr);
  ASSERT_EQ(ptr, smartPtr.get());
  ASSERT_EQ('T', smartPtr[0]);
  ASSERT_EQ('e', smartPtr[1]);
  ASSERT_EQ('s', smartPtr[2]);
  ASSERT_EQ('t', smartPtr[3]);
}

} // namespace unitTests

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

#include "common/SmartArrayPtr.hpp"

#include <gtest/gtest.h>
#include <list>
#include <stdlib.h>
#include <string>
#include <sys/time.h>
#include <unistd.h>
#include <vector>

namespace unitTests {

class cta_SmartArrayPtrTest : public ::testing::Test {
protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(cta_SmartArrayPtrTest, constructor) {
  char* ptr = new char[10];
  cta::SmartArrayPtr<char> smartPtr(ptr);

  ASSERT_EQ(ptr, smartPtr.get());
}

TEST_F(cta_SmartArrayPtrTest, reset) {
  char* ptr = new char[10];
  cta::SmartArrayPtr<char> smartPtr;

  ASSERT_EQ((char*) 0, smartPtr.get());
  smartPtr.reset(ptr);
  ASSERT_EQ(ptr, smartPtr.get());
}

TEST_F(cta_SmartArrayPtrTest, assignment) {
  cta::SmartArrayPtr<char> smartPtr1;
  cta::SmartArrayPtr<char> smartPtr2;

  ASSERT_EQ((char*) 0, smartPtr1.get());
  ASSERT_EQ((char*) 0, smartPtr2.get());

  char* ptr = new char[10];
  smartPtr1.reset(ptr);
  ASSERT_EQ(ptr, smartPtr1.get());

  smartPtr2 = smartPtr1;
  ASSERT_EQ((char*) 0, smartPtr1.get());
  ASSERT_EQ(ptr, smartPtr2.get());
}

TEST_F(cta_SmartArrayPtrTest, releaseNull) {
  cta::SmartArrayPtr<char> smartPtr;
  ASSERT_THROW(smartPtr.release(), cta::exception::NotAnOwner);
}

TEST_F(cta_SmartArrayPtrTest, subscriptRead) {
  char* ptr = new char[4];
  ptr[0] = 'T';
  ptr[1] = 'e';
  ptr[2] = 's';
  ptr[3] = 't';

  cta::SmartArrayPtr<char> smartPtr(ptr);
  ASSERT_EQ(ptr, smartPtr.get());

  ASSERT_EQ('T', smartPtr[0]);
  ASSERT_EQ('e', smartPtr[1]);
  ASSERT_EQ('s', smartPtr[2]);
  ASSERT_EQ('t', smartPtr[3]);
}

TEST_F(cta_SmartArrayPtrTest, subscriptAssigment) {
  char* ptr = new char[4];
  ptr[0] = 'T';
  ptr[1] = 'e';
  ptr[2] = 's';
  ptr[3] = 't';

  cta::SmartArrayPtr<char> smartPtr(ptr);
  ASSERT_EQ(ptr, smartPtr.get());

  ASSERT_EQ('T', smartPtr[0]);
  ASSERT_EQ('e', smartPtr[1]);
  ASSERT_EQ('s', smartPtr[2]);
  ASSERT_EQ('t', smartPtr[3]);

  smartPtr[0] = '0';
  smartPtr[1] = '1';
  smartPtr[2] = '2';
  smartPtr[3] = '3';

  ASSERT_EQ('0', ptr[0]);
  ASSERT_EQ('1', ptr[1]);
  ASSERT_EQ('2', ptr[2]);
  ASSERT_EQ('3', ptr[3]);

  ASSERT_EQ('0', smartPtr[0]);
  ASSERT_EQ('1', smartPtr[1]);
  ASSERT_EQ('2', smartPtr[2]);
  ASSERT_EQ('3', smartPtr[3]);
}

}  // namespace unitTests

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

#include "optional.hpp"

#include <gtest/gtest.h>
#include <string>
#include <algorithm>

namespace unitTests {

class cta_optionalTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_optionalTest, empty_constructors) {
  using namespace cta;

  const optional<int> o1;
  const optional<int> o2={};
  const optional<int> o3=nullopt;

  ASSERT_TRUE(o1 == o2);
  ASSERT_TRUE(o2 == o3);
  ASSERT_TRUE(o1 == nullopt);
  ASSERT_FALSE(bool(o1));
  ASSERT_FALSE(bool(o2));
  ASSERT_FALSE(bool(o3));
}

TEST_F(cta_optionalTest, int_constructors) {
  using namespace cta;

  const optional<int> o1;
  const optional<int> o2=5;
  const optional<int> o3(7);
  const int x = 9;
  const optional<int> o4(x);
  const int y = 3;
  const optional<int> o5(std::move(y));

  ASSERT_FALSE(o1 == o2);
  ASSERT_FALSE(o2 == o3);
  ASSERT_FALSE(o3 == o4);
  ASSERT_FALSE(o4 == o5);
  ASSERT_TRUE(o1 == nullopt);
  ASSERT_TRUE(o2 == 5);
  ASSERT_TRUE(o3 == 7);
  ASSERT_TRUE(o4 == x);
  ASSERT_TRUE(o5 == 3);
  ASSERT_FALSE(bool(o1));
  ASSERT_TRUE(bool(o2));
  ASSERT_TRUE(bool(o3));
  ASSERT_TRUE(bool(o4));
  ASSERT_TRUE(bool(o5));
}

TEST_F(cta_optionalTest, string_constructors) {
  using namespace cta;

  const optional<std::string> o1={};
  const optional<std::string> o2("Hello");
  const optional<std::string> o3("World");
  const std::string x = "Hi";
  const optional<std::string> o4(x);
  const std::string y = "Mum!";
  const optional<std::string> o5(std::move(y));
  const optional<std::string> o6("");

  ASSERT_FALSE(o1 == o2);
  ASSERT_FALSE(o2 == o3);
  ASSERT_FALSE(o3 == o4);
  ASSERT_FALSE(o4 == o5);
  ASSERT_TRUE(o1 == nullopt);
  ASSERT_TRUE(o2 == std::string("Hello"));
  ASSERT_TRUE(o3 == std::string("World"));
  ASSERT_TRUE(o4 == x);
  ASSERT_TRUE(o5 == std::string("Mum!"));
  ASSERT_FALSE(bool(o1));
  ASSERT_TRUE(bool(o2));
  ASSERT_TRUE(bool(o3));
  ASSERT_TRUE(bool(o4));
  ASSERT_TRUE(bool(o5));
  ASSERT_TRUE(bool(o6));
}

TEST_F(cta_optionalTest, bool_constructors) {
  using namespace cta;

  const optional<bool> o1={};
  const optional<bool> o2(true);
  const optional<bool> o3(false);
  const bool x = true;
  const optional<bool> o4(x);
  const bool y = false;
  const optional<bool> o5(std::move(y));

  ASSERT_FALSE(o1 == o2);
  ASSERT_FALSE(o2 == o3);
  ASSERT_FALSE(o3 == o4);
  ASSERT_FALSE(o4 == o5);
  ASSERT_TRUE(o1 == nullopt);
  ASSERT_TRUE(o2 == true);
  ASSERT_TRUE(o3 == false);
  ASSERT_TRUE(o4 == x);
  ASSERT_TRUE(o5 == false);
  ASSERT_FALSE(bool(o1));
  ASSERT_TRUE(bool(o2));
  ASSERT_TRUE(bool(o3));
  ASSERT_TRUE(bool(o4));
  ASSERT_TRUE(bool(o5));
}

TEST_F(cta_optionalTest, assignments) {
  using namespace cta;

  optional<bool> o1={};
  optional<bool> o2(true);
  ASSERT_TRUE(o1 == nullopt);  
  o1 = o2;
  ASSERT_TRUE(o1 == true); 
  o1 = false;
  ASSERT_TRUE(o1 == false);
  ASSERT_TRUE(bool(o1));
  bool x = true;
  o1 = std::move(x);
  ASSERT_TRUE(o1 == true);
}

TEST_F(cta_optionalTest, accessing_value) {
  using namespace cta;

  const optional<std::string> o1={};
  const optional<std::string> o2("Hello");
  const optional<std::string> o3("World");
  
  ASSERT_THROW(o1.value(), std::logic_error);
  ASSERT_NO_THROW(o2.value());
  ASSERT_TRUE(o2.value() == "Hello");
  ASSERT_NO_THROW(*o2);
  ASSERT_TRUE(*o2 == "Hello");
  ASSERT_NO_THROW(o2->size());
  ASSERT_TRUE(o2->size() == 5);
}

TEST_F(cta_optionalTest, swap) {
  using namespace cta;

  optional<std::string> o1={};
  optional<std::string> o2("Hello");
  
  ASSERT_NO_THROW(o1.swap(o2));
  ASSERT_THROW(o2.value(), std::logic_error);
  ASSERT_NO_THROW(o1.value());
  ASSERT_TRUE(o1.value() == "Hello");
  ASSERT_NO_THROW(std::swap(o1,o2));
  ASSERT_THROW(o1.value(), std::logic_error);
  ASSERT_NO_THROW(o2.value());
  ASSERT_TRUE(o2.value() == "Hello");
}

TEST_F(cta_optionalTest, comparisons) {
  using namespace cta;

  optional<std::string> o1={};
  optional<std::string> o2("Hello");
  optional<std::string> o3("World");
  optional<std::string> o4("World");
  std::string s2("Hello");
  std::string s3("World");
  
  ASSERT_TRUE(o1 == nullopt);
  ASSERT_TRUE(nullopt == o1);
  ASSERT_TRUE(o2 == std::string("Hello"));
  ASSERT_TRUE(o3 == o4);
  ASSERT_TRUE(o3 <= o4);
  ASSERT_TRUE(o3 >= o4);
  ASSERT_FALSE(o3 > o4);
  ASSERT_FALSE(o3 < o4);
  ASSERT_FALSE(o3 != o4);
  ASSERT_TRUE(o3 == s3);
  ASSERT_TRUE(s3 == o3);
  ASSERT_TRUE(s2 < o3);
  ASSERT_TRUE(s3 > o2);
  ASSERT_TRUE(o2 > o1);
  ASSERT_TRUE(o2 > nullopt);
  ASSERT_FALSE(o1 > nullopt);
  ASSERT_TRUE(o1 >= nullopt);
}


TEST_F(cta_optionalTest, sort) {
  using namespace cta;
  
  std::vector<optional<std::string>> v;
  v.push_back(std::string("Hobgoblin"));
  v.push_back(std::string("Aquator"));
  v.push_back(std::string("Griffin"));
  v.push_back(std::string("Ice monster"));
  v.push_back(std::string("Centaur"));
  v.push_back(nullopt);
  v.push_back(std::string("Emu"));
  v.push_back(std::string("Venus Fly Trap"));
  v.push_back(std::string("Bat"));
  v.push_back(std::string("Dragon"));
  
  ASSERT_NO_THROW(std::sort(v.begin(), v.end()));
  ASSERT_TRUE(v.front()==nullopt);
  ASSERT_TRUE(v.back()==std::string("Venus Fly Trap"));
  
}

} // namespace unitTests


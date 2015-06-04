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

#include "scheduler/UserIdentity.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_UserIdentityTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_UserIdentityTest, equals_operator) {
  using namespace cta;

  const uint16_t uid1 = 1111;
  const uint16_t uid2 = 2222;
  const uint16_t gid1 = 3333;
  const uint16_t gid2 = 4444;

  const UserIdentity                        user(uid1, gid1);
  const UserIdentity            sameUserAndGroup(uid1, gid1);
  const UserIdentity     sameUserDifferrentGroup(uid1, gid2);
  const UserIdentity      differentUserSameGroup(uid2, gid1);
  const UserIdentity differentUserDifferentGroup(uid2, gid2);

  ASSERT_TRUE(user == user);
  ASSERT_TRUE(user == sameUserAndGroup);
  ASSERT_TRUE(user == sameUserDifferrentGroup);
  ASSERT_FALSE(user == differentUserSameGroup);
  ASSERT_FALSE(user == differentUserDifferentGroup);
}

TEST_F(cta_UserIdentityTest, not_equals_operator) {
  using namespace cta;
  
  const uint16_t uid1 = 1111;
  const uint16_t uid2 = 2222;
  const uint16_t gid1 = 3333;
  const uint16_t gid2 = 4444;

  const UserIdentity                        user(uid1, gid1);
  const UserIdentity            sameUserAndGroup(uid1, gid1);
  const UserIdentity     sameUserDifferrentGroup(uid1, gid2);
  const UserIdentity      differentUserSameGroup(uid2, gid1);
  const UserIdentity differentUserDifferentGroup(uid2, gid2);

  ASSERT_FALSE(user != user);
  ASSERT_FALSE(user != sameUserAndGroup);
  ASSERT_FALSE(user != sameUserDifferrentGroup);
  ASSERT_TRUE(user != differentUserSameGroup);
  ASSERT_TRUE(user != differentUserDifferentGroup);
}

} // namespace unitTests

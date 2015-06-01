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

#include <gtest/gtest.h>
#include "BackendVFS.hpp"
#include "common/exception/Exception.hpp"
#include "RootEntry.hpp"
#include "Agent.hpp"

namespace unitTests {

TEST(RootEntry, BasicAccess) {
  cta::objectstore::BackendVFS be;
  { 
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
  {
    // Try to read back and dump the root entry
    cta::objectstore::RootEntry re(be);
    ASSERT_THROW(re.fetch(), cta::exception::Exception);
    cta::objectstore::ScopedSharedLock lock(re);
    ASSERT_NO_THROW(re.fetch());
    re.dump();
  }
  {
    // Try to allocate the agent register
    // Try to read back and dump the root entry
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock (re);
    // Create an agent
    cta::objectstore::Agent agent(be);
    agent.generateName("unitTest");
    re.fetch();
    cta::objectstore::CreationLog cl(99, "dummyUser", 99, "dummyGroup", 
      "unittesthost", time(NULL), "Creation of unit test agent register");
    re.addOrGetAgentRegisterPointer(agent, cl);
    ASSERT_NO_THROW(re.getAgentRegisterPointer());
    re.commit();
    //agent.registerSelf();
  }
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.remove();
  ASSERT_EQ(false, re.exists());
}

TEST(RootEntry, AdminHosts) {
  cta::objectstore::BackendVFS be;
  { 
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
  {
    // Add 2 admin hosts to the root entry
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    ASSERT_NO_THROW(re.fetch());
    cta::objectstore::CreationLog cl(99, "dummyUser", 99, "dummyGroup", 
      "unittesthost", time(NULL), "Creation of unit test agent register");
    re.addAdminHost("adminHost1", cl);
    re.addAdminHost("adminHost2", cl);
    re.commit();
  }
  {
    // Check that the admin hosts made it
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedSharedLock lock(re);
    re.fetch();
    ASSERT_TRUE(re.isAdminHost("adminHost1"));
    ASSERT_TRUE(re.isAdminHost("adminHost2"));
    ASSERT_FALSE(re.isAdminHost("adminHost3"));
  }
  {
    // Check that we can remove existing and non-existing hosts
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    re.removeAdminHost("adminHost1");
    re.removeAdminHost("noSuch");
    re.commit();
  }
  {
    // Check that we cannot re-add an existing host
    // Check that we can remove existing and non-existing hosts
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    cta::objectstore::CreationLog cl(99, "dummyUser", 99, "dummyGroup", 
      "unittesthost", time(NULL), "Creation of unit test agent register");
    ASSERT_THROW(re.addAdminHost("adminHost2", cl),
       cta::objectstore::RootEntry::DuplicateEntry);
  }
  {
    // Check that we got the expected result
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedSharedLock lock(re);
    re.fetch();
    ASSERT_FALSE(re.isAdminHost("adminHost1"));
    ASSERT_TRUE(re.isAdminHost("adminHost2"));
    ASSERT_FALSE(re.isAdminHost("adminHost3"));
    ASSERT_EQ(1, re.dumpAdminHosts().size());
    ASSERT_EQ("adminHost2", re.dumpAdminHosts().front().hostname);
  }
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.remove();
  ASSERT_EQ(false, re.exists());
}
  
TEST(RootEntry, AdminUsers) {
  cta::objectstore::BackendVFS be;
  { 
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
  cta::objectstore::UserIdentity user1(123, "user123", 456, "group456");
  cta::objectstore::UserIdentity user1prime(123, "somethingelse", 789, "group789");
  cta::objectstore::UserIdentity user2(234, "user234", 345, "group345");
  cta::objectstore::UserIdentity user2prime(234, "somethingwrong", 567, "group567");
  cta::objectstore::UserIdentity user3(345, "user234", 345, "group345");
  {
    // Add 2 admin users to the root entry
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    ASSERT_NO_THROW(re.fetch());
    cta::objectstore::CreationLog cl(99, "dummyUser", 99, "dummyGroup", 
      "unittesthost", time(NULL), "Creation of unit test agent register");
    re.addAdminUser(user1, cl);
    re.addAdminUser(user2, cl);
    re.commit();
  }
  {
    // Check that the admin hosts made it and that uid is the only meaningful
    // criteria
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedSharedLock lock(re);
    re.fetch();
    ASSERT_TRUE(re.isAdminUser(user1));
    ASSERT_TRUE(re.isAdminUser(user1prime));
    ASSERT_TRUE(re.isAdminUser(user2));
    ASSERT_TRUE(re.isAdminUser(user2prime));
    ASSERT_FALSE(re.isAdminUser(user3));

  }
  {
    // Check that we can remove existing and non-existing hosts
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    re.removeAdminUser(user1prime);
    re.removeAdminUser(user3);
    re.commit();
  }
  {
    // Check that we cannot re-add an existing user
    // Check that we can remove existing and non-existing hosts
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    cta::objectstore::CreationLog cl(99, "dummyUser", 99, "dummyGroup", 
      "unittesthost", time(NULL), "Creation of unit test agent register");
    ASSERT_THROW(re.addAdminUser(user2, cl),
       cta::objectstore::RootEntry::DuplicateEntry);
  }
  {
    // Check that we got the expected result
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedSharedLock lock(re);
    re.fetch();
    ASSERT_FALSE(re.isAdminUser(user1));
    ASSERT_FALSE(re.isAdminUser(user1prime));
    ASSERT_TRUE(re.isAdminUser(user2));
    ASSERT_TRUE(re.isAdminUser(user2prime));
    ASSERT_FALSE(re.isAdminUser(user3));
    ASSERT_EQ(1, re.dumpAdminUsers().size());
    ASSERT_EQ(user2.uid, re.dumpAdminUsers().front().user.uid);
  }
  
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.remove();
  ASSERT_EQ(false, re.exists());
}

}

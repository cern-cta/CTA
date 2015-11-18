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
#include "objectstore/SerializersExceptions.hpp"
#include "RootEntry.hpp"
#include "Agent.hpp"
#include "AgentRegister.hpp"
#include "TapePool.hpp"

namespace unitTests {

TEST(ObjectStore, RootEntryBasicAccess) {
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
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
      "unittesthost", time(NULL), "Creation of unit test agent register");
    re.addOrGetAgentRegisterPointerAndCommit(agent, cl);
    ASSERT_NO_THROW(re.getAgentRegisterAddress());
    re.commit();
    //agent.registerSelf();
  }
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeAgentRegisterAndCommit();
  re.removeIfEmpty();
  ASSERT_EQ(false, re.exists());
}

TEST(ObjectStore, RootEntryAdminHosts) {
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
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
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
    ASSERT_THROW(re.removeAdminHost("noSuch"), cta::objectstore::RootEntry::NoSuchAdminHost);
    re.commit();
  }
  {
    // Check that we cannot re-add an existing host
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
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
  re.fetch();
  re.removeIfEmpty();
  ASSERT_EQ(false, re.exists());
}
  
TEST(ObjectStore, RootEntryAdminUsers) {
  cta::objectstore::BackendVFS be;
  { 
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
  cta::objectstore::UserIdentity user1(123, 456);
  cta::objectstore::UserIdentity user1prime(123, 789);
  cta::objectstore::UserIdentity user2(234, 345);
  cta::objectstore::UserIdentity user2prime(234, 56);
  cta::objectstore::UserIdentity user3(345, 345);
  {
    // Add 2 admin users to the root entry
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    ASSERT_NO_THROW(re.fetch());
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
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
    ASSERT_THROW(re.removeAdminUser(user3), cta::objectstore::RootEntry::NoSuchAdminUser);
    re.commit();
  }
  {
    // Check that we cannot re-add an existing user
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
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
  re.fetch();
  re.removeIfEmpty();
  ASSERT_EQ(false, re.exists());
}

TEST(ObjectStore, RootEntryStorageClassesAndArchiveRoutes) {
  cta::objectstore::BackendVFS be;
  { 
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
      "unittesthost", time(NULL), "Creation of unit test agent register");
  {
    // Add 2 storage classes to the root entry
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    ASSERT_NO_THROW(re.fetch());
    re.addStorageClass("class1", 1, cl);
    re.addStorageClass("class2", 2, cl);
    re.addStorageClass("class3", 3, cl);
    // Check that we cannot add a storage class with 0 copies
    ASSERT_THROW(re.addStorageClass("class0", 0, cl),
      cta::objectstore::RootEntry::InvalidCopyNumber);
    re.commit();
  }
  {
    // Check that the storage classes made it, and that non existing storage
    // classes show up
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedSharedLock lock(re);
    re.fetch();
    ASSERT_NO_THROW(re.getStorageClassCopyCount("class1"));
    ASSERT_NO_THROW(re.getStorageClassCopyCount("class2"));
    ASSERT_NO_THROW(re.getStorageClassCopyCount("class3"));
    ASSERT_THROW(re.getStorageClassCopyCount("class4"),
      cta::objectstore::serializers::NotFound);
  }
  {
    // Check that we can remove storage class and non-existing ones
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    ASSERT_NO_THROW(re.removeStorageClass("class2"));
    ASSERT_THROW(re.removeStorageClass("class4"), cta::objectstore::RootEntry::NoSuchStorageClass);
    re.commit();
  }
  {
    // Check that we cannot re-add an existing storage class
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    ASSERT_THROW(re.addStorageClass("class1", 1, cl),
       cta::objectstore::RootEntry::DuplicateEntry);
  }
  {
    // Check that we got the expected result
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedSharedLock lock(re);
    re.fetch();
    ASSERT_NO_THROW(re.getStorageClassCopyCount("class1"));
    ASSERT_THROW(re.getStorageClassCopyCount("class2"),
      cta::objectstore::serializers::NotFound);
    ASSERT_NO_THROW(re.getStorageClassCopyCount("class3"));
    ASSERT_THROW(re.getStorageClassCopyCount("class4"),
      cta::objectstore::serializers::NotFound);
    ASSERT_EQ(2, re.dumpStorageClasses().size());
    ASSERT_EQ("class1", re.dumpStorageClasses().front().storageClass);
    ASSERT_EQ("class3", re.dumpStorageClasses().back().storageClass);
  }
  {
    // Check that we can add storage routes
    // We store an incomplete route for class3
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    ASSERT_NO_THROW(re.addArchiveRoute("class1", 1, "pool1_0", cl));
    ASSERT_NO_THROW(re.addArchiveRoute("class3", 3, "pool3_2", cl));
    re.commit();
  }
  {
    // Check that we can get routes, but not if the routing is incomplete for
    // the storage class
    // Check that we can add storage routes
    // We store an incomplete route for class3
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedSharedLock lock(re);
    re.fetch();
    ASSERT_NO_THROW(re.getArchiveRoutes("class1"));
    ASSERT_THROW(re.getArchiveRoutes("class3"),
      cta::objectstore::RootEntry::IncompleteEntry);
  }
  {
    //Check that we can complete a storage class, and cannot overwite the routes
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    ASSERT_NO_THROW(re.addArchiveRoute("class3", 1, "pool3_0", cl));
    ASSERT_NO_THROW(re.addArchiveRoute("class3", 2, "pool3_1", cl));
    ASSERT_THROW(re.addArchiveRoute("class3", 3, "pool3_2b", cl), cta::objectstore::RootEntry::ArchiveRouteAlreadyExists);
    re.commit();
    re.fetch();
    ASSERT_NO_THROW(re.getArchiveRoutes("class3"));
  }
  {
    // Check that resizing storage classes down and up work as expected
    //Check that we can complete a storage class, and overwrite the routes
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    ASSERT_NO_THROW(re.setStorageClassCopyCount("class1", 2, cl));
    ASSERT_NO_THROW(re.setStorageClassCopyCount("class3", 2, cl));
    re.commit();
    re.fetch();
    ASSERT_THROW(re.getArchiveRoutes("class1"),
      cta::objectstore::RootEntry::IncompleteEntry);
    ASSERT_NO_THROW(re.getArchiveRoutes("class3"));
    ASSERT_EQ(2,re.getArchiveRoutes("class3").size());
  }
  {
    // Check that dump works in all circumstances
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedSharedLock lock(re);
    re.fetch();
    ASSERT_NO_THROW(
    {
      re.dumpAdminUsers();
      re.dumpAdminHosts();
      re.dumpStorageClasses();
      re.dumpLibraries();
      //re.dumpTapePool();
    });
  }
  {
    // remove the remaining storage classes
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    re.removeArchiveRoute("class1", 1);
    re.removeArchiveRoute("class3", 1);
    re.removeArchiveRoute("class3", 2);
    re.removeStorageClass("class1");
    re.removeStorageClass("class3");
    re.commit();
  }
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeIfEmpty();
  ASSERT_EQ(false, re.exists());
}

TEST(ObjectStore, RootEntryibraries) {
  cta::objectstore::BackendVFS be;
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
      "unittesthost", time(NULL), "Creation of unit test agent register");
  { 
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
  {
    // Add 2 libraries to the root entry
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    ASSERT_NO_THROW(re.fetch());
    re.addLibrary("library1", cl);
    re.addLibrary("library2", cl);
    re.commit();
  }
  {
    // Check that the admin hosts made it
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedSharedLock lock(re);
    re.fetch();
    ASSERT_TRUE(re.libraryExists("library1"));
    ASSERT_TRUE(re.libraryExists("library2"));
    ASSERT_FALSE(re.libraryExists("library3"));
  }
  {
    // Check that we can remove existing and non-existing hosts
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    re.removeLibrary("library1");
    ASSERT_THROW(re.removeAdminHost("library3"), cta::objectstore::RootEntry::NoSuchAdminHost);
    re.commit();
  }
  {
    // Check that we cannot re-add an existing host
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    ASSERT_THROW(re.addLibrary("library2", cl),
       cta::objectstore::RootEntry::DuplicateEntry);
  }
  {
    // Check that we got the expected result
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedSharedLock lock(re);
    re.fetch();
    ASSERT_FALSE(re.libraryExists("library1"));
    ASSERT_TRUE(re.libraryExists("library2"));
    ASSERT_FALSE(re.libraryExists("library3"));
    ASSERT_EQ(1, re.dumpLibraries().size());
    ASSERT_EQ("library2", re.dumpLibraries().front().library);
  }
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeIfEmpty();
  ASSERT_EQ(false, re.exists());
}

TEST (ObjectStore, RootEntryTapePools) {
  cta::objectstore::BackendVFS be;
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
      "unittesthost", time(NULL), "Creation of unit test agent register");
  cta::objectstore::Agent ag(be);
  ag.initialize();
  ag.generateName("UnitTests");
  { 
    // Try to create the root entry and allocate the agent register
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.addOrGetAgentRegisterPointerAndCommit(ag, cl);
  }
  ag.insertAndRegisterSelf();
  std::string tpAddr1, tpAddr2;
  {
    // Create the tape pools
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    ASSERT_THROW(re.getTapePoolAddress("tapePool1"),
      cta::objectstore::RootEntry::NotAllocated);
    tpAddr1 = re.addOrGetTapePoolAndCommit("tapePool1", 100, 5, 5, ag, cl);
    // Check that we car read it
    cta::objectstore::TapePool tp(tpAddr1, be);
    cta::objectstore::ScopedSharedLock tpl(tp);
    ASSERT_NO_THROW(tp.fetch());
  }
  {
    // Create another pool
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    tpAddr2 = re.addOrGetTapePoolAndCommit("tapePool2", 100, 5, 5,ag, cl);
    ASSERT_TRUE(be.exists(tpAddr2));
  }
  {
    // Remove the other pool
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    re.removeTapePoolAndCommit("tapePool2");
    ASSERT_FALSE(be.exists(tpAddr2));
  }
  // Unregister the agent
  cta::objectstore::ScopedExclusiveLock agl(ag);
  ag.removeAndUnregisterSelf();
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeAgentRegisterAndCommit();
  re.removeTapePoolAndCommit("tapePool1");
  ASSERT_FALSE(be.exists(tpAddr1));
  re.removeIfEmpty();
  ASSERT_EQ(false, re.exists());
}

TEST (ObjectStore, RootEntryDriveRegister) {
  cta::objectstore::BackendVFS be;
  { 
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
      "unittesthost", time(NULL), "Creation of unit test agent register");
  cta::objectstore::Agent ag(be);
  ag.initialize();
  ag.generateName("UnitTests");
  { 
    // Try to create the root entry and allocate the agent register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    re.addOrGetAgentRegisterPointerAndCommit(ag, cl);
  }
  ag.insertAndRegisterSelf();
  std::string driveRegisterAddress;
  {
    // create the drive register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    ASSERT_THROW(re.getDriveRegisterAddress(),
      cta::objectstore::RootEntry::NotAllocated);
    ASSERT_NO_THROW(
      driveRegisterAddress = re.addOrGetDriveRegisterPointerAndCommit(ag, cl));
    ASSERT_TRUE(be.exists(driveRegisterAddress));
  }
  {
    // delete the drive register
    // create the drive register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    re.removeDriveRegisterAndCommit();
    ASSERT_FALSE(be.exists(driveRegisterAddress));
  }
  // Unregister the agent
  cta::objectstore::ScopedExclusiveLock agl(ag);
  ag.removeAndUnregisterSelf();
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeAgentRegisterAndCommit();
  re.removeIfEmpty();
  ASSERT_EQ(false, re.exists());
}

TEST(ObjectStore, RootEntryAgentRegister) {
  cta::objectstore::BackendVFS be;
  { 
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
      "unittesthost", time(NULL), "Creation of unit test agent register");
  cta::objectstore::Agent ag(be);
  ag.generateName("UnitTests");
  std::string arAddr;
  {
    // Create the agent register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    ASSERT_THROW(re.getAgentRegisterAddress(),
      cta::objectstore::RootEntry::NotAllocated);
    arAddr = re.addOrGetAgentRegisterPointerAndCommit(ag, cl);
    // Check that we car read it
    cta::objectstore::AgentRegister ar(arAddr, be);
    cta::objectstore::ScopedSharedLock arl(ar);
    ASSERT_NO_THROW(ar.fetch());
  }
  {
    // Delete the agent register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    // Check that we still get the same agent register
    ASSERT_EQ(arAddr, re.getAgentRegisterAddress());
    ASSERT_EQ(arAddr, re.addOrGetAgentRegisterPointerAndCommit(ag, cl));
    // Remove it
    ASSERT_NO_THROW(re.removeAgentRegisterAndCommit());
    // Check that the object is gone
    ASSERT_EQ(false, be.exists(arAddr));
  }
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeIfEmpty();
  ASSERT_EQ(false, re.exists());
}

TEST (ObjectStore, RootEntrySchedulerGlobalLock) {
  cta::objectstore::BackendVFS be;
  { 
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
      "unittesthost", time(NULL), "Creation of unit test scheduler global lock");
  cta::objectstore::Agent ag(be);
  ag.initialize();
  ag.generateName("UnitTests");
  { 
    // Try to create the root entry and allocate the agent register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    re.addOrGetAgentRegisterPointerAndCommit(ag, cl);
  }
  ag.insertAndRegisterSelf();
  std::string schedulerGlobalLockAddress;
  {
    // create the drive register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    ASSERT_THROW(re.getDriveRegisterAddress(),
      cta::objectstore::RootEntry::NotAllocated);
    ASSERT_NO_THROW(
      schedulerGlobalLockAddress = re.addOrGetSchedulerGlobalLockAndCommit(ag, cl));
    ASSERT_TRUE(be.exists(schedulerGlobalLockAddress));
  }
  {
    // delete the drive register
    // create the drive register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    re.removeSchedulerGlobalLockAndCommit();
    ASSERT_FALSE(be.exists(schedulerGlobalLockAddress));
  }
  // Unregister the agent
  cta::objectstore::ScopedExclusiveLock agl(ag);
  ag.removeAndUnregisterSelf();
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeAgentRegisterAndCommit();
  re.removeIfEmpty();
  ASSERT_EQ(false, re.exists());
}

}

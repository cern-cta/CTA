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

#include "MiddleTierAdminAbstractTest.hpp"
#include "cta/SqliteMiddleTierAdmin.hpp"
#include "cta/SqliteMiddleTierUser.hpp"
#include <memory>

#include <gtest/gtest.h>
namespace unitTests {

TEST_P(MiddleTierAdminAbstractTest, createStorageClass_new) {
  using namespace cta;

  const SecurityIdentity requester;
  std::auto_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, name, nbCopies,
    comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }
}

TEST_P(MiddleTierAdminAbstractTest,
  createStorageClass_already_existing) {
  using namespace cta;

  const SecurityIdentity requester;
  std::auto_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  
  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, name, nbCopies, comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }
  
  ASSERT_THROW(m_middleTier->admin().createStorageClass(requester, name, nbCopies, comment),
    std::exception);
}

TEST_P(MiddleTierAdminAbstractTest,
  createStorageClass_lexicographical_order) {
  using namespace cta;

  const SecurityIdentity requester;
  std::auto_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  
  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, "d", 1, "Comment d"));
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, "b", 1, "Comment b"));
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, "a", 1, "Comment a"));
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, "c", 1, "Comment c"));
  
  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_EQ(4, storageClasses.size());

    ASSERT_EQ(std::string("a"), storageClasses.front().getName());
    storageClasses.pop_front();
    ASSERT_EQ(std::string("b"), storageClasses.front().getName());
    storageClasses.pop_front();
    ASSERT_EQ(std::string("c"), storageClasses.front().getName());
    storageClasses.pop_front();
    ASSERT_EQ(std::string("d"), storageClasses.front().getName());
  }
}

TEST_P(MiddleTierAdminAbstractTest, deleteStorageClass_existing) {
  using namespace cta;

  const SecurityIdentity requester;
  std::auto_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  
  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, name, nbCopies, comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());
  
    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());

    ASSERT_NO_THROW(m_middleTier->admin().deleteStorageClass(requester, name));
  }

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_P(MiddleTierAdminAbstractTest,
  deleteStorageClass_in_use_by_directory) {
  using namespace cta;

  const SecurityIdentity requester;
  std::auto_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  
  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName, nbCopies,
    comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(storageClassName, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }

  ASSERT_NO_THROW(m_middleTier->user().setDirStorageClass(requester, "/",
    storageClassName));

  ASSERT_THROW(m_middleTier->admin().deleteStorageClass(requester, storageClassName),
    std::exception);

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(storageClassName, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }

  ASSERT_NO_THROW(m_middleTier->user().clearDirStorageClass(requester, "/"));

  ASSERT_NO_THROW(m_middleTier->admin().deleteStorageClass(requester, storageClassName));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_P(MiddleTierAdminAbstractTest, deleteStorageClass_in_use_by_route) {
  using namespace cta;

  const SecurityIdentity requester;
  std::auto_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  
  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName, nbCopies,
    comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(storageClassName, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(m_middleTier->admin().createTapePool(requester, tapePoolName,
    nbPartialTapes, comment));

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = m_middleTier->admin().getTapePools(requester));
    ASSERT_EQ(1, tapePools.size());

    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(tapePoolName, tapePool.getName());
  }

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(m_middleTier->admin().createArchivalRoute(requester, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = m_middleTier->admin().getArchivalRoutes(requester));
    ASSERT_EQ(1, archivalRoutes.size());

    ArchivalRoute archivalRoute;
    ASSERT_NO_THROW(archivalRoute = archivalRoutes.front());
    ASSERT_EQ(storageClassName, archivalRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archivalRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archivalRoute.getTapePoolName());
  }

  ASSERT_THROW(m_middleTier->admin().deleteStorageClass(requester, storageClassName),
    std::exception);

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(storageClassName, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }

  ASSERT_NO_THROW(m_middleTier->admin().deleteArchivalRoute(requester, storageClassName,
    copyNb));

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = m_middleTier->admin().getArchivalRoutes(requester));
    ASSERT_TRUE(archivalRoutes.empty());
  }

  ASSERT_NO_THROW(m_middleTier->admin().deleteStorageClass(requester, storageClassName));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_P(MiddleTierAdminAbstractTest, deleteStorageClass_non_existing) {
  using namespace cta;

  const SecurityIdentity requester;
  std::auto_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  
  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  ASSERT_THROW(m_middleTier->admin().deleteStorageClass(requester, name), std::exception);

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = m_middleTier->admin().getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_P(MiddleTierAdminAbstractTest, deleteTapePool_in_use) {
  using namespace cta;

  const SecurityIdentity requester;
  std::auto_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = m_middleTier->admin().getArchivalRoutes(requester));
    ASSERT_TRUE(archivalRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(m_middleTier->admin().createTapePool(requester, tapePoolName,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(m_middleTier->admin().createArchivalRoute(requester, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = m_middleTier->admin().getArchivalRoutes(requester));
    ASSERT_EQ(1, archivalRoutes.size());

    ArchivalRoute archivalRoute;
    ASSERT_NO_THROW(archivalRoute = archivalRoutes.front());
    ASSERT_EQ(storageClassName, archivalRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archivalRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archivalRoute.getTapePoolName());
  }

  ASSERT_THROW(m_middleTier->admin().deleteTapePool(requester, tapePoolName), std::exception);

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = m_middleTier->admin().getArchivalRoutes(requester));
    ASSERT_EQ(1, archivalRoutes.size());

    ArchivalRoute archivalRoute;
    ASSERT_NO_THROW(archivalRoute = archivalRoutes.front());
    ASSERT_EQ(storageClassName, archivalRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archivalRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archivalRoute.getTapePoolName());
  }
}

TEST_P(MiddleTierAdminAbstractTest, createArchivalRoute_new) {
  using namespace cta;

  const SecurityIdentity requester;
  std::auto_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = m_middleTier->admin().getArchivalRoutes(requester));
    ASSERT_TRUE(archivalRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(m_middleTier->admin().createTapePool(requester, tapePoolName,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(m_middleTier->admin().createArchivalRoute(requester, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = m_middleTier->admin().getArchivalRoutes(requester));
    ASSERT_EQ(1, archivalRoutes.size());

    ArchivalRoute archivalRoute;
    ASSERT_NO_THROW(archivalRoute = archivalRoutes.front());
    ASSERT_EQ(storageClassName, archivalRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archivalRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archivalRoute.getTapePoolName());
  }
}

TEST_P(MiddleTierAdminAbstractTest,
  createArchivalRoute_already_existing) {
  using namespace cta;

  const SecurityIdentity requester;
  std::auto_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = m_middleTier->admin().getArchivalRoutes(requester));
    ASSERT_TRUE(archivalRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(m_middleTier->admin().createTapePool(requester, tapePoolName,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(m_middleTier->admin().createArchivalRoute(requester, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = m_middleTier->admin().getArchivalRoutes(requester));
    ASSERT_EQ(1, archivalRoutes.size());

    ArchivalRoute archivalRoute;
    ASSERT_NO_THROW(archivalRoute = archivalRoutes.front());
    ASSERT_EQ(storageClassName, archivalRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archivalRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archivalRoute.getTapePoolName());
  }

  ASSERT_THROW(m_middleTier->admin().createArchivalRoute(requester, storageClassName,
    copyNb, tapePoolName, comment), std::exception);
}

TEST_P(MiddleTierAdminAbstractTest, deleteArchivalRoute_existing) {
  using namespace cta;

  const SecurityIdentity requester;
  std::auto_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = m_middleTier->admin().getArchivalRoutes(requester));
    ASSERT_TRUE(archivalRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(m_middleTier->admin().createTapePool(requester, tapePoolName,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(m_middleTier->admin().createArchivalRoute(requester, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = m_middleTier->admin().getArchivalRoutes(requester));
    ASSERT_EQ(1, archivalRoutes.size());

    ArchivalRoute archivalRoute;
    ASSERT_NO_THROW(archivalRoute = archivalRoutes.front());
    ASSERT_EQ(storageClassName, archivalRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archivalRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archivalRoute.getTapePoolName());
  }

  ASSERT_NO_THROW(m_middleTier->admin().deleteArchivalRoute(requester, storageClassName,
    copyNb));

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = m_middleTier->admin().getArchivalRoutes(requester));
    ASSERT_TRUE(archivalRoutes.empty());
  }
}

TEST_P(MiddleTierAdminAbstractTest, deleteArchivalRoute_non_existing) {
  using namespace cta;

  const SecurityIdentity requester;
  std::auto_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = m_middleTier->admin().getArchivalRoutes(requester));
    ASSERT_TRUE(archivalRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(m_middleTier->admin().createTapePool(requester, tapePoolName,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_THROW(m_middleTier->admin().deleteArchivalRoute(requester, tapePoolName, copyNb),
    std::exception);
}

TEST_P(MiddleTierAdminAbstractTest, createTape_new) {
  using namespace cta;

  const SecurityIdentity requester;
  std::auto_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = m_middleTier->admin().getLogicalLibraries(requester));
    ASSERT_TRUE(libraries.empty());
  }

  {
    std::list<TapePool> pools;
    ASSERT_NO_THROW(pools = m_middleTier->admin().getTapePools(requester));
    ASSERT_TRUE(pools.empty());
  }

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = m_middleTier->admin().getTapes(requester));
    ASSERT_TRUE(tapes.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";
  const std::string libraryComment = "Library comment";
  ASSERT_NO_THROW(m_middleTier->admin().createLogicalLibrary(requester, libraryName,
    libraryComment));
  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = m_middleTier->admin().getLogicalLibraries(requester));
    ASSERT_EQ(1, libraries.size());
  
    LogicalLibrary logicalLibrary;
    ASSERT_NO_THROW(logicalLibrary = libraries.front());
    ASSERT_EQ(libraryName, logicalLibrary.getName());
    ASSERT_EQ(libraryComment, logicalLibrary.getComment());
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string comment = "Tape pool omment";
  ASSERT_NO_THROW(m_middleTier->admin().createTapePool(requester, tapePoolName,
    nbPartialTapes, comment));
  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = m_middleTier->admin().getTapePools(requester));
    ASSERT_EQ(1, tapePools.size());
    
    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(tapePoolName, tapePool.getName());
    ASSERT_EQ(comment, tapePool.getComment());
  } 

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  ASSERT_NO_THROW(m_middleTier->admin().createTape(requester, vid, libraryName, tapePoolName,
    capacityInBytes, tapeComment));
  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = m_middleTier->admin().getTapes(requester));
    ASSERT_EQ(1, tapes.size()); 
  
    Tape tape;
    ASSERT_NO_THROW(tape = tapes.front());
    ASSERT_EQ(vid, tape.getVid());
    ASSERT_EQ(libraryName, tape.getLogicalLibraryName());
    ASSERT_EQ(tapePoolName, tape.getTapePoolName());
    ASSERT_EQ(capacityInBytes, tape.getCapacityInBytes());
    ASSERT_EQ(0, tape.getDataOnTapeInBytes());
    ASSERT_EQ(tapeComment, tape.getComment());
  } 
}

TEST_P(MiddleTierAdminAbstractTest,
  createTape_new_non_existing_library) {
  using namespace cta;

  const SecurityIdentity requester;
  std::auto_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = m_middleTier->admin().getLogicalLibraries(requester));
    ASSERT_TRUE(libraries.empty());
  }

  {
    std::list<TapePool> pools;
    ASSERT_NO_THROW(pools = m_middleTier->admin().getTapePools(requester));
    ASSERT_TRUE(pools.empty());
  }

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = m_middleTier->admin().getTapes(requester));
    ASSERT_TRUE(tapes.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string comment = "Tape pool omment";
  ASSERT_NO_THROW(m_middleTier->admin().createTapePool(requester, tapePoolName,
    nbPartialTapes, comment));
  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = m_middleTier->admin().getTapePools(requester));
    ASSERT_EQ(1, tapePools.size());
    
    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(tapePoolName, tapePool.getName());
    ASSERT_EQ(comment, tapePool.getComment());
  } 

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  ASSERT_THROW(m_middleTier->admin().createTape(requester, vid, libraryName, tapePoolName,
    capacityInBytes, tapeComment), std::exception);
}

TEST_P(MiddleTierAdminAbstractTest, createTape_new_non_existing_pool) {
  using namespace cta;

  const SecurityIdentity requester;
  std::auto_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = m_middleTier->admin().getLogicalLibraries(requester));
    ASSERT_TRUE(libraries.empty());
  }

  {
    std::list<TapePool> pools;
    ASSERT_NO_THROW(pools = m_middleTier->admin().getTapePools(requester));
    ASSERT_TRUE(pools.empty());
  }

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = m_middleTier->admin().getTapes(requester));
    ASSERT_TRUE(tapes.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";
  const std::string libraryComment = "Library comment";
  ASSERT_NO_THROW(m_middleTier->admin().createLogicalLibrary(requester, libraryName,
    libraryComment));
  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = m_middleTier->admin().getLogicalLibraries(requester));
    ASSERT_EQ(1, libraries.size());
  
    LogicalLibrary logicalLibrary;
    ASSERT_NO_THROW(logicalLibrary = libraries.front());
    ASSERT_EQ(libraryName, logicalLibrary.getName());
    ASSERT_EQ(libraryComment, logicalLibrary.getComment());
  }

  const std::string tapePoolName = "TestTapePool";

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  ASSERT_THROW(m_middleTier->admin().createTape(requester, vid, libraryName, tapePoolName,
    capacityInBytes, tapeComment), std::exception);
}

} // namespace unitTests

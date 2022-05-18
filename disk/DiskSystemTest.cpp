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
#include <gtest/gtest.h>

#include "common/utils/Regex.hpp"
#include "common/log/DummyLogger.hpp"
#include "catalogue/Catalogue.hpp"
#include "catalogue/InMemoryCatalogue.hpp"

namespace unitTests {
  
  class DiskSystemTest: public ::testing::Test {
    public:
      DiskSystemTest(): m_dummyLog("dummy", "dummy") {}
      virtual ~DiskSystemTest() {}
      
      class FailedToGetCatalogue: public std::exception {
      public:
        const char *what() const throw() {
          return "Failed to get catalogue";
        }
      };
      
      virtual void SetUp() {
        const uint64_t nbConns = 1;
        const uint64_t nbArchiveFileListingConns = 1;
        //m_catalogue = std::make_unique<catalogue::SchemaCreatingSqliteCatalogue>(m_tempSqliteFile.path(), nbConns);
        m_catalogue = std::make_unique<cta::catalogue::InMemoryCatalogue>(m_dummyLog, nbConns, nbArchiveFileListingConns);
        m_cliId.host = "host";
        m_cliId.username = "userName";
        insertDiskSystemSpinner();
        insertDiskSystemDefault();
      }
    
      virtual void TearDown() {
        m_catalogue.reset();
      }
      
      cta::catalogue::Catalogue &getCatalogue() {
        cta::catalogue::Catalogue *const ptr = m_catalogue.get();
        if(nullptr == ptr) {
          throw FailedToGetCatalogue();
        }
        return *ptr;
      }
    
    protected:
      cta::log::DummyLogger m_dummyLog;
      std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
      cta::common::dataStructures::SecurityIdentity m_cliId;
      cta::disk::DiskSystem m_diskSystemSpinner;
      cta::disk::DiskSystem m_diskSystemDefault;
      
    private:
      void insertDiskSystemSpinner(){
        auto & catalogue = getCatalogue();
        
        // create disk instance
        std::string diskInstanceName = "DiskInstanceNameSpinner";
        std::string diskInstanceComment = "Comment";
        catalogue.createDiskInstance(m_cliId, diskInstanceName, diskInstanceComment);
        // create disk instance space
        std::string diskInstanceSpaceName = "DiskInstanceSpaceSpinner";
        std::string freeSpaceQueryURL = "eos:ctaeos:spinners";
        uint64_t refrestInterval = 1; 
        std::string diskInstanceSpaceComment = "Comment";
        catalogue.createDiskInstanceSpace(m_cliId, diskInstanceSpaceName, diskInstanceName, freeSpaceQueryURL, refrestInterval, diskInstanceSpaceComment);

        m_diskSystemSpinner.name = "DiskSystemNameSpinner";
        m_diskSystemSpinner.fileRegexp = "root://ctaeos.archiveretrieve-1215709git0e38ccd0-xi98.svc.cluster.local//eos/ctaeos/cta(.*)eos.space=spinners";
        m_diskSystemSpinner.targetedFreeSpace = 1;
        m_diskSystemSpinner.sleepTime = 1;
        m_diskSystemSpinner.comment = "Comment";

        catalogue.createDiskSystem(m_cliId,m_diskSystemSpinner.name, diskInstanceName, diskInstanceSpaceName, m_diskSystemSpinner.fileRegexp,
          m_diskSystemSpinner.targetedFreeSpace, m_diskSystemSpinner.sleepTime,m_diskSystemSpinner.comment);

      }
      
      void insertDiskSystemDefault() {
        auto & catalogue = getCatalogue();
        // create disk instance
        std::string diskInstanceName = "ctaeos";
        std::string diskInstanceComment = "Comment";
        catalogue.createDiskInstance(m_cliId, diskInstanceName, diskInstanceComment);
        // create disk instance space
        std::string diskInstanceSpaceName = "DiskInstanceSpaceDefault";
        std::string freeSpaceQueryURL = "eosSpace:default";
        uint64_t refrestInterval = 1; 
        std::string diskInstanceSpaceComment = "Comment";
        catalogue.createDiskInstanceSpace(m_cliId, diskInstanceSpaceName, diskInstanceName, freeSpaceQueryURL, refrestInterval, diskInstanceSpaceComment);
        // create disk system
        m_diskSystemDefault.name = "DiskSystemNameDefault";
        m_diskSystemDefault.fileRegexp = "root://ctaeos.archiveretrieve-1215709git0e38ccd0-xi98.svc.cluster.local//eos/ctaeos/cta(.*)eos.space=default";
        m_diskSystemDefault.targetedFreeSpace = 1;
        m_diskSystemDefault.sleepTime = 1;
        m_diskSystemDefault.comment = "Comment";

        catalogue.createDiskSystem(m_cliId,m_diskSystemDefault.name, diskInstanceName, diskInstanceSpaceName, m_diskSystemDefault.fileRegexp,
          m_diskSystemDefault.targetedFreeSpace, m_diskSystemDefault.sleepTime,m_diskSystemDefault.comment);

      }
  };
  
  TEST_F(DiskSystemTest, getDSNameWithFileDstURLFromEOS){
    
    auto & catalogue = getCatalogue();
    
    auto allDiskSystem = catalogue.getAllDiskSystems();
    
    std::string dstURL = "root://ctaeos.archiveretrieve-1215709git0e38ccd0-xi98.svc.cluster.local//eos/ctaeos/cta/54065a67-a3ea-4a44-b213-6f6a6f4e2cf4?eos.lfn=fxid:7&eos.ruid=0&eos.rgid=0&eos.injection=1&eos.workflow=retrieve_written&eos.space=spinners&toto=5";
    
    ASSERT_EQ(m_diskSystemSpinner.name,allDiskSystem.getDSName(dstURL));
    
    dstURL = "root://ctaeos.archiveretrieve-1215709git0e38ccd0-xi98.svc.cluster.local//eos/ctaeos/cta/54065a67-a3ea-4a44-b213-6f6a6f4e2cf4?eos.lfn=fxid:7&eos.ruid=0&eos.rgid=0&eos.injection=1&eos.workflow=retrieve_written&eos.space=default";
    
    ASSERT_EQ(m_diskSystemDefault.name,allDiskSystem.getDSName(dstURL));
    
    dstURL = "root://ctaeos.archiveretrieve-1215709git0e38ccd0-xi98.svc.cluster.local//eos/ctaeos/cta/54065a67-a3ea-4a44-b213-6f6a6f4e2cf4?eos.lfn=fxid:7&eos.ruid=0&eos.rgid=0&eos.injection=1&eos.workflow=retrieve_written";
    
    ASSERT_THROW(allDiskSystem.getDSName(dstURL),std::out_of_range);
  }
  
  TEST_F(DiskSystemTest, fetchDiskSystemFreeSpace) {
    cta::log::LogContext lc(m_dummyLog);
    
    auto & catalogue = getCatalogue();
    // create disk instance
    std::string diskInstanceName = "DiskInstanceNameConstantFreeSpace";
    std::string diskInstanceComment = "Comment";
    catalogue.createDiskInstance(m_cliId, diskInstanceName, diskInstanceComment);
    // create disk instance space
    std::string diskInstanceSpaceName = "DiskInstanceSpaceConstantFreeSpace";
    std::string freeSpaceQueryURL = "constantFreeSpace:200";
    uint64_t refrestInterval = 1; 
    std::string diskInstanceSpaceComment = "Comment";
    catalogue.createDiskInstanceSpace(m_cliId, diskInstanceSpaceName, diskInstanceName, freeSpaceQueryURL, refrestInterval, diskInstanceSpaceComment);
    // create disk system
    cta::disk::DiskSystem constantFreeSpaceDiskSystem;

    constantFreeSpaceDiskSystem.name = "ConstantFreeSpaceDiskSystem";
    constantFreeSpaceDiskSystem.fileRegexp = "/home/test/buffer";
    constantFreeSpaceDiskSystem.targetedFreeSpace = 1;
    constantFreeSpaceDiskSystem.sleepTime = 1;
    constantFreeSpaceDiskSystem.comment = "Comment";
    
    catalogue.createDiskSystem(m_cliId,constantFreeSpaceDiskSystem.name, diskInstanceName, diskInstanceSpaceName, constantFreeSpaceDiskSystem.fileRegexp,
      constantFreeSpaceDiskSystem.targetedFreeSpace, constantFreeSpaceDiskSystem.sleepTime,constantFreeSpaceDiskSystem.comment);

    auto allDiskSystem = catalogue.getAllDiskSystems();
    
    cta::disk::DiskSystemFreeSpaceList diskSystemFreeSpaceList (allDiskSystem);
    std::set<std::string> diskSystemsToFetch {"DiskSystemNotExists"};
    ASSERT_THROW(diskSystemFreeSpaceList.fetchDiskSystemFreeSpace(diskSystemsToFetch,catalogue, lc),std::exception);
    
    diskSystemsToFetch.clear();
    diskSystemsToFetch.insert(m_diskSystemSpinner.name);
    diskSystemsToFetch.insert(m_diskSystemDefault.name);
    
    try {
      diskSystemFreeSpaceList.fetchDiskSystemFreeSpace(diskSystemsToFetch, catalogue, lc);
      ASSERT_TRUE(false);
    } catch (const cta::disk::DiskSystemFreeSpaceListException& ex) {
      ASSERT_EQ(2,ex.m_failedDiskSystems.size());
    }

    diskSystemsToFetch.insert(constantFreeSpaceDiskSystem.name);
    try {
      diskSystemFreeSpaceList.fetchDiskSystemFreeSpace(diskSystemsToFetch, catalogue, lc);
      ASSERT_TRUE(false);
    } catch (const cta::disk::DiskSystemFreeSpaceListException& ex) {
      ASSERT_EQ(2,ex.m_failedDiskSystems.size());
    }
    ASSERT_EQ(200,diskSystemFreeSpaceList.at(constantFreeSpaceDiskSystem.name).freeSpace);

  }
}

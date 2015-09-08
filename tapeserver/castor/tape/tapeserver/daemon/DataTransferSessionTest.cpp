/******************************************************************************
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

#define __STDC_CONSTANT_MACROS // For using stdint macros (stdint is included
// by inttypes.h, so we shoot first)
#include <stdint.h>
#include <inttypes.h>
#include <gtest/gtest.h>

#include "castor/legacymsg/RmcProxyDummy.hpp"
#include "castor/log/StringLogger.hpp"
#include "castor/mediachanger/MediaChangerFacade.hpp"
#include "castor/mediachanger/MmcProxyDummy.hpp"
#include "castor/messages/AcsProxyDummy.hpp"
#include "castor/messages/TapeserverProxyDummy.hpp"
#include "castor/server/ProcessCapDummy.hpp"
#include "castor/server/Threading.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/tapeserver/drive/FakeDrive.hpp"
#include "common/exception/Exception.hpp"
#include "common/Utils.hpp"
#include "Ctape.h"
#include "scheduler/Scheduler.hpp"
#include "smc_struct.h"
#include "nameserver/mockNS/MockNameServer.hpp"
#include "remotens/MockRemoteNS.hpp"
#include "scheduler/DummyScheduler.hpp"
#include "scheduler/mockDB/MockSchedulerDatabase.hpp"
#include "scheduler/MountType.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

using namespace castor::tape::tapeserver;
using namespace castor::tape::tapeserver::daemon;
namespace unitTest {

class castor_tape_tapeserver_daemon_DataTransferSessionTest: public
  ::testing::Test {
protected:

  void SetUp() {
    strncpy(m_tmpDir, "/tmp/DataTransferSessionTestXXXXXX", sizeof(m_tmpDir));
    if(!mkdtemp(m_tmpDir)) {
      const std::string errMsg = cta::Utils::errnoToString(errno);
      std::ostringstream msg;
      msg << "Failed to create directory with template"
        " /tmp/DataTransferSessionTestXXXXXX: " << errMsg;
      bzero(m_tmpDir, sizeof(m_tmpDir));
      throw cta::exception::Exception(msg.str());
    }

    struct stat statBuf;
    bzero(&statBuf, sizeof(statBuf));
    if(stat(m_tmpDir, &statBuf)) {
      const std::string errMsg = cta::Utils::errnoToString(errno);
      std::ostringstream msg;
      msg << "Failed to stat directory " << m_tmpDir << ": " << errMsg;
      throw cta::exception::Exception(msg.str());
    }

    std::ostringstream cmd;
    cmd << "touch " << m_tmpDir << "/hello";
    system(cmd.str().c_str());
  }

  void TearDown() {
    // If Setup() created a temporary directory
    if(m_tmpDir) {

      // Openn the directory
      std::unique_ptr<DIR, std::function<int(DIR*)>>
        dir(opendir(m_tmpDir), closedir);
      if(NULL == dir.get()) {
        const std::string errMsg = cta::Utils::errnoToString(errno);
        std::ostringstream msg;
        msg << "Failed to open directory " << m_tmpDir << ": " << errMsg;
        throw cta::exception::Exception(msg.str());
      }

      // Delete each of the files within the directory
      struct dirent *entry = NULL;
      while((entry = readdir(dir.get()))) {
        const std::string entryName(entry->d_name);
        if(entryName != "." && entryName != "..") {
          const std::string entryPath = std::string(m_tmpDir) + "/" + entryName;
          if(unlink(entryPath.c_str())) {
            const std::string errMsg = cta::Utils::errnoToString(errno);
            std::ostringstream msg;
            msg << "Failed to unlink " << entryPath;
            throw cta::exception::Exception(msg.str());
          }
        }
      }

      // Delete the now empty directory
      if(rmdir(m_tmpDir)) {
        const std::string errMsg = cta::Utils::errnoToString(errno);
        std::ostringstream msg;
        msg << "Failed to delete directory " << m_tmpDir << ": " << errMsg;
        throw cta::exception::Exception(msg.str());
      }
    }
  }

  /**
   * Temporary directory created with mkdtemp that will be used to contain the
   * destination remote files of the tests that need to create them.
   *
   * Please note that a new temporary directory is created and deleted for each
   * test by the Setup() and TearDown() methods.
   */
  char m_tmpDir[100];

  class MockArchiveJob: public cta::ArchiveJob {
  public:
    MockArchiveJob() {
    } 
      
    ~MockArchiveJob() throw() {
    } 
  };

  class MockRetrieveJob: public cta::RetrieveJob {
  public:
    MockRetrieveJob() {
    } 
      
    ~MockRetrieveJob() throw() {
    } 
  };
};

TEST_F(castor_tape_tapeserver_daemon_DataTransferSessionTest, DataTransferSessionGooddayRecall) {
  // 0) Prepare the logger for everyone
  castor::log::StringLogger logger("tapeServerUnitTest");
  
  // 1) prepare the fake scheduler
  std::string vid = "V12345";
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;
  std::string density = "8000GC";

  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete 
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive;

  // 4) Create the scheduler
  cta::MockNameServer ns;
  cta::MockRemoteNS rns;
  cta::MockSchedulerDatabase db;
  cta::Scheduler scheduler(ns, db, rns);

  // Always use the same requester
  const cta::SecurityIdentity requester;

  // List to remember the path of each remote file so that the existance of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;
  
  // 5) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], 
        "V12345");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid="V12345";
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
       volInfo , 0, true);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    castor::tape::SCSI::Structures::zeroStruct(&data);
    for (int fseq=1; fseq <= 10 ; fseq ++) {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file:" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      std::ostringstream archiveFilePath;
      archiveFilePath << "/test" << fseq;
      const mode_t archiveFileMode = 0655;
      const uint64_t archiveFileSize = 256*1024;
      ASSERT_NO_THROW(ns.createFile(
        requester,
        archiveFilePath.str(),
        archiveFileMode,
        archiveFileSize));
        
      // Write the file to tape
      std::unique_ptr<cta::RetrieveJob> ftr(new MockRetrieveJob());
      std::unique_ptr<cta::ArchiveJob> ftm(new MockArchiveJob());
      ftr->tapeFileLocation.fSeq = fseq;
      ftm->tapeFileLocation.fSeq = fseq;
      ftr->archiveFile.fileId = 1000 + fseq;
      ftm->archiveFile.fileId = 1000 + fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *ftm, archiveFileSize);
      ftr->tapeFileLocation.blockId = wf.getPosition();
      ftr->remotePathAndStatus.path = remoteFilePath.str();
      // Write the data (one block)
      wf.write(data, sizeof(data));
      // Close the file
      wf.close();

      // Create tape file entry in the archive namespace
      cta::NameServerTapeFile tapeFile;
      tapeFile.copyNb = 1;
      tapeFile.tapeFileLocation.fSeq = fseq;
      tapeFile.tapeFileLocation.blockId = wf.getPosition();
      tapeFile.tapeFileLocation.vid = volInfo.vid;
      tapeFile.tapeFileLocation.copyNb = 1;
      tapeFile.size = archiveFileSize;
      tapeFile.compressedSize = archiveFileSize; // No compression
      cta::Checksum tapeFileChecksum(cta::Checksum::CHECKSUMTYPE_ADLER32,
        cta::ByteArray(cta::Utils::getAdler32(data, sizeof data)));
      tapeFile.checksum = tapeFileChecksum;
      ASSERT_NO_THROW(ns.addTapeFile(
        requester,
        archiveFilePath.str(),
        tapeFile));

      // Schedule the retrieval of the file
      std::list<std::string> archiveFilePaths;
      archiveFilePaths.push_back(archiveFilePath.str());
      scheduler. queueRetrieveRequest(
        requester,
        archiveFilePaths,
        remoteFilePath.str());
    }
  }

  // 6) Create the data transfer session
  DriveConfig driveConfig("T10D6116", "T10KD6", "/dev/tape_T10D6116", "manual");
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castor::messages::AcsProxyDummy acs;
  castor::mediachanger::MmcProxyDummy mmc;
  castor::legacymsg::RmcProxyDummy rmc;
  castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
  castor::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);

  // 7) Run the data transfer session
  ASSERT_NO_THROW(sess.execute());

  // 8) Check the session git the correct VID
  ASSERT_EQ("V12345", sess.getVid());

  // 9) Check the remote files exist and have the correct size
  for(auto pathItor = remoteFilePaths.cbegin(); pathItor !=
    remoteFilePaths.cend(); pathItor++) {
    struct stat statBuf;
    bzero(&statBuf, sizeof(statBuf));
    const int statRc = stat(pathItor->c_str(), &statBuf);
    ASSERT_EQ(0, statRc);
    ASSERT_EQ(256*1024, statBuf.st_size);
  }
}

TEST_F(castor_tape_tapeserver_daemon_DataTransferSessionTest, DataTransferSessionWrongRecall) {
  // This test is the same as the previous one, with 
  // wrong parameters set for the recall, so that we fail 
  // to recall the first file and cancel the second.

  // 0) Prepare the logger for everyone
  castor::log::StringLogger logger("tapeServerUnitTest");
  
  // 1) prepare the fake scheduler
  std::string vid = "V12345";
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;
  std::string density = "8000GC";

  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete 
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive;

  // 4) Create the scheduler
  cta::MockNameServer ns;
  cta::MockRemoteNS rns;
  cta::MockSchedulerDatabase db;
  cta::Scheduler scheduler(ns, db, rns);

  // Always use the same requester
  const cta::SecurityIdentity requester;

  // List to remember the path of each remote file so that the existance of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;
  
  // 5) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], 
        "V12345");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid="V12345";
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
       volInfo , 0, true);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    castor::tape::SCSI::Structures::zeroStruct(&data);
    for (int fseq=1; fseq <= 10 ; fseq ++) {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file:" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      std::ostringstream archiveFilePath;
      archiveFilePath << "/test" << fseq;
      const mode_t archiveFileMode = 0655;
      const uint64_t archiveFileSize = 256*1024;
      ASSERT_NO_THROW(ns.createFile(
        requester,
        archiveFilePath.str(),
        archiveFileMode,
        archiveFileSize));

      // Write the file to tape
      std::unique_ptr<cta::RetrieveJob> ftr(new MockRetrieveJob());
      std::unique_ptr<cta::ArchiveJob> ftm_temp(new MockArchiveJob());
      ftr->tapeFileLocation.fSeq = fseq;
      ftm_temp->tapeFileLocation.fSeq = fseq;
      ftr->archiveFile.fileId = 1000 + fseq;
      ftm_temp->archiveFile.fileId = 1000 + fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *ftm_temp, archiveFileSize);
      ftr->tapeFileLocation.blockId = wf.getPosition();
      ftr->remotePathAndStatus.path = remoteFilePath.str();
      // Write the data (one block)
      wf.write(data, sizeof(data));
      // Close the file
      wf.close();

      // Create tape file entry in the archive namespace that is beyond the end
      // of data
      cta::NameServerTapeFile tapeFile;
      tapeFile.copyNb = 1;
      tapeFile.tapeFileLocation.fSeq = fseq + 10000;
      tapeFile.tapeFileLocation.blockId = wf.getPosition() + 10000;
      tapeFile.tapeFileLocation.vid = volInfo.vid;
      tapeFile.tapeFileLocation.copyNb = 1;
      tapeFile.size = archiveFileSize;
      tapeFile.compressedSize = archiveFileSize; // No compression
      cta::Checksum tapeFileChecksum(cta::Checksum::CHECKSUMTYPE_ADLER32,
        cta::ByteArray(cta::Utils::getAdler32(data, sizeof data)));
      tapeFile.checksum = tapeFileChecksum;
      ASSERT_NO_THROW(ns.addTapeFile(
        requester,
        archiveFilePath.str(),
        tapeFile));

      // Schedule the retrieval of the file
      std::list<std::string> archiveFilePaths;
      archiveFilePaths.push_back(archiveFilePath.str());
      scheduler. queueRetrieveRequest(
        requester,
        archiveFilePaths,
        remoteFilePath.str());
    }
  }

  // 6) Create the data transfer session
  DriveConfig driveConfig("T10D6116", "T10KD6", "/dev/tape_T10D6116", "manual");
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castor::messages::AcsProxyDummy acs;
  castor::mediachanger::MmcProxyDummy mmc;
  castor::legacymsg::RmcProxyDummy rmc;
  castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
  castor::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);

  // 7) Run the data transfer session
  ASSERT_NO_THROW(sess.execute());

  // 8) Check the session git the correct VID
  ASSERT_EQ("V12345", sess.getVid());

  // 9) Check the remote files exist and have the correct size
  for(auto pathItor = remoteFilePaths.cbegin(); pathItor !=
    remoteFilePaths.cend(); pathItor++) {
    struct stat statBuf;
    bzero(&statBuf, sizeof(statBuf));
    const int statRc = stat(pathItor->c_str(), &statBuf);
    ASSERT_EQ(0, statRc);
    ASSERT_EQ(256*1024, statBuf.st_size);
  }
}

TEST_F(castor_tape_tapeserver_daemon_DataTransferSessionTest, DataTransferSessionNoSuchDrive) {
  
  // 0) Prepare the logger for everyone
  castor::log::StringLogger logger("tapeServerUnitTest");
  
  // 1) prepare the fake scheduler
  std::string vid = "V12345";
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;
  std::string density = "8000GC";

  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete 
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive;

  // 4) Create the scheduler
  cta::MockNameServer ns;
  cta::MockRemoteNS rns;
  cta::MockSchedulerDatabase db;
  cta::Scheduler scheduler(ns, db, rns);

  // Always use the same requester
  const cta::SecurityIdentity requester;
  
  DriveConfig driveConfig("T10D6116", "T10KD6", "/dev/noSuchTape", "manual");
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024;
  castorConf.nbBufs = 10;
  castor::messages::AcsProxyDummy acs;
  castor::mediachanger::MmcProxyDummy mmc;
  castor::legacymsg::RmcProxyDummy rmc;
  castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
  castor::messages::TapeserverProxyDummy initialProcess;
  castor::server::ProcessCapDummy capUtils;
  DataTransferSession sess("tapeHost", logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  ASSERT_NO_THROW(sess.execute());
  std::string temp = logger.getLog();
  ASSERT_NE(std::string::npos, logger.getLog().find("Drive not found on this path"));
}

TEST_F(castor_tape_tapeserver_daemon_DataTransferSessionTest, DataTransferSessionFailtoMount) {
  
  // 0) Prepare the logger for everyone
  castor::log::StringLogger logger("tapeServerUnitTest");
  
  // 1) prepare the fake scheduler
  std::string vid = "V12345";
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;
  std::string density = "8000GC";

  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete 
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive;

  // 4) Create the scheduler
  cta::MockNameServer ns;
  cta::MockRemoteNS rns;
  cta::MockSchedulerDatabase db;
  cta::Scheduler scheduler(ns, db, rns);

  // Always use the same requester
  const cta::SecurityIdentity requester;
  
  // List to remember the path of each remote file so that the existance of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete 
  const bool failOnMount=true;
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive(failOnMount);
  
  // We can prepare files for reading on the drive
  {
    
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], 
        "V12345");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid="V12345";
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
       volInfo , 0, true);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    castor::tape::SCSI::Structures::zeroStruct(&data);
    for (int fseq=1; fseq <= 10 ; fseq ++) {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file:" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      std::ostringstream archiveFilePath;
      archiveFilePath << "/test" << fseq;
      const mode_t archiveFileMode = 0655;
      const uint64_t archiveFileSize = 256*1024;
      ASSERT_NO_THROW(ns.createFile(
        requester,
        archiveFilePath.str(),
        archiveFileMode,
        archiveFileSize));

      // Write the file to tape
      std::unique_ptr<cta::RetrieveJob> ftr(new MockRetrieveJob());
      std::unique_ptr<cta::ArchiveJob> ftm_temp(new MockArchiveJob());
      ftr->tapeFileLocation.fSeq = fseq;
      ftm_temp->tapeFileLocation.fSeq = fseq;
      ftr->archiveFile.fileId = 1000 + fseq;
      ftm_temp->archiveFile.fileId = 1000 + fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *ftm_temp, archiveFileSize);
      ftr->tapeFileLocation.blockId = wf.getPosition();
      ftr->remotePathAndStatus.path = remoteFilePath.str();
      // Write the data (one block)
      wf.write(data, sizeof(data));
      // Close the file
      wf.close();

      // Create tape file entry in the archive namespace
      cta::NameServerTapeFile tapeFile;
      tapeFile.copyNb = 1;
      tapeFile.tapeFileLocation.fSeq = fseq;
      tapeFile.tapeFileLocation.blockId = wf.getPosition();
      tapeFile.tapeFileLocation.vid = volInfo.vid;
      tapeFile.tapeFileLocation.copyNb = 1;
      tapeFile.size = archiveFileSize;
      tapeFile.compressedSize = archiveFileSize; // No compression
      cta::Checksum tapeFileChecksum(cta::Checksum::CHECKSUMTYPE_ADLER32,
        cta::ByteArray(cta::Utils::getAdler32(data, sizeof data)));
      tapeFile.checksum = tapeFileChecksum;
      ASSERT_NO_THROW(ns.addTapeFile(
        requester,
        archiveFilePath.str(),
        tapeFile));

      // Schedule the retrieval of the file
      std::list<std::string> archiveFilePaths;
      archiveFilePaths.push_back(archiveFilePath.str());
      scheduler. queueRetrieveRequest(
        requester,
        archiveFilePaths,
        remoteFilePath.str());
    }
  }
  DriveConfig driveConfig("T10D6116", "T10KD6", "/dev/tape_T10D6116", "manual");
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.nbDiskThreads = 3;
  castor::messages::AcsProxyDummy acs;
  castor::mediachanger::MmcProxyDummy mmc;
  castor::legacymsg::RmcProxyDummy rmc;
  castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
  castor::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  ASSERT_NO_THROW(sess.execute());
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ("V12345", sess.getVid());
}

TEST_F(castor_tape_tapeserver_daemon_DataTransferSessionTest, DataTransferSessionEmptyOnVolReq) {
  
  // 0) Prepare the logger for everyone
  castor::log::StringLogger logger("tapeServerUnitTest");
  
  // 1) prepare the fake scheduler
  std::string vid = "V12345";
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;
  std::string density = "8000GC";

  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  
  // The drive will not even be opened. so no need for one.
  mockSys.fake.m_pathToDrive["/dev/nst0"] = NULL;
  
  // 4) Create the scheduler
  cta::MockNameServer ns;
  cta::MockRemoteNS rns;
  cta::MockSchedulerDatabase db;
  cta::Scheduler scheduler(ns, db, rns);

  // Always use the same requester
  const cta::SecurityIdentity requester;
  
  DriveConfig driveConfig("T10D6116", "T10KD6", "/dev/tape_T10D6116", "manual");
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.nbDiskThreads = 3;
  castor::messages::AcsProxyDummy acs;
  castor::mediachanger::MmcProxyDummy mmc;
  castor::legacymsg::RmcProxyDummy rmc;
  castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
  castor::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  ASSERT_NO_THROW(sess.execute());
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ("", sess.getVid());
  // We should not have logged any error
  ASSERT_EQ(std::string::npos, logger.getLog().find("LVL=E"));
}

class TempFileForData {
public:
  TempFileForData(size_t size): m_size(size) {
    // Create a temporary file
    int randomFd = open("/dev/urandom", 0);
    castor::exception::Errnum::throwOnMinusOne(randomFd, "Error opening /dev/urandom");
    char path[100];
    strncpy(path, "/tmp/castorUnitTestMigrationSourceXXXXXX", 100);
    int tmpFileFd = mkstemp(path);
    castor::exception::Errnum::throwOnMinusOne(tmpFileFd, "Error creating a temporary file");
    m_path = path;
    char * buff = NULL;
    try {
      buff = new char[size];
      if(!buff) { throw castor::exception::Exception("Failed to allocate memory to read /dev/urandom"); }
      castor::exception::Errnum::throwOnMinusOne(read(randomFd, buff, size));
      castor::exception::Errnum::throwOnMinusOne(write(tmpFileFd, buff, size));
      m_checksum = adler32(0L, Z_NULL, 0);
      m_checksum = adler32(m_checksum, (const Bytef *)buff, size);
      close(randomFd);
      close(tmpFileFd);
      delete[] buff;
    } catch (...) {
      delete[] buff;
      unlink(m_path.c_str());
      throw;
    }
  }
  
  ~TempFileForData() {
    unlink(m_path.c_str());
  }
  const size_t m_size;
  const std::string path() const { return m_path; }
  uint32_t checksum() const { return m_checksum; }
private:
  std::string m_path;
  uint32_t m_checksum;
};

class tempFileVector: public std::vector<TempFileForData *> {
public:
  ~tempFileVector() {
    while(size()) {
      delete back();
      pop_back();
    }
  }
};

struct expectedResult {
  expectedResult(int fs, uint32_t cs, int eCode = 0):
    fSeq(fs), checksum(cs), errorCode(eCode) {}
  int fSeq;
  uint32_t checksum;
  int errorCode;
};
/*
TEST_F(castor_tape_tapeserver_daemon_DataTransferSessionTest, DataTransferSessionGooddayMigration) {
  // TpcpClients only supports 32 bits session number
  // This number has to be less than 2^31 as in addition there is a mix
  // of signed and unsigned numbers
  // As the current ids in prod are ~30M, we are far from overflow (Feb 2013)
  // 0) Prepare the logger for everyone
  castor::log::StringLogger logger("tapeServerUnitTest");
  
  // 1) prepare the client and run it in another thread
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  client::ClientSimulator sim(volReq, vid, density,
    castor::tape::tapegateway::WRITE_TP, castor::tape::tapegateway::WRITE);
  client::ClientSimulator::ipPort clientAddr = sim.getCallbackAddress();
  clientRunner simRun(sim);
  simRun.start();
  
  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  // construct and run the session.
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete 
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive;
  
  // Just label the tape
  castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], 
      "V12345");
  mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
  
  tempFileVector tempFiles;
  std::vector<expectedResult> expected;
  // Prepare the files (in real filesystem as they will be opened by the rfio client)
  for (int fseq=1; fseq<=10; fseq++) {
    // Create the file from which we will recall
    std::unique_ptr<TempFileForData> tf(new TempFileForData(1000));
    // Prepare the migrationRequest
    MockArchiveJob ftm;
    ftm.setFileSize(tf->m_size);
    ftm.archiveFile.fileId = 1000 + fseq;
    ftm.tapeFileLocation.fSeq = fseq;
    ftm.setPath(tf->path());
    sim.addFileToMigrate(ftm);
    expected.push_back(expectedResult(fseq, tf->checksum()));
    tempFiles.push_back(tf.release());
  }
  DriveConfig driveConfig("T10D6116", "T10KD6", "/dev/tape_T10D6116", "manual");
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestMigrationMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestMigrationMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castor::messages::AcsProxyDummy acs;
  castor::mediachanger::MmcProxyDummy mmc;
  castor::legacymsg::RmcProxyDummy rmc;
  castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
  castor::messages::TapeserverProxyDummy initialProcess;
  castor::server::ProcessCapDummy capUtils;
  cta::MockNameServer ns;
  cta::MockRemoteNS rns;
  cta::MockSchedulerDatabase db;
  cta::Scheduler scheduler(ns, db, rns);
  DataTransferSession sess("tapeHost", logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  sess.execute();
  simRun.wait();
  for (std::vector<struct expectedResult>::iterator i = expected.begin();
      i != expected.end(); i++) {
    ASSERT_EQ(i->checksum, sim.m_receivedChecksums[i->fSeq]);
  }
  ASSERT_EQ(0, sim.m_sessionErrorCode);
}

//
// This test is the same as the previous one, except that the files are deleted
// from filesystem immediately. The disk tasks will then fail on open.
///
TEST_F(castor_tape_tapeserver_daemon_DataTransferSessionTest, DataTransferSessionMissingFilesMigration) {
  // TpcpClients only supports 32 bits session number
  // This number has to be less than 2^31 as in addition there is a mix
  // of signed and unsigned numbers
  // As the current ids in prod are ~30M, we are far from overflow (Feb 2013)
  // 0) Prepare the logger for everyone
  castor::log::StringLogger logger("tapeServerUnitTest");
  
  // 1) prepare the client and run it in another thread
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  client::ClientSimulator sim(volReq, vid, density,
    castor::tape::tapegateway::WRITE_TP, castor::tape::tapegateway::WRITE);
  client::ClientSimulator::ipPort clientAddr = sim.getCallbackAddress();
  clientRunner simRun(sim);
  simRun.start();
  
  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  // construct and run the session.
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete 
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive;
  
  // Just label the tape
  castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], 
      "V12345");
  mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
  
  // Prepare the files, but delete them immediately. The migration will fail.
  for (int fseq=1; fseq<=10; fseq++) {
    // Create the file from which we will recall
    std::unique_ptr<TempFileForData> tf(new TempFileForData(1000));
    // Prepare the migrationRequest
    MockArchiveJob ftm;
    ftm.setFileSize(tf->m_size);
    ftm.archiveFile.fileId = 1000 + fseq;
    ftm.tapeFileLocation.fSeq = fseq;
    ftm.setPath(tf->path());
    sim.addFileToMigrate(ftm);
  }
  DriveConfig driveConfig("T10D6116", "T10KD6", "/dev/tape_T10D6116", "manual");
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestMigrationMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestMigrationMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castor::messages::AcsProxyDummy acs;
  castor::mediachanger::MmcProxyDummy mmc;
  castor::legacymsg::RmcProxyDummy rmc;
  castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
  castor::messages::TapeserverProxyDummy initialProcess;
  castor::server::ProcessCapDummy capUtils;
  cta::MockNameServer ns;
  cta::MockRemoteNS rns;
  cta::MockSchedulerDatabase db;
  cta::Scheduler scheduler(ns, db, rns);
  DataTransferSession sess("tapeHost", logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  sess.execute();
  simRun.wait();
  ASSERT_EQ(SEINTERNAL, sim.m_sessionErrorCode);
}

//
// This test is identical to the good day migration, but the tape will accept
// only a finite number of bytes and hence we will report a full tape skip the
// last migrations
//
TEST_F(castor_tape_tapeserver_daemon_DataTransferSessionTest, DataTransferSessionTapeFullMigration) {
  // TpcpClients only supports 32 bits session number
  // This number has to be less than 2^31 as in addition there is a mix
  // of signed and unsigned numbers
  // As the current ids in prod are ~30M, we are far from overflow (Feb 2013)
  // 0) Prepare the logger for everyone
  castor::log::StringLogger logger("tapeServerUnitTest");
  
  // 1) prepare the client and run it in another thread
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  client::ClientSimulator sim(volReq, vid, density,
    castor::tape::tapegateway::WRITE_TP, castor::tape::tapegateway::WRITE);
  client::ClientSimulator::ipPort clientAddr = sim.getCallbackAddress();
  clientRunner simRun(sim);
  simRun.start();
  
  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  // construct and run the session.
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete 
  const uint64_t tapeSize = 5000;
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive(tapeSize);
  
  // Just label the tape
  castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], 
      "V12345");
  mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
  
  tempFileVector tempFiles;
  std::vector<expectedResult> expected;
  uint64_t remainingSpace = tapeSize;
  bool failedFileDone = false;
  // Prepare the files (in real filesystem as they will be opened by the rfio client)
  for (int fseq=1; fseq<=10; fseq++) {
    // Create the file from which we will recall
    const size_t fileSize = 1000;
    std::unique_ptr<TempFileForData> tf(new TempFileForData(fileSize));
    // Prepare the migrationRequest
    MockArchiveJob ftm;
    ftm.setFileSize(tf->m_size);
    ftm.archiveFile.fileId = 1000 + fseq;
    ftm.tapeFileLocation.fSeq = fseq;
    ftm.setPath(tf->path());
    sim.addFileToMigrate(ftm);
    if (fileSize + 6 * 80 < remainingSpace) {
      expected.push_back(expectedResult(fseq, tf->checksum()));
      remainingSpace -= fileSize + 6 * 80;
    } else if (!failedFileDone) {
      // We add also the report for the first file (which will come in error)
      expected.push_back(expectedResult(fseq, 0, ENOSPC));
      failedFileDone = true;
    }
    tempFiles.push_back(tf.release());
  }
  DriveConfig driveConfig("T10D6116", "T10KD6", "/dev/tape_T10D6116", "manual");
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestMigrationMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestMigrationMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castor::messages::AcsProxyDummy acs;
  castor::mediachanger::MmcProxyDummy mmc;
  castor::legacymsg::RmcProxyDummy rmc;
  castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
  castor::messages::TapeserverProxyDummy initialProcess;
  castor::server::ProcessCapDummy capUtils;
  cta::MockNameServer ns;
  cta::MockRemoteNS rns;
  cta::MockSchedulerDatabase db;
  cta::Scheduler scheduler(ns, db, rns);
  DataTransferSession sess("tapeHost", logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  sess.execute();
  simRun.wait();
  for (std::vector<struct expectedResult>::iterator i = expected.begin();
      i != expected.end(); i++) {
    if (!i->errorCode) {
      ASSERT_EQ(i->checksum, sim.m_receivedChecksums[i->fSeq]);
    } else {
      ASSERT_EQ(i->errorCode, sim.m_receivedErrorCodes[i->fSeq]);
    }
  }
  ASSERT_EQ(ENOSPC, sim.m_sessionErrorCode);
}

TEST_F(castor_tape_tapeserver_daemon_DataTransferSessionTest, DataTransferSessionTapeFullOnFlushMigration) {
  // TpcpClients only supports 32 bits session number
  // This number has to be less than 2^31 as in addition there is a mix
  // of signed and unsigned numbers
  // As the current ids in prod are ~30M, we are far from overflow (Feb 2013)
  // 0) Prepare the logger for everyone
  castor::log::StringLogger logger("tapeServerUnitTest");
  
  // 1) prepare the client and run it in another thread
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  client::ClientSimulator sim(volReq, vid, density,
    castor::tape::tapegateway::WRITE_TP, castor::tape::tapegateway::WRITE);
  client::ClientSimulator::ipPort clientAddr = sim.getCallbackAddress();
  clientRunner simRun(sim);
  simRun.start();
  
  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  // construct and run the session.
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete 
  const uint64_t tapeSize = 5000;
  mockSys.fake.m_pathToDrive["/dev/nst0"] =
      new castor::tape::tapeserver::drive::FakeDrive(tapeSize,
        castor::tape::tapeserver::drive::FakeDrive::OnFlush);
  
  // Just label the tape
  castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], 
      "V12345");
  mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
  
  tempFileVector tempFiles;
  std::vector<expectedResult> expected;
  uint64_t remainingSpace = tapeSize;
  bool failedFileDone = false;
  // Prepare the files (in real filesystem as they will be opened by the rfio client)
  for (int fseq=1; fseq<=10; fseq++) {
    // Create the file from which we will recall
    const size_t fileSize = 1000;
    std::unique_ptr<TempFileForData> tf(new TempFileForData(fileSize));
    // Prepare the migrationRequest
    MockArchiveJob ftm;
    ftm.setFileSize(tf->m_size);
    ftm.archiveFile.fileId = 1000 + fseq;
    ftm.tapeFileLocation.fSeq = fseq;
    ftm.setPath(tf->path());
    sim.addFileToMigrate(ftm);
    if (fileSize + 6 * 80 < remainingSpace) {
      expected.push_back(expectedResult(fseq, tf->checksum()));
      remainingSpace -= fileSize + 6 * 80;
    } else if (!failedFileDone) {
      // We expect no report for this file anymore (but the report for the 
      // session)
      failedFileDone = true;
    }
    tempFiles.push_back(tf.release());
  }
  DriveConfig driveConfig("T10D6116", "T10KD6", "/dev/tape_T10D6116", "manual");
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestMigrationMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestMigrationMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castor::messages::AcsProxyDummy acs;
  castor::mediachanger::MmcProxyDummy mmc;
  castor::legacymsg::RmcProxyDummy rmc;
  castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
  castor::messages::TapeserverProxyDummy initialProcess;
  castor::server::ProcessCapDummy capUtils;
  cta::MockNameServer ns;
  cta::MockRemoteNS rns;
  cta::MockSchedulerDatabase db;
  cta::Scheduler scheduler(ns, db, rns);
  DataTransferSession sess("tapeHost", logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  sess.execute();
  simRun.wait();
  for (std::vector<struct expectedResult>::iterator i = expected.begin();
      i != expected.end(); i++) {
    if (!i->errorCode) {
      ASSERT_EQ(i->checksum, sim.m_receivedChecksums[i->fSeq]);
    } else {
      ASSERT_EQ(i->errorCode, sim.m_receivedErrorCodes[i->fSeq]);
    }
  }
  ASSERT_EQ(ENOSPC, sim.m_sessionErrorCode);
}
*/

} // namespace unitTest

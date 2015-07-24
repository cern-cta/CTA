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
#include "castor/legacymsg/VmgrProxyDummy.hpp"
#include "castor/legacymsg/VdqmProxyDummy.hpp"
#include "castor/log/StringLogger.hpp"
#include "castor/mediachanger/MediaChangerFacade.hpp"
#include "castor/mediachanger/MmcProxyDummy.hpp"
#include "castor/messages/AcsProxyDummy.hpp"
#include "castor/messages/TapeserverProxyDummy.hpp"
#include "castor/server/ProcessCapDummy.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/RetryPolicyElement.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapeserver/client/ClientSimulator.hpp"
#include "castor/tape/tapeserver/client/ClientSimSingleReply.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "castor/server/Threading.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/tapeserver/drive/FakeDrive.hpp"
#include "Ctape.h"
#include "smc_struct.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>


using namespace castor::tape::tapeserver;
using namespace castor::tape::tapeserver::daemon;
namespace unitTest {

class clientRunner: public castor::server::Thread {
public:
  clientRunner(client::ClientSimulator &client): m_sim(client) {}
private:
  void run() {
    m_sim.sessionLoop();
  }
  client::ClientSimulator & m_sim;
};

TEST(tapeServer, DataTransferSessionGooddayRecall) {
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
    castor::tape::tapegateway::READ_TP, castor::tape::tapegateway::READ);
  client::ClientSimulator::ipPort clientAddr = sim.getCallbackAddress();
  clientRunner simRun(sim);
  simRun.start();
  
  // 2) Prepare the VDQM request
  castor::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.parts.a, clientAddr.parts.b, clientAddr.parts.c, clientAddr.parts.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  
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
  
  // We can prepare files for reading on the drive
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], 
        "V12345");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::client::ClientInterface::VolumeInfo volInfo;
    volInfo.vid="V12345";
    volInfo.clientType=castor::tape::tapegateway::READ_TP;
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
       volInfo , 0, true);
    // Write a few files on the virtual tape
    // Prepare the data
    uint8_t data[1000];
    castor::tape::SCSI::Structures::zeroStruct(&data);
    for (int fseq=1; fseq <= 10 ; fseq ++) {
      castor::tape::tapegateway::FileToRecallStruct ftr;
      castor::tape::tapegateway::FileToMigrateStruct ftm_temp;
      ftr.setFseq(fseq);
      ftm_temp.setFseq(fseq);
      ftr.setFileid(1000 + fseq);
      ftm_temp.setFileid(1000 + fseq);
      castor::tape::tapeFile::WriteFile wf(&ws, ftm_temp, 256*1024);
      // Cut up the position into the old-style BlockId0-3
      ftr.setBlockId0(wf.getPosition() >> 24);
      ftr.setBlockId1( (wf.getPosition() >> 16) & 0xFF);
      ftr.setBlockId2( (wf.getPosition() >> 8) & 0xFF);
      ftr.setBlockId3(wf.getPosition() & 0xFF);
      // Set the recall destination (/dev/null)
      ftr.setPath("/dev/null");
      // Write the data (one block)
      wf.write(data, sizeof(data));
      // Close the file
      wf.close();
      // Record the file for recall
      sim.addFileToRecall(ftr, sizeof(data));
    }
  }
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
  DataTransferSession sess("tapeHost", VDQMjob, logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf);
  sess.execute();
  simRun.wait();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ("V12345", sess.getVid());
  ASSERT_EQ(0, sim.m_sessionErrorCode);
}

TEST(tapeServer, DataTransferSessionWrongRecall) {
  // This test is the same as the previous one, with 
  // wrong parameters set for the recall, so that we fail 
  // to recall the first file and cancel the second.
  castor::log::StringLogger logger("tapeServerUnitTest");
  
  // 1) prepare the client and run it in another thread
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  client::ClientSimulator sim(volReq, vid, density,
    castor::tape::tapegateway::TAPE_GATEWAY,
    castor::tape::tapegateway::READ);
  client::ClientSimulator::ipPort clientAddr = sim.getCallbackAddress();
  clientRunner simRun(sim);
  simRun.start();
  
  // 2) Prepare the VDQM request
  castor::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.parts.a, clientAddr.parts.b, clientAddr.parts.c, clientAddr.parts.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  
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
  
  // We can prepare files for reading on the drive
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], 
        "V12345");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::client::ClientInterface::VolumeInfo volInfo;
    volInfo.vid="V12345";
    volInfo.clientType=castor::tape::tapegateway::READ_TP;
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
       volInfo , 0, true);
    // Write a few files on the virtual tape
    // Prepare the data
    uint8_t data[1000];
    castor::tape::SCSI::Structures::zeroStruct(&data);
    for (int fseq=1; fseq <= 10 ; fseq ++) {
      castor::tape::tapegateway::FileToRecallStruct ftr;
      castor::tape::tapegateway::FileToMigrateStruct ftm_temp;
      ftr.setFseq(fseq);
      ftm_temp.setFseq(fseq);
      ftr.setFileid(1000 + fseq);
      ftm_temp.setFileid(1000 + fseq);
      castor::tape::tapeFile::WriteFile wf(&ws, ftm_temp, 256*1024);
      // Cut up the position into the old-style BlockId0-3
      ftr.setBlockId0(wf.getPosition() >> 24);
      ftr.setBlockId1( (wf.getPosition() >> 16) & 0xFF);
      ftr.setBlockId2( (wf.getPosition() >> 8) & 0xFF);
      ftr.setBlockId3(wf.getPosition() & 0xFF);
      // Set the recall destination (/dev/null)
      ftr.setPath("/dev/null");
      // Write the data (one block)
      wf.write(data, sizeof(data));
      // Close the file
      wf.close();
      // Record the file for recall, with an out of tape fSeq
      ftr.setFseq(ftr.fseq() + 1000);
      sim.addFileToRecall(ftr, sizeof(data));
    }
  }
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
  DataTransferSession sess("tapeHost", VDQMjob, logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf);
  sess.execute();
  simRun.wait();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ("V12345", sess.getVid());
  ASSERT_EQ(SEINTERNAL, sim.m_sessionErrorCode);
}

TEST(tapeServer, DataTransferSessionNoSuchDrive) {
  // TpcpClients only supports 32 bits session number
  // This number has to be less than 2^31 as in addition there is a mix
  // of signed and unsigned numbers
  // As the current ids in prod are ~30M, we are far from overflow (Feb 2013)
  // 1) prepare the client and run it in another thread
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  client::ClientSimulator sim(volReq, vid, density,
    castor::tape::tapegateway::READ_TP, castor::tape::tapegateway::READ);
  client::ClientSimulator::ipPort clientAddr = sim.getCallbackAddress();
  clientRunner simRun(sim);
  simRun.start();
  
  // 2) Prepare the VDQM request
  castor::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.parts.a, clientAddr.parts.b, clientAddr.parts.c, clientAddr.parts.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  
  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  // construct and run the session.
  castor::log::StringLogger logger("tapeServerUnitTest");
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  DriveConfig driveConfig("T10D6116", "T10KD6", "/dev/noSuchTape", "manual");
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024;
  castorConf.nbBufs = 10;
  castor::legacymsg::VmgrProxyDummy vmgr;
  castor::legacymsg::VdqmProxyDummy vdqm(VDQMjob);
  castor::messages::AcsProxyDummy acs;
  castor::mediachanger::MmcProxyDummy mmc;
  castor::legacymsg::RmcProxyDummy rmc;
  castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
  castor::messages::TapeserverProxyDummy initialProcess;
  castor::server::ProcessCapDummy capUtils;
  DataTransferSession sess("tapeHost", VDQMjob, logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf);
  sess.execute();
  simRun.wait();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_NE(std::string::npos, logger.getLog().find("Drive not found on this path"));
  ASSERT_EQ(SEINTERNAL, sim.m_sessionErrorCode);
}

TEST(tapeServer, DataTransferSessionFailtoMount) {
  // This test is the same as the previous one, with 
  // wrong parameters set for the recall, so that we fail 
  // to recall the first file and cancel the second.
  castor::log::StringLogger logger("tapeServerUnitTest");
  
  // 1) prepare the client and run it in another thread
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  client::ClientSimulator sim(volReq, vid, density,
    castor::tape::tapegateway::TAPE_GATEWAY,
    castor::tape::tapegateway::READ);
  client::ClientSimulator::ipPort clientAddr = sim.getCallbackAddress();
  clientRunner simRun(sim);
  simRun.start();
  
  // 2) Prepare the VDQM request
  castor::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.parts.a, clientAddr.parts.b, clientAddr.parts.c, clientAddr.parts.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  
  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  // construct and run the session.
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();

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
    castor::tape::tapeserver::client::ClientInterface::VolumeInfo volInfo;
    volInfo.vid="V12345";
    volInfo.clientType=castor::tape::tapegateway::READ_TP;
    // Prepare a non-empty files to recall list to pass the empty session
    // detection
    for (int fseq=1; fseq <= 10 ; fseq ++) {
      castor::tape::tapegateway::FileToRecallStruct ftr;
      ftr.setFseq(fseq);
      ftr.setFileid(1000 + fseq);
      // Set the recall destination (/dev/null)
      ftr.setPath("/dev/null");
      // Record the file for recall, with an out of tape fSeq
      ftr.setFseq(ftr.fseq() + 1000);
      sim.addFileToRecall(ftr, 1000);
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
  DataTransferSession sess("tapeHost", VDQMjob, logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf);
  sess.execute();
  simRun.wait();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ("V12345", sess.getVid());
  // The session is now failing (internal error is reported)
  ASSERT_EQ(1015, sim.m_sessionErrorCode);
}

TEST(tapeServer, DataTransferSessionEmptyOnVolReq) {
  // This test is the same as the previous one, with 
  // wrong parameters set for the recall, so that we fail 
  // to recall the first file and cancel the second.
  castor::log::StringLogger logger("tapeServerUnitTest");
  
  // 1) prepare the client and run it in another thread
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  client::ClientSimulator sim(volReq, vid, density,
    castor::tape::tapegateway::TAPE_GATEWAY,
    castor::tape::tapegateway::READ, client::ClientSimulator::EmptyOnVolReq);
  client::ClientSimulator::ipPort clientAddr = sim.getCallbackAddress();
  clientRunner simRun(sim);
  simRun.start();
  
  // 2) Prepare the VDQM request
  castor::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.parts.a, clientAddr.parts.b, clientAddr.parts.c, clientAddr.parts.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  
  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  // construct and run the session.
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();

  // The drive will not even be opened. so no need for one.
  mockSys.fake.m_pathToDrive["/dev/nst0"] = NULL;
  
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
  DataTransferSession sess("tapeHost", VDQMjob, logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf);
  sess.execute();
  simRun.wait();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ("", sess.getVid());
  // Currently, failures are reported by files and recall sessions do not fail.
  ASSERT_EQ(0, sim.m_sessionErrorCode);
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

TEST(tapeServer, DataTransferSessionGooddayMigration) {
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
  
  // 2) Prepare the VDQM request
  castor::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.parts.a, clientAddr.parts.b, clientAddr.parts.c, clientAddr.parts.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  
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
    castor::tape::tapegateway::FileToMigrateStruct ftm;
    ftm.setFileSize(tf->m_size);
    ftm.setFileid(1000 + fseq);
    ftm.setFseq(fseq);
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
  castor::legacymsg::VmgrProxyDummy vmgr;
  castor::legacymsg::VdqmProxyDummy vdqm(VDQMjob);
  castor::messages::AcsProxyDummy acs;
  castor::mediachanger::MmcProxyDummy mmc;
  castor::legacymsg::RmcProxyDummy rmc;
  castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
  castor::messages::TapeserverProxyDummy initialProcess;
  castor::server::ProcessCapDummy capUtils;
  DataTransferSession sess("tapeHost", VDQMjob, logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf);
  sess.execute();
  simRun.wait();
  for (std::vector<struct expectedResult>::iterator i = expected.begin();
      i != expected.end(); i++) {
    ASSERT_EQ(i->checksum, sim.m_receivedChecksums[i->fSeq]);
  }
  ASSERT_EQ(0, sim.m_sessionErrorCode);
}

/**
 * This test is the same as the previous one, except that the files are deleted
 * from filesystem immediately. The disk tasks will then fail on open.
 */
TEST(tapeServer, DataTransferSessionMissingFilesMigration) {
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
  
  // 2) Prepare the VDQM request
  castor::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.parts.a, clientAddr.parts.b, clientAddr.parts.c, clientAddr.parts.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  
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
    castor::tape::tapegateway::FileToMigrateStruct ftm;
    ftm.setFileSize(tf->m_size);
    ftm.setFileid(1000 + fseq);
    ftm.setFseq(fseq);
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
  castor::legacymsg::VmgrProxyDummy vmgr;
  castor::legacymsg::VdqmProxyDummy vdqm(VDQMjob);
  castor::messages::AcsProxyDummy acs;
  castor::mediachanger::MmcProxyDummy mmc;
  castor::legacymsg::RmcProxyDummy rmc;
  castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
  castor::messages::TapeserverProxyDummy initialProcess;
  castor::server::ProcessCapDummy capUtils;
  DataTransferSession sess("tapeHost", VDQMjob, logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf);
  sess.execute();
  simRun.wait();
  ASSERT_EQ(SEINTERNAL, sim.m_sessionErrorCode);
}

/**
 * This test is identical to the good day migration, but the tape will accept
 * only a finite number of bytes and hence we will report a full tape skip the
 * last migrations
 */
TEST(tapeServer, DataTransferSessionTapeFullMigration) {
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
  
  // 2) Prepare the VDQM request
  castor::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.parts.a, clientAddr.parts.b, clientAddr.parts.c, clientAddr.parts.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  
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
    castor::tape::tapegateway::FileToMigrateStruct ftm;
    ftm.setFileSize(tf->m_size);
    ftm.setFileid(1000 + fseq);
    ftm.setFseq(fseq);
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
  castor::legacymsg::VmgrProxyDummy vmgr;
  castor::legacymsg::VdqmProxyDummy vdqm(VDQMjob);
  castor::messages::AcsProxyDummy acs;
  castor::mediachanger::MmcProxyDummy mmc;
  castor::legacymsg::RmcProxyDummy rmc;
  castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
  castor::messages::TapeserverProxyDummy initialProcess;
  castor::server::ProcessCapDummy capUtils;
  DataTransferSession sess("tapeHost", VDQMjob, logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf);
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

TEST(tapeServer, DataTransferSessionTapeFullOnFlushMigration) {
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
  
  // 2) Prepare the VDQM request
  castor::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.parts.a, clientAddr.parts.b, clientAddr.parts.c, clientAddr.parts.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  
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
    castor::tape::tapegateway::FileToMigrateStruct ftm;
    ftm.setFileSize(tf->m_size);
    ftm.setFileid(1000 + fseq);
    ftm.setFseq(fseq);
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
  castor::legacymsg::VmgrProxyDummy vmgr;
  castor::legacymsg::VdqmProxyDummy vdqm(VDQMjob);
  castor::messages::AcsProxyDummy acs;
  castor::mediachanger::MmcProxyDummy mmc;
  castor::legacymsg::RmcProxyDummy rmc;
  castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
  castor::messages::TapeserverProxyDummy initialProcess;
  castor::server::ProcessCapDummy capUtils;
  DataTransferSession sess("tapeHost", VDQMjob, logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf);
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

} // namespace unitTest

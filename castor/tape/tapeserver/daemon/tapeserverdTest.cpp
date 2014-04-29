/******************************************************************************
 *                      tapeserverdTest.cpp
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
#include "castor/tape/tapeserver/client/ClientSimulator.hpp"
#include "castor/tape/tapeserver/client/ClientSimSingleReply.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "../threading/Threading.hpp"
#include "castor/log/StringLogger.hpp"
#include "MountSession.hpp"
#include "../system/Wrapper.hpp"
#include "Ctape.h"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapeserver/file/File.hpp"

using namespace castor::tape::tapeserver;
using namespace castor::tape::tapeserver::daemon;
namespace unitTest {

class clientRunner: public castor::tape::threading::Thread {
public:
  clientRunner(client::ClientSimulator &client): m_sim(client) {}
private:
  void run() {
    m_sim.sessionLoop();
  }
  client::ClientSimulator & m_sim;
};

  
TEST(tapeServer, MountSessionGoodday) {
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
  client::ClientSimulator sim(volReq, vid, density, tapegateway::READ_TP,
    tapegateway::READ);
  client::ClientSimulator::ipPort clientAddr = sim.getCallbackAddress();
  clientRunner simRun(sim);
  simRun.start();
  
  // 2) Prepare the VDQM request
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
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
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::drives::FakeDrive;
  
  // We can prepare files for reading on the drive
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], 
        "V12345", true);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
        "V12345", 0, true);
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
  utils::TpconfigLines tpConfig;
  // Actual TPCONFIG lifted from prod
  tpConfig.push_back(utils::TpconfigLine("T10D6116", "T10KD6", 
  "/dev/tape_T10D6116", "8000GC", "down", "acs0,1,1,6", "T10000"));
  tpConfig.push_back(utils::TpconfigLine("T10D6116", "T10KD6", 
  "/dev/tape_T10D6116", "5000GC", "down", "acs0,1,1,6", "T10000"));
  MountSession::CastorConf castorConf;
  castorConf.rtcopydBufsz = 1024*1024; // 1 MB memory buffers
  castorConf.rtcopydNbBufs = 10;
  castorConf.tapebridgeBulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.tapebridgeBulkRequestRecallMaxFiles = 1000;
  castorConf.tapeserverdDiskThreads = 1;
  MountSession sess(VDQMjob, logger, mockSys, tpConfig, castorConf);
  sess.execute();
  simRun.wait();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ("V12345", sess.getVid());
}

TEST(tapeServer, MountSessionNoSuchDrive) {
  // TpcpClients only supports 32 bits session number
  // This number has to be less than 2^31 as in addition there is a mix
  // of signed and unsigned numbers
  // As the current ids in prod are ~30M, we are far from overflow (Feb 2013)
  // 1) prepare the client and run it in another thread
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  client::ClientSimulator sim(volReq, vid, density, tapegateway::READ_TP,
    tapegateway::READ);
  client::ClientSimulator::ipPort clientAddr = sim.getCallbackAddress();
  clientRunner simRun(sim);
  simRun.start();
  
  // 2) Prepare the VDQM request
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
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
  utils::TpconfigLines tpConfig;
  // Actual TPCONFIG lifted from prod
  tpConfig.push_back(utils::TpconfigLine("T10D6116", "T10KD6", 
  "/dev/noSuchTape", "8000GC", "down", "acs0,1,1,6", "T10000"));
  tpConfig.push_back(utils::TpconfigLine("T10D6116", "T10KD6", 
  "/dev/noSuchTape", "5000GC", "down", "acs0,1,1,6", "T10000"));
  MountSession::CastorConf castorConf;
  castorConf.rtcopydBufsz = 1024;
  castorConf.rtcopydNbBufs = 10;
  MountSession sess(VDQMjob, logger, mockSys, tpConfig, castorConf);
  sess.execute();
  simRun.wait();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_NE(std::string::npos, logger.getLog().find("Drive not found on this path"));
}

}

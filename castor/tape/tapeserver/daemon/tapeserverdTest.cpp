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

#include <gtest/gtest.h>
#include "ClientSimulator.hpp"
#include "ClientSimSingleReply.hpp"
#include "ClientInterface.hpp"
#include "../threading/Threading.hpp"
#include "castor/log/StringLogger.hpp"
#include "MountSession.hpp"
#include "../system/Wrapper.hpp"
#include "Ctape.h"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"

using namespace castor::tape::tapeserver::daemon;

namespace unitTest {

class clientRunner: public castor::tape::threading::Thread {
public:
  clientRunner(ClientSimulator &client): m_sim(client) {}
private:
  void run() {
    m_sim.sessionLoop();
  }
  ClientSimulator & m_sim;
};

  
TEST(tapeServer, MountSessionGoodday) {
  // TpcpClients only supports 32 bits session number
  // This number has to be less than 2^31 as in addition there is a mix
  // of signed and unsigned numbers
  // As the current ids in prod are ~30M, we are far from overflow (Feb 2013)
  // 1) prepare the client and run it in another thread
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimulator sim(volReq, vid, density);
  struct ClientSimulator::ipPort clientAddr = sim.getCallbackAddress();
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
  mockSys.fake.setupForVirtualDriveSLC6();
  utils::TpconfigLines tpConfig;
  // Actual TPCONFIG lifted from prod
  tpConfig.push_back(utils::TpconfigLine("T10D6116", "T10KD6", 
  "/dev/tape_T10D6116", "8000GC", "down", "acs0,1,1,6", "T10000"));
  tpConfig.push_back(utils::TpconfigLine("T10D6116", "T10KD6", 
  "/dev/tape_T10D6116", "5000GC", "down", "acs0,1,1,6", "T10000"));
  MountSession sess(VDQMjob, logger, mockSys, tpConfig);
  sess.execute();
  simRun.wait();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ("V12345", sess.getVid());
}

template <class Repl>
class clientSingleReplRunner: public castor::tape::threading::Thread {
public:
  clientSingleReplRunner(ClientSimSingleReply<Repl> &client): m_sim(client) {}
private:
  void run() {
    m_sim.sessionLoop();
  }
  ClientSimSingleReply<Repl> & m_sim;
};

TEST(tapeServerClientInterface, VolReqVol) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<Volume> csVol(volReq, vid, density);
  struct ClientSimSingleReply<Volume>::ipPort clientAddr = csVol.getCallbackAddress();
  clientSingleReplRunner<Volume> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  ASSERT_NO_THROW(cInterf.fetchVolumeId(volInfo, reqRep));
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, VolReqNoMore) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<NoMoreFiles> csVol(volReq, vid, density);
  struct ClientSimSingleReply<NoMoreFiles>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<NoMoreFiles> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  ASSERT_THROW(cInterf.fetchVolumeId(volInfo, reqRep), ClientInterface::EndOfSession);
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, VolReqEndError) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<EndNotificationErrorReport> csVol(volReq, vid, density);
  struct ClientSimSingleReply<EndNotificationErrorReport>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<EndNotificationErrorReport> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  ASSERT_THROW(cInterf.fetchVolumeId(volInfo, reqRep), ClientInterface::EndOfSession);
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, VolReqVolReq) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<VolumeRequest> csVol(volReq, vid, density);
  struct ClientSimSingleReply<VolumeRequest>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<VolumeRequest> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  ASSERT_THROW(cInterf.fetchVolumeId(volInfo, reqRep), ClientInterface::UnexpectedResponse);
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, VolReqVolSeqBreak) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<VolumeRequest> csVol(volReq, vid, density, true);
  struct ClientSimSingleReply<VolumeRequest>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<VolumeRequest> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  ASSERT_THROW(cInterf.fetchVolumeId(volInfo, reqRep), ClientInterface::UnexpectedResponse);
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, EndSessionNotifAck) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<NotificationAcknowledge> csVol(volReq, vid, density);
  struct ClientSimSingleReply<NotificationAcknowledge>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<NotificationAcknowledge> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  ASSERT_NO_THROW(cInterf.reportEndOfSession(reqRep));
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, EndSessionNotifAckSeqBreak) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<NotificationAcknowledge> csVol(volReq, vid, density, true);
  struct ClientSimSingleReply<NotificationAcknowledge>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<NotificationAcknowledge> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  ASSERT_THROW(cInterf.reportEndOfSession(reqRep), ClientInterface::UnexpectedResponse);
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, EndSessionVolReq) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<VolumeRequest> csVol(volReq, vid, density);
  struct ClientSimSingleReply<VolumeRequest>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<VolumeRequest> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  ASSERT_THROW(cInterf.reportEndOfSession(reqRep), ClientInterface::UnexpectedResponse);
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, EndSessionErrorNotifAck) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<NotificationAcknowledge> csVol(volReq, vid, density);
  struct ClientSimSingleReply<NotificationAcknowledge>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<NotificationAcknowledge> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  ASSERT_NO_THROW(cInterf.reportEndOfSessionWithError("SNAFU!", SEINTERNAL, reqRep));
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, EndSessionErrorVolReq) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<VolumeRequest> csVol(volReq, vid, density);
  struct ClientSimSingleReply<VolumeRequest>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<VolumeRequest> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  ASSERT_THROW(cInterf.reportEndOfSessionWithError("SNAFU!", SEINTERNAL, reqRep),
  ClientInterface::UnexpectedResponse);
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, EndSessionErrorNotifAckSeqBreak) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<NotificationAcknowledge> csVol(volReq, vid, density, true);
  struct ClientSimSingleReply<NotificationAcknowledge>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<NotificationAcknowledge> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  ASSERT_THROW(cInterf.reportEndOfSessionWithError("SNAFU!", SEINTERNAL, reqRep),
  ClientInterface::UnexpectedResponse);
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, FilesToMigrateReqFilesToMigrate) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<FilesToMigrateList> csVol(volReq, vid, density);
  struct ClientSimSingleReply<FilesToMigrateList>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<FilesToMigrateList> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  // We are responsible for the freeing of the result: chuck it into an auto_ptr.
  std::auto_ptr<FilesToMigrateList> resp;
  ASSERT_NO_THROW(resp.reset(cInterf.getFilesToMigrate(10, 10, reqRep)));
  ASSERT_NE((FilesToMigrateList*)NULL, resp.get());
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, FilesToMigrateReqNoMore) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<NoMoreFiles> csVol(volReq, vid, density);
  struct ClientSimSingleReply<NoMoreFiles>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<NoMoreFiles> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  // We are responsible for the freeing of the result: chuck it into an auto_ptr.
  std::auto_ptr<FilesToMigrateList> resp;
  ASSERT_NO_THROW(resp.reset(cInterf.getFilesToMigrate(10, 10, reqRep)));
  ASSERT_EQ((FilesToMigrateList*)NULL, resp.get());
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, FilesToMigrateReqFilesToMigrateSeqBreak) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<FilesToMigrateList> csVol(volReq, vid, density, true);
  struct ClientSimSingleReply<FilesToMigrateList>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<FilesToMigrateList> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  // We are responsible for the freeing of the result: chuck it into an auto_ptr.
  std::auto_ptr<FilesToMigrateList> resp;
  ASSERT_THROW(resp.reset(cInterf.getFilesToMigrate(10, 10, reqRep)),ClientInterface::UnexpectedResponse);
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, MigrationReportNotifAck) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<NotificationAcknowledge> csVol(volReq, vid, density);
  struct ClientSimSingleReply<NotificationAcknowledge>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<NotificationAcknowledge> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  // We have to create the migration report
  FileMigrationReportList migRep;
  ASSERT_NO_THROW(cInterf.reportMigrationResults(migRep, reqRep));
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, MigrationReportEndNotifError) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<EndNotificationErrorReport> csVol(volReq, vid, density);
  struct ClientSimSingleReply<EndNotificationErrorReport>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<EndNotificationErrorReport> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  // We have to create the migration report
  FileMigrationReportList migRep;
  ASSERT_THROW(cInterf.reportMigrationResults(migRep, reqRep), castor::tape::Exception);
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, MigrationReportNotifAckSeqBreak) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<NotificationAcknowledge> csVol(volReq, vid, density, true);
  struct ClientSimSingleReply<NotificationAcknowledge>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<NotificationAcknowledge> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  // We have to create the migration report
  FileMigrationReportList migRep;
  ASSERT_THROW(cInterf.reportMigrationResults(migRep, reqRep),
    ClientInterface::UnexpectedResponse);
  // Cleanup
  csVolRun.wait();
}


TEST(tapeServerClientInterface, FilesToRecallReqFilesToMigrate) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<FilesToRecallList> csVol(volReq, vid, density);
  struct ClientSimSingleReply<FilesToRecallList>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<FilesToRecallList> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  // We are responsible for the freeing of the result: chuck it into an auto_ptr.
  std::auto_ptr<FilesToRecallList> resp;
  ASSERT_NO_THROW(resp.reset(cInterf.getFilesToRecall(10, 10, reqRep)));
  ASSERT_NE((FilesToRecallList*)NULL, resp.get());
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, FilesToRecallReqNoMore) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<NoMoreFiles> csVol(volReq, vid, density);
  struct ClientSimSingleReply<NoMoreFiles>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<NoMoreFiles> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  // We are responsible for the freeing of the result: chuck it into an auto_ptr.
  std::auto_ptr<FilesToRecallList> resp;
  ASSERT_NO_THROW(resp.reset(cInterf.getFilesToRecall(10, 10, reqRep)));
  ASSERT_EQ((FilesToRecallList*)NULL, resp.get());
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, FilesToRecallReqFilesToMigrateSeqBreak) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<FilesToRecallList> csVol(volReq, vid, density, true);
  struct ClientSimSingleReply<FilesToRecallList>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<FilesToRecallList> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  // We are responsible for the freeing of the result: chuck it into an auto_ptr.
  std::auto_ptr<FilesToRecallList> resp;
  ASSERT_THROW(resp.reset(cInterf.getFilesToRecall(10, 10, reqRep)),ClientInterface::UnexpectedResponse);
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, RecallReportNotifAck) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<NotificationAcknowledge> csVol(volReq, vid, density);
  struct ClientSimSingleReply<NotificationAcknowledge>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<NotificationAcknowledge> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  // We have to create the migration report
  FileRecallReportList recallRep;
  ASSERT_NO_THROW(cInterf.reportRecallResults(recallRep, reqRep));
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, RecallReportEndNotifError) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<EndNotificationErrorReport> csVol(volReq, vid, density);
  struct ClientSimSingleReply<EndNotificationErrorReport>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<EndNotificationErrorReport> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  // We have to create the migration report
  FileRecallReportList recallRep;
  ASSERT_THROW(cInterf.reportRecallResults(recallRep, reqRep), castor::tape::Exception);
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, RecallReportNotifAckSeqBreak) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  ClientSimSingleReply<NotificationAcknowledge> csVol(volReq, vid, density, true);
  struct ClientSimSingleReply<NotificationAcknowledge>::ipPort clientAddr = 
  csVol.getCallbackAddress();
  clientSingleReplRunner<NotificationAcknowledge> csVolRun(csVol);
  csVolRun.start();
  // Setup a clientInterface to talk to it
  castor::tape::legacymsg::RtcpJobRqstMsgBody VDQMjob;
  snprintf(VDQMjob.clientHost, CA_MAXHOSTNAMELEN+1, "%d.%d.%d.%d",
    clientAddr.a, clientAddr.b, clientAddr.c, clientAddr.d);
  snprintf(VDQMjob.driveUnit, CA_MAXUNMLEN+1, "T10D6116");
  snprintf(VDQMjob.dgn, CA_MAXDGNLEN+1, "LIBXX");
  VDQMjob.clientPort = clientAddr.port;
  VDQMjob.volReqId = volReq;
  // Setup an interface to it.
  ClientInterface cInterf(VDQMjob);
  ClientInterface::VolumeInfo volInfo;
  ClientInterface::RequestReport reqRep;
  // We have to create the migration report
  FileRecallReportList recallRep;
  ASSERT_THROW(cInterf.reportRecallResults(recallRep, reqRep),
    ClientInterface::UnexpectedResponse);
  // Cleanup
  csVolRun.wait();
}

}

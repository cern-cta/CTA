/******************************************************************************
 *                      ClientInterfaceTest.cpp
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
#include "ClientProxy.hpp"
#include "../threading/Threading.hpp"
#include "castor/log/StringLogger.hpp"
#include "castor/tape/tapeserver/daemon/MountSession.hpp"
#include "../system/Wrapper.hpp"
#include "Ctape.h"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"

using namespace castor::tape::tapeserver;
using namespace castor::tape::tapeserver::daemon;

namespace unitTest {
  using namespace castor::tape::tapeserver::client;
template <class Repl>
class clientSingleReplRunner: public castor::tape::threading::Thread {
public:
  clientSingleReplRunner(client::ClientSimSingleReply<Repl> &client): m_sim(client) {}
private:
  void run() {
    m_sim.sessionLoop();
  }
  client::ClientSimSingleReply<Repl> & m_sim;
};

TEST(tapeServerClientInterface, VolReqVol) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  client::ClientSimSingleReply<Volume> csVol(volReq, vid, density);
  client::ClientSimSingleReply<Volume>::ipPort clientAddr = csVol.getCallbackAddress();
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
  client::ClientProxy cInterf(VDQMjob);
  client::ClientProxy::VolumeInfo volInfo;
  client::ClientProxy::RequestReport reqRep;
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
  client::ClientSimSingleReply<NoMoreFiles> csVol(volReq, vid, density);
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
  ClientProxy cInterf(VDQMjob);
  ClientProxy::VolumeInfo volInfo;
  ClientProxy::RequestReport reqRep;
  ASSERT_THROW(cInterf.fetchVolumeId(volInfo, reqRep), ClientProxy::EndOfSession);
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
  ClientProxy cInterf(VDQMjob);
  ClientProxy::VolumeInfo volInfo;
  ClientProxy::RequestReport reqRep;
  ASSERT_THROW(cInterf.fetchVolumeId(volInfo, reqRep), ClientProxy::EndOfSession);
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
  ClientProxy cInterf(VDQMjob);
  ClientProxy::VolumeInfo volInfo;
  ClientProxy::RequestReport reqRep;
  ASSERT_THROW(cInterf.fetchVolumeId(volInfo, reqRep), ClientProxy::UnexpectedResponse);
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
  ClientProxy cInterf(VDQMjob);
  ClientProxy::VolumeInfo volInfo;
  ClientProxy::RequestReport reqRep;
  ASSERT_THROW(cInterf.fetchVolumeId(volInfo, reqRep), ClientProxy::UnexpectedResponse);
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
  ClientProxy cInterf(VDQMjob);
  ClientProxy::VolumeInfo volInfo;
  ClientProxy::RequestReport reqRep;
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
  ClientProxy cInterf(VDQMjob);
  ClientProxy::VolumeInfo volInfo;
  ClientProxy::RequestReport reqRep;
  ASSERT_THROW(cInterf.reportEndOfSession(reqRep), ClientProxy::UnexpectedResponse);
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
  ClientProxy cInterf(VDQMjob);
  ClientProxy::VolumeInfo volInfo;
  ClientProxy::RequestReport reqRep;
  ASSERT_THROW(cInterf.reportEndOfSession(reqRep), ClientProxy::UnexpectedResponse);
  // Cleanup
  csVolRun.wait();
}

TEST(tapeServerClientInterface, EndSessionErrorNotifAck) {
  using namespace castor::tape::tapegateway;
  // Create the "client" that will just reply a scripted reply
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  std::string density = "8000GC";
  client::ClientSimSingleReply<NotificationAcknowledge> csVol(volReq, vid, density);
  client::ClientSimSingleReply<NotificationAcknowledge>::ipPort clientAddr = 
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
  ClientProxy cInterf(VDQMjob);
  ClientProxy::VolumeInfo volInfo;
  ClientProxy::RequestReport reqRep;
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
  ClientProxy cInterf(VDQMjob);
  ClientProxy::VolumeInfo volInfo;
  ClientProxy::RequestReport reqRep;
  ASSERT_THROW(cInterf.reportEndOfSessionWithError("SNAFU!", SEINTERNAL, reqRep),
  ClientProxy::UnexpectedResponse);
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
  ClientProxy cInterf(VDQMjob);
  ClientProxy::VolumeInfo volInfo;
  ClientProxy::RequestReport reqRep;
  ASSERT_THROW(cInterf.reportEndOfSessionWithError("SNAFU!", SEINTERNAL, reqRep),
  ClientProxy::UnexpectedResponse);
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
  ClientProxy cInterf(VDQMjob);
  ClientProxy::VolumeInfo volInfo;
  ClientProxy::RequestReport reqRep;
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
  ClientProxy cInterf(VDQMjob);
  ClientProxy::VolumeInfo volInfo;
  ClientProxy::RequestReport reqRep;
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
  ClientProxy cInterf(VDQMjob);
  ClientProxy::VolumeInfo volInfo;
  ClientProxy::RequestReport reqRep;
  // We are responsible for the freeing of the result: chuck it into an auto_ptr.
  std::auto_ptr<FilesToMigrateList> resp;
  ASSERT_THROW(resp.reset(cInterf.getFilesToMigrate(10, 10, reqRep)),ClientProxy::UnexpectedResponse);
  // Cleanup
  csVolRun.wait();
}

}

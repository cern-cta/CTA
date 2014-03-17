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

}

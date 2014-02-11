/******************************************************************************
 *                      tapeserverdTest.hpp
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
#include "clientSimulator.hpp"
#include "tapeSession.hpp"
#include "../threading/Threading.hpp"

using namespace castor::tape::server;

namespace unitTest {

class clientRunner: public castor::tape::threading::Thread {
public:
  clientRunner(clientSimulator &client): m_sim(client) {}
private:
  void run() {
    m_sim.sessionLoop();
  }
  clientSimulator & m_sim;
};

  
TEST(tapeServer, simpleGetVol) {
  // TpcpClients only supports 32 bits session number
  // This number has to be less than 2^31 as in addition there is a mix
  // of signed and unsigned numbers
  // As the current ids in prod are ~30M, we are far from overflow (Feb 2013)
  uint32_t volReq = 0xBEEF;
  std::string vid = "V12345";
  clientSimulator sim(volReq, vid);
  struct clientSimulator::ipPort clientAddr = sim.getCallbackAddress();
  clientRunner simRun(sim);
  simRun.start();
  tapeSession sess(clientAddr.ip, clientAddr.port, volReq);
  sess.getVolume();
  simRun.wait();
  ASSERT_EQ("V12345", sess.m_vid);
}

}

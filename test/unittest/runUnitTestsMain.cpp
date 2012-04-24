/******************************************************************************
 *                test/castor/tape/tapebridge/runUnitTestsMain.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "test/unittest/castor/tape/net/NetTest.hpp"
#include "test/unittest/castor/tape/tapebridge/BridgeClientInfo2SenderTest.hpp"
#include "test/unittest/castor/tape/tapebridge/BridgeProtocolEngineTest.hpp"
#include "test/unittest/castor/tape/tapebridge/BridgeSocketCatalogueTest.hpp"
#include "test/unittest/castor/tape/tapebridge/BulkRequestConfigParamsTest.hpp"
#include "test/unittest/castor/tape/tapebridge/ClientAddressLocalTest.hpp"
#include "test/unittest/castor/tape/tapebridge/ClientAddressTcpIpTest.hpp"
#include "test/unittest/castor/tape/tapebridge/ClientProxyTest.hpp"
#include "test/unittest/castor/tape/tapebridge/ConfigParamTest.hpp"
#include "test/unittest/castor/tape/tapebridge/FileToMigrateTest.hpp"
#include "test/unittest/castor/tape/tapebridge/FileToRecallTest.hpp"
#include "test/unittest/castor/tape/tapebridge/GetMoreWorkConnectionTest.hpp"
#include "test/unittest/castor/tape/tapebridge/MigrationReportConnectionTest.hpp"
#include "test/unittest/castor/tape/tapebridge/TapeBridgeDaemonTest.hpp"
#include "test/unittest/castor/tape/tapebridge/TapeFlushConfigParamsTest.hpp"
#include "test/unittest/castor/tape/tapebridge/PendingMigrationsStoreTest.hpp"
#include "test/unittest/castor/tape/tapebridge/SessionErrorTest.hpp"
#include "test/unittest/castor/tape/tapebridge/SessionErrorListTest.hpp"
#include "test/unittest/castor/tape/tapebridge/VdqmRequestHandlerTest.hpp"
#include "test/unittest/castor/tape/utils/IndexedContainerTest.hpp"
#include "test/unittest/castor/tape/utils/UtilsTest.hpp"
#include "test/unittest/rtcopy/RtcopyTest.hpp"
#include "test/unittest/tapebridge/MarshallTapeBridgeClientInfo2Test.hpp"
#include "test/unittest/tapebridge/MarshallTapeBridgeFlushedToTapeTest.hpp"
#include "test/unittest/tapebridge/RecvTapeBridgeFlushedToTapeAckTest.hpp"
#include "test/unittest/tapebridge/SendTapeBridgeFlushedToTapeTest.hpp"
#include "castor/tape/utils/SmartFILEPtr.hpp"
#include "h/net.h"

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/ui/text/TestRunner.h>
#include <stdio.h>

extern "C" int rtcp_InitLog (char *, FILE *, FILE *, SOCKET *);

int main() {
  char rtcpLogErrTxt[1024];
  rtcp_InitLog(rtcpLogErrTxt,NULL,stderr,NULL);

  castor::tape::utils::SmartFILEPtr stdinFd(stdin);
  castor::tape::utils::SmartFILEPtr stdoutFd(stdout);
  castor::tape::utils::SmartFILEPtr stderrFd(stderr);

  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry =
    CppUnit::TestFactoryRegistry::getRegistry();

  runner.addTest(registry.makeTest());
  const int runnerRc = runner.run();

  return runnerRc == 0 ? 1 : 0;
}

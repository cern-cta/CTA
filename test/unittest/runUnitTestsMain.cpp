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

#include "castor/tape/utils/SmartFILEPtr.hpp"
#include "castor/BaseObject.hpp"
#include "castor/Services.hpp"
#include "h/Castor_limits.h"
#include "h/net.h"

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/ui/text/TestRunner.h>
#include <stdio.h>

extern "C" int rtcp_InitLog (char *, FILE *, FILE *, int *);

extern "C" int rpfd;
int rpfd = -1;
extern "C" int jid;
int jid = 0;
extern "C" char devtype[CA_MAXDVTLEN+1];
char devtype[CA_MAXDVTLEN+1] = "DEVTYPE";

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

  delete castor::BaseObject::services();

  return runnerRc == 0 ? 1 : 0;
}

/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <gtest/gtest.h>

#include "ProcessManager.hpp"
#include "TestSubprocessHandlers.hpp"

namespace unitTests {

TEST(cta_Daemon, ProcessManager) {
  cta::tape::daemon::ProcessManager pm;
  {
    std::unique_ptr<EchoSubprocess> es(new EchoSubprocess("Echo subprocess", pm));
    // downcast pointer
    std::unique_ptr<cta::tape::daemon::SubprocessHandler> sph = std::move(es);
    pm.addHandler(std::move(sph));
    pm.run();
  }
  EchoSubprocess & es = dynamic_cast<EchoSubprocess&>(pm.at("Echo subprocess"));
  ASSERT_TRUE(es.echoReceived());
}


// TODO: process manager managing several handlers.

} //namespace unitTests
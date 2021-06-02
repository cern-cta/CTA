/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <gtest/gtest.h>

#include "common/SourcedParameter.hpp"
#include "tapeserver/daemon/FetchReportOrFlushLimits.hpp"

namespace unitTests {

TEST(cta_Daemon, SourcedParameter) {
  cta::SourcedParameter<cta::tape::daemon::FetchReportOrFlushLimits> mountCriteria("unitTest", "mountCriteria");
  mountCriteria.set("12, 34", "Unit test");
  ASSERT_EQ(12, mountCriteria.value().maxBytes);
  ASSERT_EQ(34, mountCriteria.value().maxFiles);
}

} // namespace unitTests

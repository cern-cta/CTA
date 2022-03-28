/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "StringLogger.hpp"

#include <gtest/gtest.h>

using namespace cta::log;

namespace unitTests {
  TEST(cta_log_StringLogger, basicTest) {
    std::string jat = "Just a test";
    StringLogger sl("dummy", "cta_log_StringLogger", DEBUG);
    sl(INFO, jat);
    ASSERT_NE(std::string::npos, sl.getLog().find(jat));
  }
}

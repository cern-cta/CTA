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

#include "FileLogger.hpp"
#include "tests/TempFile.hpp"

#include <gtest/gtest.h>
#include <fstream>
#include <sstream>

using namespace cta::log;

namespace unitTests {
  TEST(cta_log_FileLogger, basicTest) {
    std::string jat = "Just a test";
    TempFile tf;
    FileLogger fl("dummy", "cta_log_StringLogger", tf.path(), DEBUG);
    fl(INFO, jat);
    std::ifstream ifs(tf.path());
    std::stringstream res;
    res << ifs.rdbuf();
    ASSERT_NE(std::string::npos, res.str().find(jat));
  }
}

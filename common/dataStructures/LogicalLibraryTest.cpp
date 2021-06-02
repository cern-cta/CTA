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

#include "common/dataStructures/LogicalLibrary.hpp"

#include <gtest/gtest.h>
#include <algorithm>

namespace unitTests {

class cta_common_dataStructures_LogicalLibraryTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_common_dataStructures_LogicalLibraryTest, constructor) {
  using namespace cta::common::dataStructures;

  LogicalLibrary logicalLibrary;
  ASSERT_FALSE(logicalLibrary.isDisabled);
}

} // namespace unitTests

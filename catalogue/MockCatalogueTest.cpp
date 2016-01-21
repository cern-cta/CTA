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

#include "catalogue/MockCatalogue.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_MockCatalogueTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_MockCatalogueTest, getNextArchiveFileId) {
  using namespace cta;

  MockCatalogue catalogue;

  const uint64_t archiveFileId1 = catalogue.getNextArchiveFileId();
  const uint64_t archiveFileId2 = catalogue.getNextArchiveFileId();

  ASSERT_GT(archiveFileId2, archiveFileId1);
}

} // namespace unitTests

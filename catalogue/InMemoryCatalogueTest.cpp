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

#include "catalogue/InMemoryCatalogue.hpp"
#include "common/log/DummyLogger.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_catalogue_InMemoryCatalogue : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_catalogue_InMemoryCatalogue, createSameSchemaInTwoSeparateInMemoryDatabases) {
  using namespace cta;

  log::DummyLogger dummyLog("dummy");
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 0;

  // First in-memory database
  {
    catalogue::InMemoryCatalogue inMemoryCatalogue(dummyLog, nbConns, nbArchiveFileListingConns);
  }

  // Second in-memory database
  {
    catalogue::InMemoryCatalogue inMemoryCatalogue(dummyLog, nbConns, nbArchiveFileListingConns);
  }
}

} // namespace unitTests

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

#include "catalogue/InMemoryCatalogue.hpp"
#include "catalogue/rdbms/RdbmsSchemaCatalogue.hpp"
#include "common/log/DummyLogger.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_catalogue_InMemoryCatalogue : public ::testing::Test {
protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(cta_catalogue_InMemoryCatalogue, createSameSchemaInTwoSeparateInMemoryDatabases) {
  using namespace cta;

  log::DummyLogger dummyLog("dummy", "dummy");
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 0;

  // First in-memory database
  { catalogue::InMemoryCatalogue inMemoryCatalogue(dummyLog, nbConns, nbArchiveFileListingConns); }

  // Second in-memory database
  { catalogue::InMemoryCatalogue inMemoryCatalogue(dummyLog, nbConns, nbArchiveFileListingConns); }
}

TEST_F(cta_catalogue_InMemoryCatalogue, schemaTables) {
  using namespace cta;

  log::DummyLogger dummyLog("dummy", "dummy");
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 0;

  catalogue::InMemoryCatalogue inMemoryCatalogue(dummyLog, nbConns, nbArchiveFileListingConns);
  const auto tableNameList =
    static_cast<catalogue::RdbmsSchemaCatalogue*>(inMemoryCatalogue.Schema().get())->getTableNames();
  std::set<std::string> tableNameSet;

  std::map<std::string, uint32_t> tableNameToListPos;
  uint32_t listPos = 0;
  for (auto& tableName : tableNameList) {
    ASSERT_EQ(tableNameToListPos.end(), tableNameToListPos.find(tableName));
    tableNameToListPos[tableName] = listPos++;
  }

  ASSERT_NE(tableNameToListPos.end(), tableNameToListPos.find("ADMIN_USER"));
  ASSERT_NE(tableNameToListPos.end(), tableNameToListPos.find("ARCHIVE_FILE"));
  ASSERT_NE(tableNameToListPos.end(), tableNameToListPos.find("ARCHIVE_ROUTE"));
  ASSERT_NE(tableNameToListPos.end(), tableNameToListPos.find("CTA_CATALOGUE"));
  ASSERT_NE(tableNameToListPos.end(), tableNameToListPos.find("LOGICAL_LIBRARY"));
  ASSERT_NE(tableNameToListPos.end(), tableNameToListPos.find("MOUNT_POLICY"));
  ASSERT_NE(tableNameToListPos.end(), tableNameToListPos.find("REQUESTER_GROUP_MOUNT_RULE"));
  ASSERT_NE(tableNameToListPos.end(), tableNameToListPos.find("REQUESTER_MOUNT_RULE"));
  ASSERT_NE(tableNameToListPos.end(), tableNameToListPos.find("STORAGE_CLASS"));
  ASSERT_NE(tableNameToListPos.end(), tableNameToListPos.find("TAPE"));
  ASSERT_NE(tableNameToListPos.end(), tableNameToListPos.find("TAPE_FILE"));
  ASSERT_NE(tableNameToListPos.end(), tableNameToListPos.find("TAPE_POOL"));
}

}  // namespace unitTests

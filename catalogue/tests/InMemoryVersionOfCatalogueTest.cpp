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

#include "catalogue/InMemoryCatalogueFactory.hpp"
#include "catalogue/tests/modules/AdminUserCatalogueTest.hpp"
#include "catalogue/tests/modules/ArchiveFileCatalogueTest.hpp"
#include "catalogue/tests/modules/ArchiveRouteCatalogueTest.hpp"
#include "catalogue/tests/modules/DiskInstanceCatalogueTest.hpp"
#include "catalogue/tests/modules/DiskInstanceSpaceCatalogueTest.hpp"
#include "catalogue/tests/modules/DiskSystemCatalogueTest.hpp"
#include "catalogue/tests/modules/DriveConfigCatalogueTest.hpp"
#include "catalogue/tests/modules/DriveStateCatalogueTest.hpp"
#include "catalogue/tests/modules/FileRecycleLogCatalogueTest.hpp"
#include "catalogue/tests/modules/LogicalLibraryCatalogueTest.hpp"
#include "catalogue/tests/modules/MediaTypeCatalogueTest.hpp"
#include "catalogue/tests/modules/MountPolicyCatalogueTest.hpp"
#include "catalogue/tests/modules/RequesterActivityMountRuleTest.hpp"
#include "catalogue/tests/modules/RequesterGroupMountRuleCatalogueTest.hpp"
#include "catalogue/tests/modules/RequesterMountRuleTest.hpp"
#include "catalogue/tests/modules/SchemaCatalogueTest.hpp"
#include "catalogue/tests/modules/StorageClassCatalogueTest.hpp"
#include "catalogue/tests/modules/TapeCatalogueTest.hpp"
#include "catalogue/tests/modules/TapeFileCatalogueTest.hpp"
#include "catalogue/tests/modules/TapePoolCatalogueTest.hpp"
#include "catalogue/tests/modules/VirtualOrganizationCatalogueTest.hpp"
#ifdef STDOUT_LOGGING
#include "common/log/StdoutLogger.hpp"
#else
#include "common/log/DummyLogger.hpp"
#endif

namespace unitTests {

namespace {

const uint64_t g_in_memory_CatalogueTest_nbConn = 1;
const uint64_t g_in_memory_nbArchiveFileListingConns = 1;
const uint64_t g_in_memory_maxTriesToConnect = 1;
#ifdef STDOUT_LOGGING
cta::log::StdoutLogger g_in_memory_CatalogueTest_dummyLogger("stdout", "stdout");
#else
cta::log::DummyLogger g_in_memory_CatalogueTest_dummyLogger("dummy", "dummy");
#endif

cta::catalogue::InMemoryCatalogueFactory g_inMemoryCatalogueFactory(g_in_memory_CatalogueTest_dummyLogger,
  g_in_memory_CatalogueTest_nbConn, g_in_memory_nbArchiveFileListingConns, g_in_memory_maxTriesToConnect);

cta::catalogue::CatalogueFactory *g_inMemoryCatalogueFactoryPtr = &g_inMemoryCatalogueFactory;

}  // anonymous namespace

INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_SchemaTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_AdminUserTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_DiskSystemTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_DiskInstanceTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_DiskInstanceSpaceTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_VirtualOrganizationTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_ArchiveRouteTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_MediaTypeTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_StorageClassTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_TapePoolTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_TapeTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_MountPolicyTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_RequesterActivityMountRuleTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_RequesterMountRuleTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_RequesterGroupMountRuleTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_LogicalLibraryTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_TapeFileTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_FileRecycleLogTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_DriveConfigTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_DriveStateTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));
INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_ArchiveFileTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));

}  // namespace unitTests

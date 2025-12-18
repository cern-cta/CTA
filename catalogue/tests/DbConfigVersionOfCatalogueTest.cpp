/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

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
#include "catalogue/tests/modules/PhysicalLibraryCatalogueTest.hpp"
#include "catalogue/tests/modules/RequesterActivityMountRuleTest.hpp"
#include "catalogue/tests/modules/RequesterGroupMountRuleCatalogueTest.hpp"
#include "catalogue/tests/modules/RequesterMountRuleTest.hpp"
#include "catalogue/tests/modules/SchemaCatalogueTest.hpp"
#include "catalogue/tests/modules/StorageClassCatalogueTest.hpp"
#include "catalogue/tests/modules/TapeCatalogueTest.hpp"
#include "catalogue/tests/modules/TapeFileCatalogueTest.hpp"
#include "catalogue/tests/modules/TapePoolCatalogueTest.hpp"
#include "catalogue/tests/modules/VirtualOrganizationCatalogueTest.hpp"
#include "tests/GlobalCatalogueFactoryForUnitTests.hpp"

namespace unitTests {

INSTANTIATE_TEST_CASE_P(DbConfigFile, cta_catalogue_SchemaTest, ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile, cta_catalogue_AdminUserTest, ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile, cta_catalogue_DiskSystemTest, ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile,
                        cta_catalogue_DiskInstanceTest,
                        ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile,
                        cta_catalogue_DiskInstanceSpaceTest,
                        ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile,
                        cta_catalogue_VirtualOrganizationTest,
                        ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile,
                        cta_catalogue_ArchiveRouteTest,
                        ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile, cta_catalogue_MediaTypeTest, ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile,
                        cta_catalogue_StorageClassTest,
                        ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile, cta_catalogue_TapePoolTest, ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile, cta_catalogue_TapeTest, ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile,
                        cta_catalogue_MountPolicyTest,
                        ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile,
                        cta_catalogue_RequesterActivityMountRuleTest,
                        ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile,
                        cta_catalogue_RequesterMountRuleTest,
                        ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile,
                        cta_catalogue_RequesterGroupMountRuleTest,
                        ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile,
                        cta_catalogue_LogicalLibraryTest,
                        ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile,
                        cta_catalogue_PhysicalLibraryTest,
                        ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile, cta_catalogue_TapeFileTest, ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile,
                        cta_catalogue_FileRecycleLogTest,
                        ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile,
                        cta_catalogue_DriveConfigTest,
                        ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile, cta_catalogue_DriveStateTest, ::testing::Values(&g_catalogueFactoryForUnitTests));
INSTANTIATE_TEST_CASE_P(DbConfigFile,
                        cta_catalogue_ArchiveFileTest,
                        ::testing::Values(&g_catalogueFactoryForUnitTests));

}  // namespace unitTests

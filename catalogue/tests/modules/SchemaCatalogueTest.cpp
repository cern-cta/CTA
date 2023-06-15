/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <gtest/gtest.h>

#include "catalogue/Catalogue.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/SchemaCatalogueTest.hpp"
#include "common/Constants.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_SchemaTest::cta_catalogue_SchemaTest() : m_dummyLog("dummy", "dummy") {}

void cta_catalogue_SchemaTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_SchemaTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_SchemaTest, getSchemaVersion) {
  const auto schemaDbVersion = m_catalogue->Schema()->getSchemaVersion();
  ASSERT_EQ(static_cast<uint64_t>(CTA_CATALOGUE_SCHEMA_VERSION_MAJOR),
            schemaDbVersion.getSchemaVersion<cta::catalogue::SchemaVersion::MajorMinor>().first);
  ASSERT_EQ(static_cast<uint64_t>(CTA_CATALOGUE_SCHEMA_VERSION_MINOR),
            schemaDbVersion.getSchemaVersion<cta::catalogue::SchemaVersion::MajorMinor>().second);
}

TEST_P(cta_catalogue_SchemaTest, ping) {
  m_catalogue->Schema()->ping();
}

}  // namespace unitTests
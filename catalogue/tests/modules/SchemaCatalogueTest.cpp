/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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

cta_catalogue_SchemaTest::cta_catalogue_SchemaTest()
  : m_dummyLog("dummy", "dummy") {
}

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

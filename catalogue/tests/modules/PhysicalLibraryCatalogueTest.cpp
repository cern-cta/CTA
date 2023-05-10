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
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/PhysicalLibraryCatalogueTest.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/TapePool.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/PhysicalLibrary.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_PhysicalLibraryTest::cta_catalogue_PhysicalLibraryTest()
  : m_dummyLog("dummy", "dummy"),
    m_admin("admin", "admin", "admin", ""),
    m_physicalLibrary1(CatalogueTestUtils::getPhysicalLibrary1()),
    m_physicalLibrary2(CatalogueTestUtils::getPhysicalLibrary2()) {
}

void cta_catalogue_PhysicalLibraryTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_PhysicalLibraryTest::TearDown() {
  m_catalogue.reset();
}

}  // namespace unitTests

/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include "catalogue/CatalogueTest.hpp"
#include "catalogue/InMemoryCatalogueFactory.hpp"
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

} // anonymous namespace

INSTANTIATE_TEST_CASE_P(InMemory, cta_catalogue_CatalogueTest, ::testing::Values(&g_inMemoryCatalogueFactoryPtr));

} // namespace unitTests

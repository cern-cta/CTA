
/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ObjectStoreFixture.hpp"

#include "Helpers.hpp"

namespace unitTests {

void ObjectStore::SetUp() {
  // We need to cleanup the queue statistics cache before every test
  cta::objectstore::Helpers::flushStatisticsCache();
}

}  // namespace unitTests

/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBTestFactory.hpp"
#else
#include "objectstore/BackendVFS.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#endif

namespace cta::tape::daemon::testingUtils {

// Use the same isolated backend factories as TapeSessionTest, including its temporary PostgreSQL environment.
// The catalogue unique_ptr itself must stay in place: the object-store wrapper retains a reference to it.
inline std::unique_ptr<SchedulerDatabase> createSchedulerDatabase(std::unique_ptr<catalogue::Catalogue>& catalogue) {
#ifdef CTA_PGSCHED
  return RelationalDBTestFactory().create(catalogue);
#else
  return OStoreDBFactory<objectstore::BackendVFS>().create(catalogue);
#endif
}

}  // namespace cta::tape::daemon::testingUtils

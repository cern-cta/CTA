/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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

#pragma once

#include "common/config/Config.hpp"
#include "common/log/LogContext.hpp"

#include "catalogue/Catalogue.hpp"
#include "scheduler/Scheduler.hpp"

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

namespace cta::maintd {

/**
 * Responsible for create a RoutineRunner with a specific set of registered routines based on the provided config.
 */
class RoutineRunnerFactory {
public:
  RoutineRunnerFactory(const cta::common::Config& config, cta::log::LogContext& lc);
  std::unique_ptr<RoutineRunner> create();

private:
  const cta::common::Config& m_config;
  cta::log::LogContext& m_lc;

  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::unique_ptr<cta::SchedulerDBInit_t> m_schedDbInit;
  std::unique_ptr<cta::SchedulerDB_t> m_schedDb;
  std::unique_ptr<cta::Scheduler> m_scheduler;
};

}  // namespace cta::maintd

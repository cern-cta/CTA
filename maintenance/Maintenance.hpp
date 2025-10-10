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

#include "IMaintenanceRunner.hpp"
#include "common/config/Config.hpp"
#include "common/exception/Exception.hpp"
#include "common/process/SignalHandler.hpp"
#include "common/log/LogContext.hpp"
#include "scheduler/Scheduler.hpp"
#include "catalogue/Catalogue.hpp"

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

namespace cta::maintenance {

CTA_GENERATE_EXCEPTION_CLASS(InvalidConfiguration);

/**
 * Handler for garbage collector subprocesses. This long lived process should live
 * as long as the main process, but will be respawned in case of crash.
 */
class Maintenance {
public:
  Maintenance(cta::log::LogContext& lc, const cta::common::Config& config);

  ~Maintenance() = default;

  uint32_t run();

private:
  std::list<std::unique_ptr<IMaintenanceRunner>> m_maintenanceRunners;
  cta::log::LogContext& m_lc;

  std::unique_ptr<SchedulerDBInit_t> m_schedDbInit;
  std::unique_ptr<cta::Scheduler> m_scheduler;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::unique_ptr<cta::SchedulerDB_t> m_scheddb;

  std::unique_ptr<cta::SignalHandler> m_signalHandler = std::make_unique<cta::SignalHandler>();

  int m_sleepInterval;
};

}  // namespace cta::maintenance

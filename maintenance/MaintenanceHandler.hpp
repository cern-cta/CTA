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

#include "DiskReportRunner.hpp"
#include "configuration/MaintenanceConfiguration.hpp"

namespace cta::maintenance {

/**
 * Handler for garbage collector subprocesses. This long lived process should live
 * as long as the main process, but will be respawned in case of crash.
 */
class MaintenanceServer {
public:
  MaintenanceServer(const common::MaintenanceConfiguration & maintenanceConfig);
  ~MaintenanceServer();

  // Loop mode
  void maintenanceLoop(bool mode);
private:
  std::unique_ptr<cta::catalogue::Catalogue> catalogue;
  std::unique_ptr<cta::SchedulerDB_t> sched_db;
  std::unique_ptr<cta::Scheduler> scheduler;

  std::list<MaintenanceTask>


  /** The poll period for the garbage collector */
  static const time_t s_pollInterval = 10;
};

} // namespace cta::maintenance

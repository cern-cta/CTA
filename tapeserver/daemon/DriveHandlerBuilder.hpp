/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2023 CERN
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

#include <memory>

namespace cta {

class Scheduler;

namespace catalogue {
class Catalogue;
}

namespace tape {
namespace daemon {

class DriveHandler;
class TapedConfiguration;
class TpconfigLine;
class ProcessManager;

class DriveHandlerBuilder {

public:
  DriveHandlerBuilder(const TapedConfiguration* tapedConfig, const TpconfigLine* driveConfig, ProcessManager* pm);

  ~DriveHandlerBuilder() = default;

  std::unique_ptr<DriveHandler> build();

private:
  const TapedConfiguration* m_tapedConfig;
  const TpconfigLine* m_driveConfig;
  ProcessManager* m_processManager;

  std::unique_ptr<cta::catalogue::Catalogue> createCatalogue();

  std::unique_ptr<Scheduler> createScheduler(cta::catalogue::Catalogue* catalogue);

  // std::unique_ptr<castor::tape::tapeserver::daemon::CleanerSession> createCleanerSession(
  //   const std::unique_ptr<Scheduler>& scheduler, cta::log::LogContext* lc);
};

} // namespace daemon
} // namespace tape
} // namespace cta

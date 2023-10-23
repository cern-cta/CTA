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

#include "tapeserver/daemon/DriveHandler.hpp"

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

class DriveHandlerBuilder : public DriveHandler {

public:
  DriveHandlerBuilder(const TapedConfiguration* tapedConfig, const TpconfigLine* driveConfig, ProcessManager* pm);

  ~DriveHandlerBuilder() override = default;
private:

  std::unique_ptr<cta::catalogue::Catalogue> createCatalogue(const std::string& methodCaller) const override;

  std::unique_ptr<cta::Scheduler> createScheduler(const std::string& prefixProcessName,
    const uint64_t minFilesToWarrantAMount, const uint64_t minBytesToWarrantAMount) override;

  castor::tape::tapeserver::daemon::Session::EndOfSessionAction executeCleanerSession(
    cta::Scheduler* scheduler) const override;

  castor::tape::tapeserver::daemon::Session::EndOfSessionAction executeDataTransferSession(
    cta::Scheduler* scheduler, cta::tape::daemon::DriveHandlerProxy* driveHandlerProxy) const override;
};

} // namespace daemon
} // namespace tape
} // namespace cta

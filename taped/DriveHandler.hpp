/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapedConfig.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/config/Config.hpp"
#include "common/log/LogContext.hpp"
#include "scheduler/Scheduler.hpp"
#include "system/Wrapper.hpp"

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

#include <stop_token>

namespace cta::tape::daemon {

class DriveHandler final {
public:
  DriveHandler(const TapedConfig& tapedConfig, cta::log::Logger& lc);

  ~DriveHandler() = default;

  void stop();

  int run();

  bool isLive() const;

  bool isReady() const;

private:
  void waitForDriveToBeUp();
  void putDriveDown(std::string_view errorMsg);
  bool executeDataTransferSession(std::unique_ptr<TapeMount> tapeMount);
  void executeCleanerSession() noexcept {};
  std::unique_ptr<TapeMount> getNextMount();

  std::stop_source m_stopSource;

  const TapedConfig& m_config;
  const common::dataStructures::DriveInfo m_driveInfo;
  log::LogContext m_lc;

  mediachanger::MediaChangerFacade m_mediaChanger;
  // TODO: maybe we can get away with not having this here
  System::realWrapper m_sysWrapper;
  std::unique_ptr<catalogue::Catalogue> m_catalogue;
  // TODO: can we settle with just catalogue and scheduler?
  std::unique_ptr<SchedulerDBInit_t> m_schedDbInit;
  std::unique_ptr<SchedulerDB_t> m_schedDb;
  std::unique_ptr<Scheduler> m_scheduler;
};

}  // namespace cta::tape::daemon

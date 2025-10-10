/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <atomic>
#include <memory>
#include <string>

#include "common/exception/Exception.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/TapeMount.hpp"

namespace cta {

namespace catalogue {
class Catalogue;
}

/**
  * The class driving a retrieve mount.
  * The class only has private constructors as it is instanciated by
  * the Scheduler class.
  */
class LabelMount: public TapeMount {
  friend class Scheduler;
protected:
  /**
    * Constructor
    */
  explicit LabelMount(catalogue::Catalogue & catalogue);

  /**
    * Constructor
    *
    * @param dbMount The database representation of this mount
    */
  LabelMount(catalogue::Catalogue & catalogue, std::unique_ptr<cta::SchedulerDatabase::LabelMount> dbMount);

public:
  CTA_GENERATE_EXCEPTION_CLASS(WrongMountType);
  CTA_GENERATE_EXCEPTION_CLASS(NotImplemented);

  /**
    * Destructor
    */
  ~LabelMount() final = default;

  /**
    * Returns The type of this tape mount.
    *
    * @return The type of this tape mount.
    */
  cta::common::dataStructures::MountType getMountType() const override;

  /**
    * Returns the volume identifier of the tape to be mounted.
    *
    * @return The volume identifier of the tape to be mounted.
    */
  std::string getVid() const override;

  /**
    * Returns the mount transaction id.
    *
    * @return The mount transaction id.
    */
  std::string getMountTransactionId() const override;

  /**
    * Return std::nullopt as activities are for retrieve mounts;
    *
    * @return std::nullopt.
    */
  std::optional<std::string> getActivity() const override { return std::nullopt; }


  /**
    * Indicates that the mount was completed.
    */
  void complete() override;

  /**
    * Report a drive status change
    */
  void setDriveStatus(cta::common::dataStructures::DriveStatus status, const std::optional<std::string> & reason = std::nullopt) override;

  /**
    * Report a tape session statistics
    */
  void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) override;

  /**
    * Report a tape mounted event
    * @param logContext
    */
  void setTapeMounted(log::LogContext &logContext) const override;

  CTA_GENERATE_EXCEPTION_CLASS(SessionNotRunning);

  /**
    * Returns the tape pool of the tape to be mounted.
    *
    * @return The tape pool of the tape to be mounted.
    */
  virtual std::string getPoolName() const;

  /**
    * Returns the mount transaction id.
    *
    * @return The mount transaction id.
    */
  uint32_t getNbFiles() const override;

protected:
  /**
    * The database representation of this mount.
    */
  std::unique_ptr<cta::SchedulerDatabase::ArchiveMount> m_dbMount;

  /**
    * A reference to the file catalogue.
    */
  catalogue::Catalogue & m_catalogue;

  /**
    * Internal tracking of the session completion
    */
  std::atomic<bool> m_sessionRunning;
};

} // namespace cta

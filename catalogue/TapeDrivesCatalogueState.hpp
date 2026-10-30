/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/DriveDownRequest.hpp"
#include "common/dataStructures/DriveReport.hpp"
#include "common/dataStructures/DriveStatus.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/exception/UserError.hpp"

#include <optional>
#include <string>
#include <string_view>

namespace cta {

namespace catalogue {
class Catalogue;
}

namespace log {
class LogContext;
}

namespace common::dataStructures {
class DesiredDriveState;
struct DriveInfo;
struct TapeDrive;
struct SecurityIdentity;
}  // namespace common::dataStructures

struct ReportDriveStatsInputs {
  time_t reportTime;
  uint64_t bytesTransferred;
  uint64_t filesTransferred;
};

/// Manage desired drive state (requested operation) and reported drive state (observed operation).
class TapeDrivesCatalogueState {
public:
  /// Bind to the catalogue without changing desired or reported drive state.
  explicit TapeDrivesCatalogueState(catalogue::Catalogue& catalogue);

  /// Destroy this helper without changing catalogue entries.
  ~TapeDrivesCatalogueState() = default;

  /// Create or replace the drive entry, initializing both desired and reported state.
  /// desiredState supplies the requested state; type and status supply the reported state.
  void createTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
                             const common::dataStructures::DesiredDriveState& desiredState,
                             const common::dataStructures::MountType& type,
                             const common::dataStructures::DriveStatus& status,
                             const common::dataStructures::SecurityIdentity& identity,
                             log::LogContext& lc) const;

  /// A drive with the same name belongs to a different host or logical library.
  CTA_GENERATE_EXCEPTION_CLASS(DriveAlreadyExistsException);

  /// The requested drive has no catalogue entry.
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchDrive);

  /// Check whether the drive name is unused or already belongs to this host and logical library.
  /// Does not modify desired or reported state.
  /// @throws DriveAlreadyExistsException if the existing entry belongs to another host or logical library.
  void checkDriveCanBeCreated(const cta::common::dataStructures::DriveInfo& driveInfo) const;

  /// Delete the entire drive entry, removing both desired and reported state.
  /// Catalogue exceptions of type cta::exception::Exception are logged and suppressed.
  void removeDrive(const std::string& drive, log::LogContext& lc) const;

  /// Read desired state, including its reason and user comment, without modifying reported state.
  /// @throws NoSuchDrive if the catalogue entry is absent.
  common::dataStructures::DesiredDriveState getDesiredDriveState(const std::string& driveName) const;

  /// Request Up and clear force-down; an empty reason clears the previous reason.
  void requestDriveUp(const std::string& driveName, std::string_view reason, log::LogContext& lc) const;

  /// Request Down without changing reported activity; reason policy is explicit.
  /// Preserving an existing specific reason is best effort: the read and update are not atomic.
  /// Requesting Down does not report that a session has ended; reporting Down does.
  /// @see reportDriveState()
  void requestDriveDown(const std::string& driveName,
                        const common::dataStructures::DriveDownRequest& request,
                        log::LogContext& lc) const;

  /// Update only the operator comment, without emitting an Up/Down event.
  void setDriveComment(const std::string& driveName, std::string_view comment, log::LogContext& lc) const;

  /// Update reported session transfer statistics and report timestamps.
  /// Does not change reported drive status or desired state.
  void updateDriveStatistics(const common::dataStructures::DriveInfo& driveInfo,
                             const ReportDriveStatsInputs& inputs,
                             log::LogContext& lc) const;

  /// Publish readiness and optional tape activity without modifying desired state.
  /// Depending on the reported state, this may reset various timers in the drive state table.
  void reportDriveState(const common::dataStructures::DriveInfo& driveInfo,
                        const common::dataStructures::DriveReport& report,
                        log::LogContext& lc) const;

private:
  struct ReportDriveStatusInputs;

  /// Compatibility encoding belongs here, not in scheduler or daemon callers.
  static std::optional<common::dataStructures::DriveStatus>
  mapToDriveStatus(const common::dataStructures::DriveReport& report);

  void updateDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
                         const ReportDriveStatusInputs& inputs,
                         log::LogContext& lc) const;

  void writeDesiredState(const std::string& driveName,
                         const common::dataStructures::DesiredDriveState& desiredState,
                         log::LogContext& lc,
                         bool logStateChange = true) const;

  cta::catalogue::Catalogue& m_catalogue;

  common::dataStructures::TapeDrive setTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
                                                       const common::dataStructures::DesiredDriveState& desiredState,
                                                       const common::dataStructures::MountType& type,
                                                       const common::dataStructures::DriveStatus& status,
                                                       const common::dataStructures::SecurityIdentity& identity) const;
  void setDriveDown(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveUp(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveStarting(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveMounting(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveTransferring(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveUnloading(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveUnmounting(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveDrainingToDisk(common::dataStructures::TapeDrive& driveState,
                              const ReportDriveStatusInputs& inputs) const;
};

}  // namespace cta

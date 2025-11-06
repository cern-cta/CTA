/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
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

#include <opentelemetry/context/runtime_context.h>

#include "DriveSessionTracker.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/TapedInstruments.hpp"
#include "common/dataStructures/TapeDrive.hpp"

namespace castor::tape::tapeserver::daemon {

using namespace cta;

//------------------------------------------------------------------------------
// Callback for observing metrics
//------------------------------------------------------------------------------
static void ObserveSessionState(opentelemetry::metrics::ObserverResult observer_result, void* state) noexcept {
  // Recover the object pointer
  auto* driveSessionTracker = static_cast<DriveSessionTracker*>(state);
  if (!driveSessionTracker) {
    return;
  }

  auto optionalTapeDrive = driveSessionTracker->queryTapeDrive();
  if(!optionalTapeDrive.has_value()) {
    return;
  }
  auto actualDriveStatus = optionalTapeDrive.value().driveStatus;
  auto actualMountType = optionalTapeDrive.value().mountType;

  if (std::holds_alternative<std::shared_ptr<opentelemetry::metrics::ObserverResultT<int64_t>>>(observer_result)) {
    auto typed_observer = std::get<std::shared_ptr<opentelemetry::metrics::ObserverResultT<int64_t>>>(observer_result);
    if (driveSessionTracker->hasState() && !driveSessionTracker->isSameState(actualDriveStatus, actualMountType)) {
      // If there was a state before and said state is not the same as the new one, we need to emit a 0 for that old state
      // and update it to the new state
      auto oldDriveStatus = driveSessionTracker->getDriveStatus().value();
      auto oldMountType = driveSessionTracker->getMountType().value();
      driveSessionTracker->setState(actualDriveStatus, actualMountType);
      typed_observer->Observe(
        0,
        {
          {semconv::attr::kCtaTapedSessionState, common::dataStructures::toString(oldDriveStatus)},
          {semconv::attr::kCtaTapedMountType,    common::dataStructures::toString(oldMountType)  }
      });
    }
    // Observe the new state
    typed_observer->Observe(
      1,
      {
        {semconv::attr::kCtaTapedSessionState, common::dataStructures::toString(actualDriveStatus)},
        {semconv::attr::kCtaTapedMountType,    common::dataStructures::toString(actualMountType)  }
    });
  }
}

DriveSessionTracker::DriveSessionTracker(std::shared_ptr<catalogue::Catalogue> catalogue, std::string_view driveName)
    : m_catalogue(catalogue),
      m_driveName(driveName) {
  telemetry::metrics::ctaTapedSessionStatus->AddCallback(ObserveSessionState, this);
}

DriveSessionTracker::~DriveSessionTracker() {
  telemetry::metrics::ctaTapedSessionStatus->RemoveCallback(ObserveSessionState, this);
}

std::optional<common::dataStructures::TapeDrive> DriveSessionTracker::queryTapeDrive() const {
  return m_catalogue->DriveState()->getTapeDrive(m_driveName);
}

bool DriveSessionTracker::isSameState(common::dataStructures::DriveStatus driveStatus,
                                      common::dataStructures::MountType mountType) const {
  if (!hasState()) {
    return false;
  }
  return driveStatus == m_driveStatus && mountType == m_mountType;
}

bool DriveSessionTracker::hasState() const {
  return m_driveStatus.has_value() && m_mountType.has_value();
}

void DriveSessionTracker::setState(common::dataStructures::DriveStatus driveStatus,
                                   common::dataStructures::MountType mountType) {
  m_driveStatus = driveStatus;
  m_mountType = mountType;
}

std::optional<common::dataStructures::DriveStatus> DriveSessionTracker::getDriveStatus() const {
  return m_driveStatus;
}

std::optional<common::dataStructures::MountType> DriveSessionTracker::getMountType() const {
  return m_mountType;
}

}  // namespace castor::tape::tapeserver::daemon

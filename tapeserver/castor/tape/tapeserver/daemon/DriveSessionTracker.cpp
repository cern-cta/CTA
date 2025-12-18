/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveSessionTracker.hpp"

#include "common/dataStructures/TapeDrive.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/TapedInstruments.hpp"

#include <opentelemetry/context/runtime_context.h>

namespace castor::tape::tapeserver::daemon {

using namespace cta;

//------------------------------------------------------------------------------
// Callback for observing metrics
//------------------------------------------------------------------------------
static void ObserveSessionState(opentelemetry::metrics::ObserverResult observer_result, void* state) noexcept {
  // Recover the object pointer
  const auto driveSessionTracker = static_cast<DriveSessionTracker*>(state);
  if (!driveSessionTracker) {
    return;
  }

  const auto optionalTapeDrive = driveSessionTracker->queryTapeDrive();
  if (!optionalTapeDrive.has_value()) {
    return;
  }
  const auto actualDriveStatus = optionalTapeDrive.value().driveStatus;

  if (std::holds_alternative<std::shared_ptr<opentelemetry::metrics::ObserverResultT<int64_t>>>(observer_result)) {
    auto typed_observer = std::get<std::shared_ptr<opentelemetry::metrics::ObserverResultT<int64_t>>>(observer_result);
    // All drive statuses are emitted at each interval to prevent missing metrics
    // See https://opentelemetry.io/docs/specs/semconv/system/k8s-metrics/#metric-k8spodphase
    for (const auto driveStatus : common::dataStructures::AllDriveStatuses) {
      int64_t observed = (driveStatus == actualDriveStatus ? 1 : 0);
      typed_observer->Observe(observed,
                              {
                                {semconv::attr::kCtaTapedDriveState, common::dataStructures::toString(driveStatus)}
      });
    }
  }
}

static void ObserveMountType(opentelemetry::metrics::ObserverResult observer_result, void* state) noexcept {
  // Recover the object pointer
  const auto driveSessionTracker = static_cast<DriveSessionTracker*>(state);
  if (!driveSessionTracker) {
    return;
  }

  const auto optionalTapeDrive = driveSessionTracker->queryTapeDrive();
  if (!optionalTapeDrive.has_value()) {
    return;
  }
  const auto actualMountType = optionalTapeDrive.value().mountType;

  if (std::holds_alternative<std::shared_ptr<opentelemetry::metrics::ObserverResultT<int64_t>>>(observer_result)) {
    auto typed_observer = std::get<std::shared_ptr<opentelemetry::metrics::ObserverResultT<int64_t>>>(observer_result);
    // All mount types are emitted at each interval to prevent missing metrics
    // Note that we ignore the Label and ArchiveAllTypes mount types as these are not actively used
    using enum common::dataStructures::MountType;
    constexpr std::array<common::dataStructures::MountType, 4> mountTypes = {ArchiveForUser,
                                                                             ArchiveForRepack,
                                                                             Retrieve,
                                                                             NoMount};
    for (const auto mountType : mountTypes) {
      int64_t observed = (mountType == actualMountType ? 1 : 0);
      typed_observer->Observe(
        observed,
        {
          {semconv::attr::kCtaTapedMountType, common::dataStructures::toCamelCaseString(mountType)}
      });
    }
  }
}

DriveSessionTracker::DriveSessionTracker(std::shared_ptr<catalogue::Catalogue> catalogue, std::string_view driveName)
    : m_catalogue(catalogue),
      m_driveName(driveName) {
  telemetry::metrics::CtaTapedDriveStatus->AddCallback(ObserveSessionState, this);
  telemetry::metrics::ctaTapedMountType->AddCallback(ObserveMountType, this);
}

DriveSessionTracker::~DriveSessionTracker() {
  telemetry::metrics::CtaTapedDriveStatus->RemoveCallback(ObserveSessionState, this);
  telemetry::metrics::ctaTapedMountType->RemoveCallback(ObserveMountType, this);
}

std::optional<common::dataStructures::TapeDrive> DriveSessionTracker::queryTapeDrive() const {
  return m_catalogue->DriveState()->getTapeDrive(m_driveName);
}

}  // namespace castor::tape::tapeserver::daemon

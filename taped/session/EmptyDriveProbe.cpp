/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "EmptyDriveProbe.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::tape::daemon::EmptyDriveProbe::EmptyDriveProbe(cta::log::Logger& log,
                                                    const cta::common::dataStructures::DriveInfo& driveInfo,
                                                    System::virtualWrapper& sysWrapper)
    : m_log(log),
      m_driveInfo(driveInfo),
      m_sysWrapper(sysWrapper) {}

//------------------------------------------------------------------------------
// driveIsEmpty()
//------------------------------------------------------------------------------
bool cta::tape::daemon::EmptyDriveProbe::driveIsEmpty() noexcept {
  std::string errorMessage;

  try {
    return exceptionThrowingDriveIsEmpty();
  } catch (cta::exception::Exception& ex) {
    errorMessage = ex.getMessage().str();
  } catch (std::exception& se) {
    errorMessage = se.what();
  } catch (...) {
    errorMessage = "Caught an unknown exception";
  }

  m_probeErrorMsg = std::string("EmptyDriveProbe: ") + errorMessage;
  // Reaching this point means the probe failed and an exception was thrown
  std::vector<cta::log::Param> params = {cta::log::Param("tapeDrive", m_driveInfo.driveName),
                                         cta::log::Param(cta::semconv::log::exceptionMessage, errorMessage)};
  m_log(cta::log::ERR, "Probe failed", params);
  return false;
}

//------------------------------------------------------------------------------
// getProbeErrorMsg()
//------------------------------------------------------------------------------
std::optional<std::string> cta::tape::daemon::EmptyDriveProbe::getProbeErrorMsg() {
  return m_probeErrorMsg;
}

//------------------------------------------------------------------------------
// exceptionThrowingDriveIsEmpty
//------------------------------------------------------------------------------
bool cta::tape::daemon::EmptyDriveProbe::exceptionThrowingDriveIsEmpty() {
  std::vector<cta::log::Param> params;
  params.emplace_back("tapeDrive", m_driveInfo.driveName);

  std::unique_ptr<drive::DriveInterface> drivePtr = createDrive();
  drive::DriveInterface& drive = *drivePtr.get();

  // Drives that manage their own state (e.g. VtlDrive — no SCSI device, no
  // robot arm, no mechanical damage risk) opt out of the hardware probe.
  // Any stale state left by a crashed session is handled inside the drive
  // implementation itself (e.g. VtlDrive::waitUntilReady()).
  if (!drive.requiresEmptyDriveProbe()) {
    m_log(cta::log::INFO, "Empty-drive probe skipped: drive opts out of hardware probe", params);
    return true;
  }

  if (drive.hasTapeInPlace()) {
    m_log(cta::log::INFO, "Probe found tape drive with a tape inside", params);
    return false;
  } else {
    m_log(cta::log::INFO, "Probe found tape drive is empty", params);
    return true;
  }
}

//------------------------------------------------------------------------------
// createDrive
//------------------------------------------------------------------------------
std::unique_ptr<cta::tape::drive::DriveInterface> cta::tape::daemon::EmptyDriveProbe::createDrive() {
  std::unique_ptr<cta::tape::drive::DriveInterface> drive = drive::createDrive(m_driveInfo, m_sysWrapper);

  if (nullptr == drive.get()) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to instantiate drive object";
    throw ex;
  }

  return drive;
}

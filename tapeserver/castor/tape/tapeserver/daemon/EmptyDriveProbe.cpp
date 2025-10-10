/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "castor/tape/tapeserver/daemon/EmptyDriveProbe.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::EmptyDriveProbe::EmptyDriveProbe(
  cta::log::Logger &log,
  const cta::tape::daemon::DriveConfigEntry &driveConfig,
  System::virtualWrapper &sysWrapper):
  m_log(log),
  m_driveConfig(driveConfig),
  m_sysWrapper(sysWrapper) {
}

//------------------------------------------------------------------------------
// driveIsEmpty()
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::EmptyDriveProbe::driveIsEmpty() noexcept {
  std::string errorMessage;

  try {
    return exceptionThrowingDriveIsEmpty();
  } catch(cta::exception::Exception &ex) {
    errorMessage = ex.getMessage().str();
  } catch(std::exception &se) {
    errorMessage = se.what();
  } catch(...) {
    errorMessage = "Caught an unknown exception";
  }

  m_probeErrorMsg = std::string("EmptyDriveProbe: ") + errorMessage;
  // Reaching this point means the probe failed and an exception was thrown
  std::list<cta::log::Param> params = {
    cta::log::Param("tapeDrive", m_driveConfig.unitName),
    cta::log::Param("exceptionMessage", errorMessage)};
  m_log(cta::log::ERR, "Probe failed", params);
  return false;
}

//------------------------------------------------------------------------------
// getProbeErrorMsg()
//------------------------------------------------------------------------------
std::optional<std::string> castor::tape::tapeserver::daemon::EmptyDriveProbe::getProbeErrorMsg(){
  return m_probeErrorMsg;
}

//------------------------------------------------------------------------------
// exceptionThrowingDriveIsEmpty
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::EmptyDriveProbe::
  exceptionThrowingDriveIsEmpty() {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("tapeDrive", m_driveConfig.unitName));

  std::unique_ptr<drive::DriveInterface> drivePtr = createDrive();
  drive::DriveInterface &drive = *drivePtr.get();

  if(drive.hasTapeInPlace()) {
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
std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface>
  castor::tape::tapeserver::daemon::EmptyDriveProbe::createDrive() {
  SCSI::DeviceVector dv(m_sysWrapper);
  SCSI::DeviceInfo driveInfo = dv.findBySymlink(m_driveConfig.devFilename);

  // Instantiate the drive object
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface>
    drive(drive::createDrive(driveInfo, m_sysWrapper));

  if(nullptr == drive.get()) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to instantiate drive object";
    throw ex;
  }

  return drive;
}

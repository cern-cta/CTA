/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "Exception.hpp"

//------------------------------------------------------------------------------
// ExceptionLauncher
//------------------------------------------------------------------------------
void castor::tape::SCSI::ExceptionLauncher(const SCSI::Structures::LinuxSGIO_t &
  sgio, const std::string& context) {
  std::stringstream contextWithStatuses;
  contextWithStatuses << context
                      << std::hex << std::nouppercase << std::showbase
                      << " status=" << static_cast<unsigned int>(sgio.status)
                      << " host_status=" << sgio.host_status
                      << " driver_status=" << sgio.driver_status
                      << ":";

  checkAndThrowSgStatus(sgio, contextWithStatuses.str());
  checkAndThrowSgHostStatus(sgio, contextWithStatuses.str());
  checkAndThrowSgDriverStatus(sgio, contextWithStatuses.str());
}

//------------------------------------------------------------------------------
// checkAndThrowSgStatus
//------------------------------------------------------------------------------
void castor::tape::SCSI::checkAndThrowSgStatus(
  const SCSI::Structures::LinuxSGIO_t & sgio, const std::string& context) {
  if (SCSI::Status::GOOD != sgio.status) {
    if (SCSI::Status::CHECK_CONDITION == sgio.status) {
      unsigned char senseKey;
      castor::tape::SCSI::Structures::senseData_t<255> * sense =
        (SCSI::Structures::senseData_t<255> *)sgio.sbp;
      try {
        senseKey = sense->getSenseKey();
      } catch (...) {
        throw Exception(sgio.status,
          (SCSI::Structures::senseData_t<255> *)sgio.sbp, context);
      }
      switch (senseKey) {
        case castor::tape::SCSI::senseKeys::notReady :
          throw NotReadyException(sgio.status,
            (SCSI::Structures::senseData_t<255> *)sgio.sbp, context);
        case castor::tape::SCSI::senseKeys::unitAttention :
          throw UnitAttentionException(sgio.status,
            (SCSI::Structures::senseData_t<255> *)sgio.sbp, context);
        default:
          throw Exception(sgio.status,
            (SCSI::Structures::senseData_t<255> *)sgio.sbp, context);
      }
    } else {
      throw Exception(sgio.status,
        (SCSI::Structures::senseData_t<255> *)sgio.sbp, context);
    }
  }
}

//------------------------------------------------------------------------------
// checkAndThrowSgHostStatus
//------------------------------------------------------------------------------
void castor::tape::SCSI::checkAndThrowSgHostStatus(
  const SCSI::Structures::LinuxSGIO_t & sgio, const std::string& context) {
  if (SCSI::HostStatus::OK != sgio.host_status) {
    throw HostException(sgio.host_status, context);
  }
}

//------------------------------------------------------------------------------
// checkAndThrowSgDriverStatus
//------------------------------------------------------------------------------
void castor::tape::SCSI::checkAndThrowSgDriverStatus(
  const SCSI::Structures::LinuxSGIO_t & sgio, const std::string& context) {
  if (SCSI::DriverStatus::OK != sgio.driver_status) {
    throw DriverException(sgio.driver_status,
      (SCSI::Structures::senseData_t<255> *)sgio.sbp, context);
  }
}

//------------------------------------------------------------------------------
// Exception
//------------------------------------------------------------------------------
castor::tape::SCSI::Exception::Exception(
  unsigned char status, castor::tape::SCSI::Structures::senseData_t<255>* sense,
  const std::string& context): cta::exception::Exception("") {
  std::stringstream w;
  w << context << (context.size()?" ":"")
          << "SCSI command failed with status "
          << SCSI::statusToString(status);
  if (SCSI::Status::CHECK_CONDITION == status) {
    w << ": Sense Information";
    try {
      w << ": " << sense->getSenseKeyString();
    } catch (Exception &ex) {
      w << ": In addition, failed to get Sense Key string: "
              << ex.getMessage().str();
    }
    try {
      w << ": " << sense->getACSString();
    } catch (Exception &ex) {
      w << ": In addition, failed to get ACS string: "
              << ex.getMessage().str();
    }
  }
  setWhat(w.str());
}

//------------------------------------------------------------------------------
// HostException
//------------------------------------------------------------------------------
castor::tape::SCSI::HostException::HostException(
  const unsigned short int host_status, const std::string& context):
  cta::exception::Exception("") {
  std::stringstream w;
  w << context << (context.size()?" ":"")
          << "SCSI command failed with host_status: "
          << SCSI::hostStatusToString(host_status);
  setWhat(w.str());
}

//------------------------------------------------------------------------------
// DriverException
//------------------------------------------------------------------------------
castor::tape::SCSI::DriverException::DriverException(
  const unsigned short int driver_status,
  castor::tape::SCSI::Structures::senseData_t<255>* sense,
  const std::string& context) : cta::exception::Exception("") {
  std::stringstream w;
  const std::string driverSuggestions =
          SCSI::driverStatusSuggestionsToString(driver_status);
  w << context << (context.size() ? " " : "")
          << "SCSI command failed with driver_status: "
          << SCSI::driverStatusToString(driver_status)
          << (driverSuggestions.size() ? ": Driver suggestions:" : "")
          << driverSuggestions;

  if (SCSI::DriverStatus::SENSE & driver_status) {
    w << ": Sense Information";
    try {
      w << ": " << sense->getSenseKeyString();
    } catch (Exception &ex) {
      w << ": In addition, failed to get Sense Key string: "
              << ex.getMessage().str();
    }
    try {
      w << ": " << sense->getACSString();
    } catch (Exception &ex) {
      w << ": In addition, failed to get ACS string: "
              << ex.getMessage().str();
    }
  }

  setWhat(w.str());
}

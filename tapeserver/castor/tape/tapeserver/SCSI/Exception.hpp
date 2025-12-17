/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Constants.hpp"
#include "Structures.hpp"
#include "common/exception/Exception.hpp"

#include <sstream>
#include <string>

namespace castor::tape::SCSI {

/**
 * An exception class turning SCSI sense data into a loggable string
 */
class Exception : public cta::exception::Exception {
public:
  Exception(unsigned char status, Structures::senseData_t<255>* sense, const std::string& context = "");
  ~Exception() override = default;
};

/**
 * Failed with NotReady error.
 */
class NotReadyException : public castor::tape::SCSI::Exception {
public:
  /**
   * Constructor
   */
  NotReadyException(unsigned char status,
                    castor::tape::SCSI::Structures::senseData_t<255>* sense,
                    const std::string& context = "")
      : Exception(status, sense, context) {}
};  // class NotReadyException

/**
 * Failed with UnitAttention error.
 */
class UnitAttentionException : public castor::tape::SCSI::Exception {
public:
  /**
   * Constructor
   */
  UnitAttentionException(unsigned char status,
                         castor::tape::SCSI::Structures::senseData_t<255>* sense,
                         const std::string& context = "")
      : Exception(status, sense, context) {}
};  // class UnitAttentionException

/**
 * Failed with host error
 */
class HostException : public cta::exception::Exception {
public:
  explicit HostException(const unsigned short int host_status, const std::string& context = "");
  ~HostException() final = default;
};

/**
 * Failed with driver error.
 */
class DriverException : public cta::exception::Exception {
public:
  DriverException(const unsigned short int driver_status,
                  Structures::senseData_t<255>* sense,
                  const std::string& context = "");
  ~DriverException() final = default;
};

/**
 * Automated exception launcher in case of SCSI command error. Does nothing
 * in the absence of errors.
 * @param sgio the sgio struct.
 */
void ExceptionLauncher(const SCSI::Structures::LinuxSGIO_t& sgio, const std::string& context = "");

/**
 * Check and throw exception in case of SCSI command error with bad status.
 * Does nothing in the absence of errors.
 * @param sgio the sgio struct.
 * @param context The string to be used as the beginning for the exception
 *                message.
 */
void checkAndThrowSgStatus(const SCSI::Structures::LinuxSGIO_t& sgio, const std::string& context);

/**
 * Check and throw exception in case of SCSI command error with bad host
 * status. Does nothing in the absence of errors.
 * @param sgio the sgio struct.
 * @param context The string to be used as the beginning for the exception
 *                message.
 */
void checkAndThrowSgHostStatus(const SCSI::Structures::LinuxSGIO_t& sgio, const std::string& context);

/**
 * Check and throw exception in case of SCSI command error with bad driver
 * status. Does nothing in the absence of errors.
 * @param sgio the sgio struct.
 * @param context The string to be used as the beginning for the exception
 *                message.
 */
void checkAndThrowSgDriverStatus(const SCSI::Structures::LinuxSGIO_t& sgio, const std::string& context);
}  // namespace castor::tape::SCSI

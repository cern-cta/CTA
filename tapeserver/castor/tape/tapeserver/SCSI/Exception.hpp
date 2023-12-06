/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include <sstream>
#include <string>

#include "common/exception/Exception.hpp"
#include "Structures.hpp"
#include "Constants.hpp"

namespace castor::tape::SCSI {

/**
 * An exception class turning SCSI sense data into a loggable string
 */
class Exception : public cta::exception::Exception {
public:
  Exception(unsigned char status, Structures::senseData_t<255>* sense, const std::string& context = "");
  virtual ~Exception() = default;
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
    castor::tape::SCSI::Structures::senseData_t<255> * sense,
    const std::string& context = ""):
    Exception(status, sense, context) {}
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
    castor::tape::SCSI::Structures::senseData_t<255> * sense,
    const std::string & context = ""):
    Exception(status, sense, context) {}
};  // class UnitAttentionException

/**
 * Failed with host error
 */
class HostException : public cta::exception::Exception {
public:
  HostException(const unsigned short int host_status, const std::string& context = "");
  virtual ~HostException() = default;
};

/**
 * Failed with driver error.
 */
class DriverException : public cta::exception::Exception {
public:
  DriverException(const unsigned short int driver_status, Structures::senseData_t<255>* sense, const std::string& context = "");
  virtual ~DriverException() = default;
};

/**
 * Automated exception launcher in case of SCSI command error. Does nothing
 * in the absence of errors.
 * @param sgio the sgio struct.
 */
void ExceptionLauncher(const SCSI::Structures::LinuxSGIO_t & sgio, const std::string& context = "");

/**
 * Check and throw exception in case of SCSI command error with bad status.
 * Does nothing in the absence of errors.
 * @param sgio the sgio struct.
 * @param context The string to be used as the beginning for the exception
 *                message.
 */
void checkAndThrowSgStatus(const SCSI::Structures::LinuxSGIO_t & sgio,
  const std::string& context);

/**
 * Check and throw exception in case of SCSI command error with bad host
 * status. Does nothing in the absence of errors.
 * @param sgio the sgio struct.
 * @param context The string to be used as the beginning for the exception
 *                message.
 */
void checkAndThrowSgHostStatus(const SCSI::Structures::LinuxSGIO_t & sgio, const std::string& context);

/**
 * Check and throw exception in case of SCSI command error with bad driver
 * status. Does nothing in the absence of errors.
 * @param sgio the sgio struct.
 * @param context The string to be used as the beginning for the exception
 *                message.
 */
void checkAndThrowSgDriverStatus(const SCSI::Structures::LinuxSGIO_t & sgio, const std::string& context);
} // namespace castor::tape::SCSI

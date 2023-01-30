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

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "tapeserver/castor/tape/tapeserver/RAO/RAOParams.hpp"
#include "tapeserver/castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "tapeserver/castor/tape/tapeserver/SCSI/Structures.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "tapeserver/castor/tape/tapeserver/RAO/RAOAlgorithmFactory.hpp"
#include "common/log/LogContext.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

/**
 * This class will be used to manage everything that is linked to RAO.
 * It centralizes all the RAO-related task that are executed during the
 * RecallTaskInjector lifecycle.
 */
class RAOAlgorithmFactoryFactory;

class RAOManager {
public:
  friend RAOAlgorithmFactoryFactory;
  RAOManager();
   /**
   * Constructor of a RAO manager
   * @param config the configuration of the RAO to manage
   * @param drive the DriveInterface of the drive that is mounting.
   * @param catalogue the pointer to the CTA catalogue
   */
  RAOManager(const RAOParams & config, castor::tape::tapeserver::drive::DriveInterface * drive, cta::catalogue::Catalogue * catalogue);

  /**
   * Copy constructor
   * @param manager the RAOManager to copy
   */
  RAOManager(const RAOManager & manager);

  /**
   * Assignment operator
   * */
  RAOManager & operator=(const RAOManager & manager);

  /**
   * Returns true if RAO will be used, false otherwise
   */
  bool useRAO() const;

  /**
   * Returns true if the manager has informations about the drive's User Data Segments limits to
   * perform RAO for enteprise drive
   */
  bool hasUDS() const;

  /**
   * Returns the number of files that will be supported by the RAO algorithm
   */
  std::optional<uint64_t> getMaxFilesSupported() const;

  /**
   * Disable the RAO algorithm
   */
  void disableRAO();

  /**
   * Set the enterprise RAO User Data Segments limits
   * that will be used by this manager to perform the Enterprise RAO query on the drive
   * @param raoLimits the enterprise RAO user data segments limits
   */
  void setEnterpriseRAOUdsLimits(const SCSI::Structures::RAO::udsLimits & raoLimits);

  /**
   * Query the RAO of the files passed in parameter
   * @param jobs the vector of jobs to query the RAO
   * @param lc the log context
   * @return the vector with re-arranged indexes of the jobs passed in parameter
   * It does not returns the fseqs, but a vector of indexes that will be used by the recall task injector to pick
   * the correct job after RAO has been done
   */
  std::vector<uint64_t> queryRAO(const std::vector<std::unique_ptr<cta::RetrieveJob>> & jobs, cta::log::LogContext & lc);

  virtual ~RAOManager();

private:
  //RAO Configuration Data
  RAOParams m_raoParams;
  /** Enterprise Drive-specific RAO parameters */
  SCSI::Structures::RAO::udsLimits m_enterpriseRaoLimits;
  //Is true if the drive have been able to get the RAO UDS limits numbers
  bool m_hasUDS = false;
  //The maximum number of files that will be considered for RAO
  std::optional<uint64_t> m_maxFilesSupported;
  //Pointer to the drive interface of the drive currently used by the tapeserver
  castor::tape::tapeserver::drive::DriveInterface * m_drive;
  bool m_isDriveEnterpriseEnabled = false;
  cta::catalogue::Catalogue * m_catalogue;

  /**
   * Returns true if the manager can instanciate an Enterprise RAO Algorithm
   * false otherwise
   */
  bool isDriveEnterpriseEnabled() const;

  /**
   * Returns the pointer to the interface of the drive currently mounting the tape
   */
  castor::tape::tapeserver::drive::DriveInterface * getDrive() const;

  /**
   * Returns the pointer to the catalogue of this manager
   */
  cta::catalogue::Catalogue * getCatalogue() const;

   /**
   * Returns the RAO data that is used by this RAOManager
   */
  RAOParams getRAOParams() const;

  /**
   * Log a warning message after failing an RAO instanciation or execution
   * @param exceptionMsg the exception message to log
   * @param lc the log context to log the warning message
   */
  void logWarningAfterRAOOperationFailed(const std::string & warningMsg, const std::string & exceptionMsg, cta::log::LogContext & lc) const;

};

}}}}

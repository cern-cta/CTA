/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once
#include <string>
#include <cstdint>
#include "common/log/DummyLogger.hpp"

namespace cta { namespace tape { namespace daemon {
/**
 * Class containing all the parameters needed by the transfer process
 * to manage its drive
 */
struct DriveConfiguration {
  /** The name of the drive */
  std::string unitName;
  
  /** The logical library to which the drive belongs */
  std::string logicalLibrary;
  
  /** The filename of the device file of the tape drive. */
  std::string devFilename;

  /** The slot in the tape library that contains the tape drive. */
  std::string librarySlot;

  /** The size in bytes of a data-transfer buffer. */
  uint32_t bufferSize;

  /** The total number of data-transfer buffers to be instantiated. */
  uint32_t bufferCount;
  
  /** The structure representing the maximum number of bytes and files 
   cta-taped will fetch or report in one access to the object store*/
  struct fetchReportOrFLushLimits {
    uint64_t maxBytes;
    uint64_t maxFiles;
    fetchReportOrFLushLimits(): maxBytes(0), maxFiles(0) {}
  };
  
  /** Archive jobs fetch/report limits */
  fetchReportOrFLushLimits bulkArchive;
  
  /** Retrieve jobs fetch/report limits */
  fetchReportOrFLushLimits bulkRetrieve;

  /** Amount of data/files after which we will flush data to tape (archive only) */
  fetchReportOrFLushLimits flushData;

  /** The number of disk I/O threads. */
  uint32_t nbDiskThreads;

  /** Constructor that sets all integer member-variables to 0 and all string
   * member-variables to the empty string. */
  DriveConfiguration(): bufferSize(0), bufferCount(0), nbDiskThreads(0) {};

  /** Returns a configuration structure based on the contents of
   * /etc/cta/cta.conf, /etc/cta/TPCONFIG and compile-time constants.
   *
   * @param log pointer to NULL or an optional logger object.
   * @return The configuration structure. */
  static DriveConfiguration createFromCtaConf(
    cta::log::Logger & log = gDummyLogger);
  
  /** Returns a configuration structure based on the contents of
   * /etc/cta/cta.conf, /etc/cta/TPCONFIG and compile-time constants.
   *
   * @param log pointer to NULL or an optional logger object.
   * @return The configuration structure. */
  static DriveConfiguration createFromCtaConf(
    const std::string & generalConfigPath,
    const std::string & tapeConfigFile,
    cta::log::Logger & log = gDummyLogger);
  
private:
  /** A private dummy logger which will simplify the implementaion of the 
   * functions (just unconditionally log things). */
  static cta::log::DummyLogger gDummyLogger;
}; // DriveConfiguration

}}} // namespace cta::tape::daemon
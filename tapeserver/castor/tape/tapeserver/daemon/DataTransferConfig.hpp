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

#include <stdint.h>
#include <string>

namespace castor::tape::tapeserver::daemon {

/**
 * The contents of the cta.conf file to be used by a DataTransferSession
 */
struct DataTransferConfig {
  /**
   * The size of a data-transfer buffer in bytes
   */
  uint32_t bufsz = 0;

  /**
   * The total number of data-transfer buffers to be instantiated
   */
  uint32_t nbBufs = 0;

  /**
   * Maximum number of bytes in a set of files to be archived to tape
   */
  uint64_t bulkRequestMigrationMaxBytes = 0;

  /**
   * Maximum number of files in a set of files to be archived to tape
   */
  uint64_t bulkRequestMigrationMaxFiles = 0;

  /**
   * Maximum number of bytes in a set of files to be recalled from tape
   */
  uint64_t bulkRequestRecallMaxBytes = 0;

  /**
   * Maximum number of files in a set of files to be recalled from tape
   */
  uint64_t bulkRequestRecallMaxFiles = 0;

  /**
   * Maximum number of bytes to be written before a flush to tape (synchronised tape-mark). Note that as
   * a flush occurs on a file boundary, usually more bytes will be written to tape before the actual flush occurs.
   */
  uint64_t maxBytesBeforeFlush = 0;

  /**
   * Maximum number of files to be written before a flush to tape (synchronised or non-immediate tape-mark)
   */
  uint64_t maxFilesBeforeFlush = 0;

  /**
   * Number of disk I/O threads
   */
  uint32_t nbDiskThreads = 0;

  /**
   * Timeout for XRoot functions
   *
   * Default is 0 (no timeout)
   */
  uint16_t xrootTimeout = 0;

  /**
   * Whether to use Logical Block Protection (LBP)
   */
  bool useLbp = false;

  /**
   * Whether to use Recommended Access Order (RAO)
   */
  bool useRAO = false;
  
  /**
   * Which RAO LTO algorithm to use
   */
  std::string raoLtoAlgorithm;
  
  /**
   * Options to be provided to the raoLtoAlgorithm
   */
  std::string raoLtoAlgorithmOptions;

  /**
   * Whether to use encryption
   */
  bool useEncryption = true;

  /**
   * Path to the operator-provided encyption control script (empty string for none)
   */
  std::string externalEncryptionKeyScript;  
    
  /**
   * Path to the operator-provided disk free space fetch script (empty string for none)
   */
  std::string externalFreeDiskSpaceScript;

  /**
   * Timeout after which the mounting of a tape is considered failed
   */
  uint32_t tapeLoadTimeout = 300;

  /**
   * Maximum time allowed after mounting without a single tape block move
   */
  time_t wdNoBlockMoveMaxSecs = 600;

  /**
   * Time to wait after scheduling came up idle
   */
  time_t wdIdleSessionTimer = 10;

  /**
   * Timeout after which the tape server stops trying to get the next mount
   */
  time_t wdGetNextMountMaxSecs = 900;
};

} // namespace castor::tape::tapeserver::daemon

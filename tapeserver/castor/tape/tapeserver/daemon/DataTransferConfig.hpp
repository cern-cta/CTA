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

#include "common/log/Logger.hpp"

#include <stdint.h>
#include <string>

namespace castor::tape::tapeserver::daemon {

/**
 * The contents of the cta.conf file to be used by a DataTransferSession.
 */
struct DataTransferConfig {

  /**
   * The size in bytes of a data-transfer buffer.
   */
  uint32_t bufsz;

  /**
   * The total number of data-transfer buffers to be instantiated.
   */
  uint32_t nbBufs;

  /**
   * When the tapebridged daemon requests the tapegatewayd daemon for a set of
   * files to migrate to tape, this parameter defines the maximum number of
   * bytes the set of files should represent.
   */
  uint64_t bulkRequestMigrationMaxBytes;

  /**
   * When the tapebridged daemon requests the tapegatewayd daemon for a set of
   * files to migrate to tape, this parameter defines the maximum number of files
   * the set may contain.
   */
  uint64_t bulkRequestMigrationMaxFiles;

  /**
   * When the tapebridged daemon requests the tapegatewayd daemon for a set of
   * files to recall from tape, this parameter defines the maximum number of bytes
   * the set of files should represent.
   */
  uint64_t bulkRequestRecallMaxBytes;

  /**
   * When the tapebridged daemon requests the tapegatewayd daemon for a set of
   * files to recall from tape, this parameter defines the maximum number of bytes
   * the set of files should represent.
   */
  uint64_t bulkRequestRecallMaxFiles;

  /**
   * The value of this parameter defines the maximum number of bytes to be written
   * to tape before a flush to tape (synchronised tape-mark).  Please note that a
   * flush occurs on a file boundary therefore more bytes will normally be written
   * to tape before the actual flush occurs.
   */
  uint64_t maxBytesBeforeFlush;

  /**
   * The value of this parameter defines the maximum number of files to be written
   * to tape before a flush to tape (synchronised or non-immediate tape-mark).
   */
  uint64_t maxFilesBeforeFlush;

  /**
   * The number of disk I/O threads.
   */
  uint32_t nbDiskThreads;

  /**
   * The timeout for all the xroot functions. The default is 0 (no timeout)
   */
  uint16_t xrootTimeout;

  /**
   * The boolean variable describing to use on not to use Logical
   * Block Protection.
   */
  bool useLbp;

  /**
   * The boolean variable describing to use on not to use Recommended
   * Access Order
   */
  bool useRAO;
  
  /**
   * The name of the RAO LTO algorithm to use
   */
  std::string raoLtoAlgorithm;
  
  /**
   * The options that can be used by the raoAlgorithm
   */
  std::string raoLtoAlgorithmOptions;

  /**
   * The boolean variable describing to use on not to use Encryption
   */
  bool useEncryption;

  /**
   * The path to the operator provided encyption control script (or empty string)
   */
  std::string externalEncryptionKeyScript;  
    
  /**
   * The path to the operator provided EOS free space fetch script (or empty string)
   */
  std::string externalFreeDiskSpaceScript;

  /**
   * The timeout after which the mount of a tape is considered failed
   */
  uint32_t tapeLoadTimeout;

  /**
   * Maximum time allowed after mounting without a single tape block move
   */
  time_t wdNoBlockMoveMaxSecs;

  /**
   * Time to wait after scheduling came up idle
   */
  time_t wdIdleSessionTimer;

  /**
  * The timeout after which the tape server stops trying to get the next mount
  */
  time_t wdGlobalLockAcqMaxSecs;

  /**
   * Constructor that sets all integer member-variables to 0 and all string
   * member-variables to the empty string.
   */
  DataTransferConfig() noexcept;

}; // DataTransferConfig

} // namespace castor::tape::tapeserver::daemon

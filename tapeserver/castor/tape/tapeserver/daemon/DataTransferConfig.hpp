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

#pragma once

#include "common/log/Logger.hpp"

#include <stdint.h>
#include <string>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/**
 * The contents of the castor.conf file to be used by a DataTransferSession.
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
   * The location of the file containing the private RSA key to be used when
   * using XROOT as the remote transfer protocol.
   */
  std::string xrootPrivateKey;

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
  std::string fetchEosFreeSpaceScript;

  /**
   * The timeout after which the mount of a tape is considered failed
   */
  uint32_t tapeLoadTimeout;

  /**
   * Constructor that sets all integer member-variables to 0 and all string
   * member-variables to the empty string.
   */
  DataTransferConfig() throw();

}; // DataTransferConfig

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

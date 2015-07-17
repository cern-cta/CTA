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
#include "common/archiveNS/ArchiveFileStatus.hpp"

namespace cta {

/**
 * The path and status of an archive file.
 */
struct TapeCopyLocationAndStatus {
  /** Constructor. */
  TapeCopyLocationAndStatus();

  /**
   * Constructor.
   *
   * @param path The path of the file in the archive.
   * @param status The status of the file in the archive.
   */
  TapeCopyLocationAndStatus(
    const std::string &vid,
    const uint64_t fSeq,
    const uint64_t blockId,
    const uint64_t size,
    const uint64_t fileId,
    const Checksum &checksum);

  std::string vid;  /**< The tape on which the copy is located */
  uint64_t fSeq;    /**< The file sequence number (fSeq) for the copy on tape */
  uint64_t blockId; /**< The tape block holding the first header block for the file */
  uint64_t size;    /**< The tape copy's size */
  uint64_t fileId;  /**< The file ID as recorded in the tape copy's header */
  Checksum checksum;/**< The tape copy's checksum */
}; // class TapeCopyLocationAndStatus

} // namespace cta

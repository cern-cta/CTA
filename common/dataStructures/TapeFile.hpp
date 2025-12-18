/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <common/checksum/ChecksumBlob.hpp>
#include <list>
#include <map>
#include <string>

namespace cta::common::dataStructures {

/**
 * This struct holds location information of a specific tape file
 */
struct TapeFile {
  TapeFile();

  TapeFile(const std::string& tpid,
           uint64_t seq,
           uint64_t blkId,
           uint64_t flsize,
           uint8_t cpNb,
           time_t cTime,
           const checksum::ChecksumBlob& checksum)
      : vid(tpid),
        fSeq(seq),
        blockId(blkId),
        fileSize(flsize),
        copyNb(cpNb),
        creationTime(cTime),
        checksumBlob(std::move(checksum)) {}

  bool operator==(const TapeFile& rhs) const;

  bool operator!=(const TapeFile& rhs) const;

  /**
   * The volume identifier of the tape on which the file has been written.
   */
  std::string vid;
  /**
   * The position of the file on tape in the form of its file sequence
   * number.
   */
  // TODO: could be modified to match SCSI nomenclature (Logical file identifier), delta factor 3 between our files and tape's
  uint64_t fSeq;
  /**
   * The position of the file on tape in the form of its logical block
   * identifier.
   */
  // TODO: change denomination to match SCSI nomenclature (logical object identifier).
  uint64_t blockId;
  /**
   * The uncompressed (logical) size of the tape file in bytes. This field is redundant as it already exists in the
   * ArchiveFile class, so it may be removed in future.
   */
  uint64_t fileSize;
  /**
   * The copy number of the file. Copy numbers start from 1. Copy number 0
   * is an invalid copy number.
   */
  uint8_t copyNb;
  /**
   * The time the file recorded in the catalogue.
   */
  time_t creationTime;

  /**
   * Set of checksum (type, value) pairs
   */
  checksum::ChecksumBlob checksumBlob;

  /**
   * Returns true if this tape file matches the copy number passed in parameter.
   */
  bool matchesCopyNb(uint8_t copyNb) const;

};  // struct TapeFile

std::ostream& operator<<(std::ostream& os, const TapeFile& obj);

}  // namespace cta::common::dataStructures

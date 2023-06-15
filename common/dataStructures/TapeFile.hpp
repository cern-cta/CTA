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

#include <list>
#include <map>
#include <string>

#include <common/checksum/ChecksumBlob.hpp>

namespace cta {
namespace common {
namespace dataStructures {

/**
 * This struct holds location information of a specific tape file 
 */
struct TapeFile {
  TapeFile();

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

}  // namespace dataStructures
}  // namespace common
}  // namespace cta

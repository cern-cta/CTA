/*!
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019 CERN
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

#include <set>

#include <common/checksum/Checksum.hpp>
#include <common/checksum/ChecksumBlobSizeMismatch.hpp>

namespace cta {

/*!
 * A set of one or more checksums
 */
struct ChecksumBlob {
  std::set<Checksum> cs;

  /*!
   * Check all the checksums in the blob match, throw an exception if they don't
   */
  void validate(const ChecksumBlob &blob) const {
     if(cs.size() != blob.cs.size()) {
       throw ChecksumBlobSizeMismatch("Checksum blob sizes of " + std::to_string(cs.size()) + " and " +
         std::to_string(blob.cs.size()) + " do not match.");
     }

     auto it1 = cs.begin();
     auto it2 = blob.cs.begin();
     for( ; it1 != cs.end(); ++it1, ++it2) {
       it1->validate(*it2);
     }
  }

  /*!
   * Returns true if all the checksums in the blob match
   */
  bool operator==(const ChecksumBlob &blob) const {
    try {
      this->validate(blob);
    } catch(ChecksumBlobSizeMismatch &ex) {
      return false;
    } catch(ChecksumTypeMismatch &ex) {
      return false;
    } catch(ChecksumValueMismatch &ex) {
      return false;
    }
    return true;
  }

  /*!
   * Serialise to a byte array
   */
  std::string serialize() const;

  /*!
   * Deserialize from a byte array
   */
  void deserialize(const std::string &bytearray);
};




#if 0
    if(archiveFile->checksumBlob != event.checksumBlob) {
      catalogue::ChecksumTypeMismatch ex;
      ex.getMessage() << "Checksum type mismatch: expected=" << archiveFile->checksumType << ", actual=" <<
        event.checksumType << ": " << fileContext.str();
      throw ex;
    }

    if(archiveFile->checksumValue != event.checksumValue) {
      catalogue::ChecksumValueMismatch ex;
      ex.getMessage() << "Checksum value mismatch: expected=" << archiveFile->checksumValue << ", actual=" <<
        event.checksumValue << ": " << fileContext.str();
      throw ex;
    }
#endif

} // namespace cta


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
#include <common/exception/ChecksumBlobSizeMismatch.hpp>

namespace cta {

/*!
 * A set of one or more checksums
 */
class ChecksumBlob {
public:
  /*!
   * Insert a new checksum into the blob
   *
   * @param[in] value    Little-endian byte array containing the checksum
   */
  void insert(Checksum::ChecksumType type, const std::string &value);

  /*!
   * Insert a new 32-bit checksum into the blob
   *
   * @param[in] value    32-bit unsigned integer containing the checksum
   */
  void insert(Checksum::ChecksumType type, uint32_t value);

  /*!
   * Check all the checksums in the blob match, throw an exception if they don't
   */
  void validate(const ChecksumBlob &blob) const {
     if(m_cs.size() != blob.m_cs.size()) {
       throw exception::ChecksumBlobSizeMismatch("Checksum blob sizes of " + std::to_string(m_cs.size()) +
         " and " + std::to_string(blob.m_cs.size()) + " do not match.");
     }

     auto it1 = m_cs.begin();
     auto it2 = blob.m_cs.begin();
     for( ; it1 != m_cs.end(); ++it1, ++it2) {
       it1->validate(*it2);
     }
  }

  /*!
   * Returns true if all the checksums in the blob match
   */
  bool operator==(const ChecksumBlob &blob) const {
    try {
      this->validate(blob);
    } catch(exception::ChecksumBlobSizeMismatch &ex) {
      return false;
    } catch(exception::ChecksumTypeMismatch &ex) {
      return false;
    } catch(exception::ChecksumValueMismatch &ex) {
      return false;
    }
    return true;
  }

  /*!
   * Serialise to a byte array
   */
  std::string serialize() const;

  /*!
   * Length of the serialized byte array
   */
  size_t length() const;

  /*!
   * True if there are no checksums in the blob
   */
  bool empty() const { return m_cs.empty(); }

  /*!
   * Deserialize from a byte array
   */
  void deserialize(const std::string &bytearray);

private:
  friend std::ostream &operator<<(std::ostream &os, const ChecksumBlob &csb);
  std::set<Checksum> m_cs;
};

} // namespace cta

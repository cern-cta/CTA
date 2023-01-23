/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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
#include <optional>
#include <string>

#include "common/exception/UserError.hpp"

namespace cta {

namespace common {
namespace dataStructures {
struct SecurityIdentity;
}  // namespace dataStructures
}  // namespace common

namespace catalogue {

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringMediaTypeName);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringMediaType);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedMediaTypeUsedByTapes);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAZeroCapacity);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringCartridge);

struct MediaType;
struct MediaTypeWithLogs;

class MediaTypeCatalogue {
public:
  virtual ~MediaTypeCatalogue() = default;

  /**
   * Creates a tape media type.
   *
   * @param admin The administrator.
   * @param mediaType The tape media type.
   */
  virtual void createMediaType(const common::dataStructures::SecurityIdentity &admin, const MediaType &mediaType) = 0;

  /**
   * Deletes the specified tape media type.
   *
   * @param name The name of the tape media type.
   */
  virtual void deleteMediaType(const std::string &name) = 0;

  /**
   * Returns all tape media types.
   *
   * @return All tape media types.
   */
  virtual std::list<MediaTypeWithLogs> getMediaTypes() const = 0;

  /**
   * Return the media type associated to the tape corresponding to the
   * vid passed in parameter
   * @param vid the vid of the tape to return its media type
   * @return the media type associated to the tape corresponding to the vid passed in parameter
   */
  virtual MediaType getMediaTypeByVid(const std::string & vid) const = 0;

  /**
   * Modifies the name of the specified tape media type.
   *
   * @param admin The administrator.
   * @param currentName The current name of the tape media type.
   * @param newName The new name of the tape media type.
   */
  virtual void modifyMediaTypeName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &currentName, const std::string &newName) = 0;

  /**
   * Modifies the cartidge of the specified tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param cartridge The new cartidge.
   */
  virtual void modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &cartridge) = 0;

  /**
   * Modify the capacity in bytes of a tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param capacityInBytes The new capacity in bytes.
   */
  virtual void modifyMediaTypeCapacityInBytes(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t capacityInBytes) = 0;

  /**
   * Modify the SCSI primary density code of a tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param primaryDensityCode The new SCSI primary density code.
   */
  virtual void modifyMediaTypePrimaryDensityCode(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint8_t primaryDensityCode) = 0;

  /**
   * Modify the SCSI secondary density code of a tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param secondaryDensityCode The new SCSI secondary density code.
   */
  virtual void modifyMediaTypeSecondaryDensityCode(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint8_t secondaryDensityCode) = 0;

  /**
   * Modify the number of tape wraps of a tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param nbWraps The new number of tape wraps.
   */
  virtual void modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::optional<std::uint32_t> &nbWraps) = 0;

  /**
   * Modify the minimum longitudinal tape position of a tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param minLPos The new minimum longitudinal tape position.
   */
  virtual void modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::optional<std::uint64_t> &minLPos) = 0;

  /**
   * Modify the maximum longitudinal tape position of a tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param maxLPos The new maximum longitudinal tape position.
   */
  virtual void modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::optional<std::uint64_t> &maxLPos) = 0;

  /**
   * Modify the comment of a tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param comment The new comment.
   */
  virtual void modifyMediaTypeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &comment) = 0;
};

} // namespace catalogue
} // namespace cta
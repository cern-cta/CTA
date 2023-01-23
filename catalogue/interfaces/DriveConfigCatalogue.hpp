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
#include <tuple>
#include <utility>

namespace cta {

namespace catalogue {

/**
 * Specifies the interface to a factory Catalogue objects.
 */
class DriveConfigCatalogue {
public:
  virtual ~DriveConfigCatalogue() = default;

  /*
   * Struct with a drive configuration
   */
  struct DriveConfig {
    std::string tapeDriveName;
    std::string category;
    std::string keyName;
    std::string value;
    std::string source;
  };

  /**
   * Creates a specified parameter of the configuration for a certain Tape Drive
   * @param tapeDriveName The name of the tape drive.
   * @param category The category of the parameter.
   * @param keyName The key of the parameter.
   * @param value The value of the parameter.
   * @param source The source from which the parameter was gotten.
   */
  virtual void createTapeDriveConfig(const std::string &tapeDriveName, const std::string &category,
    const std::string &keyName, const std::string &value, const std::string &source) = 0;

  /**
   * Gets all Drive Configurations of all TapeDrives.
   * @return Drive Configurations of all TapeDrives.
   */
  virtual std::list<DriveConfig> getTapeDriveConfigs() const = 0;

  /**
   * Gets the Key and Names of configurations of all TapeDrives
   * @return Keys and Names of configurations.
   */
  virtual std::list<std::pair<std::string, std::string>> getTapeDriveConfigNamesAndKeys() const = 0;

  /**
   * Modifies a specified parameter of the configuration for a certain Tape Drive
   * @param tapeDriveName The name of the tape drive.
   * @param category The category of the parameter.
   * @param keyName The key of the parameter.
   * @param value The value of the parameter.
   * @param source The source from which the parameter was gotten.
   */
  virtual void modifyTapeDriveConfig(const std::string &tapeDriveName, const std::string &category,
    const std::string &keyName, const std::string &value, const std::string &source) = 0;

  /**
   * Gets a specified parameter of the configuration for a certain Tape Drive
   * @param tapeDriveName The name of the tape drive.
   * @param keyName The key of the parameter.
   * @return Returns the category, value and source of a parameter of the configuarion
   */
  virtual std::optional<std::tuple<std::string, std::string, std::string>> getTapeDriveConfig(const std::string &tapeDriveName,
    const std::string &keyName) const = 0;

  /**
   * Deletes the entry of a Drive Configuration
   * @param tapeDriveName The name of the tape drive.
   */
  virtual void deleteTapeDriveConfig(const std::string &tapeDriveName, const std::string &keyName) = 0;
};  // class TapeDriveCatalogue

}  // namespace catalogue
}  // namespace cta

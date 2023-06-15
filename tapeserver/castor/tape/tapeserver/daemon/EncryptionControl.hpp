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

#include <json-c/json.h>

#include "VolumeInfo.hpp"
#include "catalogue/Catalogue.hpp"
#include "scheduler/Scheduler.hpp"
#include "tapeserver/castor/tape/tapeserver/drive/DriveInterface.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/**
 * A class allowing the enabling and disabling of tape encryption via a script
 * provided by the operators. This script has the responsibility of providing
 * the encryption key to the tape drive.
 */
class EncryptionControl {
public:
  struct EncryptionStatus {
    bool on;
    std::string keyName;  // encryption key identifier
    std::string key;      // encryption key
    std::string stdout;
  };

  /** @param scriptPath The path to the operator provided script for acquiring the key */
  explicit EncryptionControl(const bool useEncryption, const std::string& scriptPath);
  /**
   * Will call the encryption script provided by the operators to acquire the encryption key and then enable the
   * encryption if necessary.
   * @param m_drive The drive object on which the encryption is to be enabled.
   * @param volInfo The volume info used by encryption script: VID, tape pool name, encryption ID
   * @param catalogue Catalogue instance to modify tape encryption key
   * @param isWriteSession if true, set encryption key when writing to the new tape.
   * @return {true, keyName, key, stdout} if the encryption has been set, {false, "", "", stdout} otherwise.
   */
  EncryptionStatus enable(castor::tape::tapeserver::drive::DriveInterface& m_drive,
                          castor::tape::tapeserver::daemon::VolumeInfo& volInfo,
                          cta::catalogue::Catalogue& catalogue,
                          bool isWriteSession = false);

  /**
   * Wrapper function to clear the encryption parameters from the drive - essentially meaning disabling the encryption.
   * @param m_drive The drive object on which the encryption is to be disabled.
   * @return true if encryption parameters cleared, false otherwise.
   */
  bool disable(castor::tape::tapeserver::drive::DriveInterface& m_drive);

private:
  bool m_useEncryption;  // Wether encryption must be enabled for the tape

  std::string m_path;  // Path to the key management script file

  /**
   * Parse the JSON output of the key management script and translate information into Encryption Status struct.
   * Expected to find keys key_id, encryption_key, message and the respective values as JSON strings.
   * @param input The JSON input to parse
   * @return {true, keyName, key, stdout} if the encryption has been set, {false, "", "", stdout} otherwise.
   */
  EncryptionStatus parse_json_script_output(const std::string& input);

  /**
   * Flatten all levels of a JSON object into a map of strings. When parsing nested JSON objects, it uses the key
   * of the object as prefix.
   * @param prefix Prefix for the keys of the map
   * @param jobj The JSON-C object which is flattened
   * @return A map of strings to strings representing the flattened hierarchy of the JSON input.
   */
  std::map<std::string, std::string> flatten_json_object_to_map(const std::string& prefix, json_object* jobj);

  /**
   * Flattens the arguments list of strings to a single string.
   * @param args The arguments list
   * @param delimiter The delimiter between the different arguments
   * @return A string representation of the arguments passed.
   */
  std::string argsToString(std::list<std::string> args, const std::string& delimiter);
};

}  // namespace daemon
}  // namespace tapeserver
}  // namespace tape
}  // namespace castor

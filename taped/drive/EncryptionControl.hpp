/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveInterface.hpp"
#include "catalogue/Catalogue.hpp"
#include "scheduler/Scheduler.hpp"
#include "taped/session/VolumeInfo.hpp"

#include <json-c/json.h>
#include <list>
#include <map>
#include <string>

namespace cta::tape::daemon {

/// @brief Configure tape encryption using an operator-provided key-management script.
///
/// The script supplies key material; this controller applies it to the drive and records new tape key names.
class EncryptionControl {
public:
  /// Key-management result containing key material and the script diagnostic message.
  struct EncryptionStatus {
    bool on;
    std::string keyName;  ///< Key identifier recorded in the catalogue.
    std::string key;      ///< Key material passed to the drive; must not be logged.
    std::string stdout;   ///< Parsed message field, rather than the complete script output.
  };

  /// @brief Configure whether encryption is required and where to obtain tape keys.
  /// @param useEncryption Whether a missing key-management script is an error when enabling encryption.
  /// @param scriptPath Absolute script path, or an empty string when no script is configured.
  /// @throws cta::exception::Exception If a nonempty script path is not absolute.
  explicit EncryptionControl(const bool useEncryption, const std::string& scriptPath);

  /// @brief Obtain key material when required, then enable or clear drive encryption.
  ///
  /// For a new encrypted write, record the key name in the catalogue before installing the key.
  /// If assigning a key to a nonempty tape is rejected, disable the tape and propagate the error.
  /// Script execution, JSON parsing, catalogue and drive failures propagate to the caller.
  /// @param m_drive Borrowed drive on which to apply encryption settings.
  /// @param volInfo Tape identity, pool and existing encryption key name.
  /// @param catalogue Catalogue used to read pool policy and update the tape key name.
  /// @param isWriteSession Whether a new tape key name may be assigned for writing.
  /// @return Encryption state, key name, key material and the script diagnostic message.
  EncryptionStatus enable(cta::tape::drive::DriveInterface& m_drive,
                          cta::tape::daemon::VolumeInfo& volInfo,
                          cta::catalogue::Catalogue& catalogue,
                          bool isWriteSession = false);

  /// @brief Clear encryption parameters from the borrowed drive, propagating drive failures.
  /// @return True if supported encryption parameters were cleared; false if encryption is unsupported.
  bool disable(cta::tape::drive::DriveInterface& m_drive) const;

  /// Return the configured script path as a reference valid for the controller lifetime.
  const std::string& getScriptPath() const;

private:
  const std::string c_defaultUserNameUpdate = "cta-taped";
  bool m_useEncryption;  ///< Require a key-management script when encryption is configured.

  std::string m_path;  ///< Absolute key-management script path; empty when no script is configured.

  /// @brief Parse key_name, encryption_key and message string fields from the script JSON output.
  ///
  /// Encryption is enabled in the result only when both key fields are nonempty.
  /// @throws cta::exception::Exception If parsing fails or any required field is missing.
  EncryptionStatus parse_json_script_output(const std::string& input);

  /// @brief Collect JSON string values, prefixing nested fields with their immediate enclosing object key.
  ///
  /// Non-string leaf values are ignored; the borrowed JSON object is not modified.
  /// @param prefix Prefix applied to string fields at this level.
  /// @param jobj JSON object whose fields are traversed recursively.
  std::map<std::string, std::string> flatten_json_object_to_map(const std::string& prefix, json_object* jobj);

  /// Join script arguments with the delimiter for diagnostics; return an empty string for no arguments.
  std::string argsToString(std::list<std::string> args, const std::string& delimiter) const;
};

}  // namespace cta::tape::daemon

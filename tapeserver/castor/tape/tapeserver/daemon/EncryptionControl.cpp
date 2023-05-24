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

#include <algorithm>
#include <memory>

#include "common/exception/Exception.hpp"
#include "common/threading/SubProcess.hpp"
#include "EncryptionControl.hpp"
#include "catalogue/TapePool.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
EncryptionControl::EncryptionControl(bool useEncryption, const std::string& scriptPath) :
  m_useEncryption(useEncryption),
  m_path(scriptPath) {
  if (!m_path.empty() && m_path[0] != '/') {
    throw cta::exception::Exception("In EncryptionControl::EncryptionControl: the script path is not absolute: "
                                    + m_path);
  }
}

//------------------------------------------------------------------------------
// enable
//------------------------------------------------------------------------------
auto EncryptionControl::enable(castor::tape::tapeserver::drive::DriveInterface &m_drive,
                               castor::tape::tapeserver::daemon::VolumeInfo &volInfo,
                               cta::catalogue::Catalogue &catalogue, bool isWriteSession) -> EncryptionStatus {
  EncryptionStatus encStatus;
  if (m_path.empty()) {
    if (m_useEncryption) {
      // If encryption is enabled, an external script is required
      throw cta::exception::Exception("In EncryptionControl::enableEncryption: "
                                      "failed to enable encryption: path provided is empty "
                                      "but tapeserver is configured to use encryption");
    }
    encStatus = {false, "", "", ""};
    disable(m_drive);
    return encStatus;
  }

  // Read session with no encryption key name
  // Tape is guaranteed to be unencrypted, no need to run external script
  if (!isWriteSession && !volInfo.encryptionKeyName.has_value()) {
    encStatus = {false, "", "", ""};
    disable(m_drive);
    return encStatus;
  }

  const auto pool = catalogue.TapePool()->getTapePool(volInfo.tapePool);

  // Write session with no encryption key name and encryption is disabled for the tape pool
  // Tape is guaranteed to be unencrypted, no need to run external script
  if (isWriteSession && !pool->encryption && !volInfo.encryptionKeyName.has_value()) {
    encStatus = {false, "", "", ""};
    disable(m_drive);
    return encStatus;
  }

  // In other cases we call external script to get the key value from the JSON data store
  std::list<std::string> args(
    {m_path, "--encryption-key-name", volInfo.encryptionKeyName.value_or(""), "--pool-name", volInfo.tapePool});

  cta::threading::SubProcess sp(m_path, args);
  sp.wait();
  if (sp.wasKilled() || sp.exitValue() != EXIT_SUCCESS) {
    std::ostringstream ex;
    ex << "In EncryptionControl::enableEncryption: failed to enable encryption: ";
    if (sp.wasKilled()) {
      ex << "script was killed with signal: " << sp.killSignal();
    }
    else {
      ex << "script returned: " << sp.exitValue();
    }
    ex << " called=" << "\'" << argsToString(args, " ") << "\'"
                    << " stdout=" << sp.stdout()
                    << " stderr=" << sp.stderr();
    throw cta::exception::Exception(ex.str());
  }
  encStatus = parse_json_script_output(sp.stdout());

  // In write session check if we need to set the key name for the new tape
  // If tape pool encryption is enabled and key name is empty, it means we are writing to the new tape
  if (isWriteSession && pool->encryption && !volInfo.encryptionKeyName.has_value()) {
    catalogue.Tape()->modifyTapeEncryptionKeyName({"ctaops", cta::utils::getShortHostname()}, volInfo.vid,
                                                  encStatus.keyName);
    encStatus.on = true;
  }

  if (encStatus.on) {
    m_drive.setEncryptionKey(encStatus.key);
  }
  else {
    /*
     * If tapeserver fails completely and leaves drive in dirty state, we should always clear
     * encryption key from the drive if data are to be written unencrypted.
     */
    disable(m_drive);
  }
  return encStatus;
}

//------------------------------------------------------------------------------
// disable
//------------------------------------------------------------------------------
bool EncryptionControl::disable(castor::tape::tapeserver::drive::DriveInterface& m_drive) {
  return m_drive.clearEncryptionKey();
}

namespace {
struct JsonObjectDeleter {
  void operator()(json_object *jo) { json_object_put(jo); }
};
}

namespace {
struct JsonTokenerDeleter {
  void operator()(json_tokener *jt) { json_tokener_free(jt); }
};
}

//------------------------------------------------------------------------------
// JSON Parsing
//------------------------------------------------------------------------------
EncryptionControl::EncryptionStatus EncryptionControl::parse_json_script_output(const std::string& input) {
  EncryptionStatus encryption_status;

  // Parse JSON response of the script
  std::unique_ptr<json_tokener, JsonTokenerDeleter> tok(json_tokener_new());
  json_tokener_set_flags(tok.get(), JSON_TOKENER_STRICT);
  std::unique_ptr<json_object, JsonObjectDeleter> jobj;
  int input_length = input.length();
  enum json_tokener_error jerr;
  do {
    jobj.reset(json_tokener_parse_ex(tok.get(), input.c_str(), input_length));
  } while ((jerr = json_tokener_get_error(tok.get())) == json_tokener_continue);

  if (jerr != json_tokener_success) {
    // Handle errors
    throw cta::exception::Exception("In EncryptionControl::parse_json_script_output: failed to parse "
                                    "encryption script's output.");
  }

  std::map<std::string, std::string> stdout_map = flatten_json_object_to_map("", jobj.get());

  if (
    stdout_map.find("key_name") == stdout_map.end() ||
    stdout_map.find("encryption_key") == stdout_map.end() ||
    stdout_map.find("message") == stdout_map.end()
    ) {
    throw cta::exception::Exception("In EncryptionControl::parse_json_script_output: invalid json interface.");
  }

  encryption_status.on = (!stdout_map["key_name"].empty() && !stdout_map["encryption_key"].empty());
  encryption_status.keyName = stdout_map["key_name"];
  encryption_status.key = stdout_map["encryption_key"];
  encryption_status.stdout = stdout_map["message"];
  return encryption_status;
}

std::map<std::string, std::string> EncryptionControl::flatten_json_object_to_map(const std::string& prefix,
                                                                                 json_object *jobj) {
  std::map<std::string, std::string> ret;

  json_object_object_foreach(jobj, key, val) {
    if (json_object_get_type(val) == json_type_object) {
      std::map<std::string, std::string> sec_map = flatten_json_object_to_map(key, val);
      ret.insert(sec_map.begin(), sec_map.end());
    }
    else {
      if (json_object_get_type(val) == json_type_string) {  // parse only string values at the deepest level
        ret[prefix + key] = json_object_get_string(val);
      }
    }
  }
  return ret;
}

std::string EncryptionControl::argsToString(std::list<std::string> args, const std::string& delimiter) {
  if (args.empty()) {
    return "";
  }
  std::ostringstream toBeReturned;
  std::copy(args.begin(), --args.end(),
            std::ostream_iterator<std::string>(toBeReturned, delimiter.c_str()));
  toBeReturned << *args.rbegin();

  return toBeReturned.str();
}

}  // namespace daemon
}  // namespace tapeserver
}  // namespace tape
}  // namespace castor

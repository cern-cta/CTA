/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <algorithm>
#include <memory>

#include "common/exception/Exception.hpp"
#include "common/threading/SubProcess.hpp"
#include "common/utils/Regex.hpp"
#include "EncryptionControl.hpp"

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
  if (m_path.size() && m_path[0] != '/') {
    cta::exception::Exception ex("In EncryptionControl::EncryptionControl: the script path is not absolute: ");
    ex.getMessage() << m_path;
  }
}

//------------------------------------------------------------------------------
// enable
//------------------------------------------------------------------------------
auto EncryptionControl::enable(castor::tape::tapeserver::drive::DriveInterface& m_drive,
                               const std::string& vid, SetTag st) -> EncryptionStatus {

  EncryptionStatus encStatus;
  if (m_path.empty()) {
    if (m_useEncryption) {
      //if encryption is enabled, an external script is required
      cta::exception::Exception ex;
      ex.getMessage() << "In EncryptionControl::enableEncryption: "
                         "failed to enable encryption: path provided is empty but tapeserver is configured to use encryption";
      throw ex;
    }
    encStatus = {false, "", "", ""};
    return encStatus;
  }

  std::list<std::string> args({m_path, "--vid", vid});

  switch (st) {
    case SetTag::NO_SET_TAG:
      break;
    case SetTag::SET_TAG:
      args.emplace_back("--set-tag");
      break;
  }
  cta::threading::SubProcess sp(m_path, args);
  sp.wait();
  if (sp.wasKilled() || sp.exitValue() != EXIT_SUCCESS) {
    cta::exception::Exception ex;
    ex.getMessage() << "In EncryptionControl::enableEncryption: "
                       "failed to enable encryption: ";
    if (sp.wasKilled()) {
      ex.getMessage() << "script was killed with signal: " << sp.killSignal();
    }
    else {
      ex.getMessage() << "script returned: " << sp.exitValue();
    }
    ex.getMessage() << " called=" << "\'" << argsToString(args, " ") << "\'"
                    << " stdout=" << sp.stdout()
                    << " stderr=" << sp.stderr();
    throw ex;
  }
  encStatus = parse_json_script_output(sp.stdout());
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
    cta::exception::Exception ex("In EncryptionControl::parse_json_script_output: failed to parse "
                                 "encryption script's output.");
    throw ex;
  }

  std::map<std::string, std::string> stdout_map = flatten_json_object_to_map("", jobj.get());

  if (
    stdout_map.find("key_id") == stdout_map.end() ||
    stdout_map.find("encryption_key") == stdout_map.end() ||
    stdout_map.find("message") == stdout_map.end()
    ) {
    cta::exception::Exception ex("In EncryptionControl::parse_json_script_output: invalid json interface.");
    throw ex;
  }

  encryption_status.on = (!stdout_map["key_id"].empty() && !stdout_map["encryption_key"].empty());
  encryption_status.keyName = stdout_map["key_id"];
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

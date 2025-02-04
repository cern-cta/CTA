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

#include <filesystem>
#include <sstream>
#include <iostream>

// #include "frontend/common/AdminCmd.hpp"
#include "CtaAdminCmdNonStreaming.hpp"
#include <cmdline/CtaAdminTextFormatter.hpp>
#include "tapeserver/daemon/common/TapedConfiguration.hpp"

// GLOBAL VARIABLES : used to pass information between main thread and stream handler thread

// global synchronisation flag
std::atomic<bool> isHeaderSent(false);

// initialise an output buffer of 1000 lines
cta::admin::TextFormatter formattedText(1000);

const std::filesystem::path DEFAULT_CLI_CONFIG = "/etc/cta/cta-cli.conf"; // should this be anything different for the grpc frontend?

/*!
* User error exception
*/
class UserException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};


namespace cta::admin {

std::atomic<bool> CtaAdminCmdNonStreaming::is_json(false);
std::atomic<bool> CtaAdminCmdNonStreaming::is_first_record(true);

CtaAdminCmdNonStreaming::CtaAdminCmdNonStreaming(int argc, const char* const* const argv) : m_execname(argv[0]) {
  auto& admincmd = *(m_request.mutable_admincmd());

  admincmd.set_client_version(CTA_VERSION);
  admincmd.set_protobuf_tag(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);

  // Strip path from execname

  size_t p = m_execname.find_last_of('/');
  if (p != std::string::npos) {
    m_execname.erase(0, p + 1);
  }

  // Parse the command

  cmdLookup_t::const_iterator cmd_it;

  // Client-side only options

  int argno = 1;

  if (argc <= argno) {
    throwUsage();
  }

  if (std::string(argv[argno]) == "--json") {
    is_json = true;
    ++argno;
  }

  if (std::string(argv[argno]) == "--config") {
    m_config = argv[++argno];
    ++argno;
  }

  // Commands, subcommands and server-side options

  if (argc <= argno || (cmd_it = cmdLookup.find(argv[argno++])) == cmdLookup.end()) {
    throwUsage();
  } else {
    admincmd.set_cmd(cmd_it->second);
  }

  // Help is a special subcommand which suppresses errors and prints usage

  if (argc > argno && std::string(argv[argno]) == "help") {
    throwUsage();
  }

  // Parse the subcommand

  bool has_subcommand = cmdHelp.at(admincmd.cmd()).has_subcommand();

  if (has_subcommand) {
    subcmdLookup_t::const_iterator subcmd_it;

    if (argc <= argno) {
      throwUsage("Missing subcommand");
    } else if ((subcmd_it = subcmdLookup.find(argv[argno])) == subcmdLookup.end()) {
      throwUsage(std::string("Invalid subcommand: ") + argv[argno]);
    } else {
      admincmd.set_subcmd(subcmd_it->second);
    }
  }

  // Parse the options

  auto option_list_it = cmdOptions.find(cmd_key_t {admincmd.cmd(), admincmd.subcmd()});

  if (option_list_it == cmdOptions.end()) {
    throwUsage(std::string("Invalid subcommand: ") + argv[argno]);
  }

  parseOptions(has_subcommand ? argno + 1 : argno, argc, argv, option_list_it->second); // parseOptions will populate m_request.admincmd member fields
}

const std::string CtaAdminCmdNonStreaming::getConfigFilePath() const {
  std::filesystem::path config_file = DEFAULT_CLI_CONFIG;

  if (std::getenv("HOME")) {
    const std::filesystem::path home = std::getenv("HOME");
    const std::filesystem::path home_dir_config_file = home / ".cta/cta-cli.conf";
    if (std::filesystem::exists(home_dir_config_file)) {
      config_file = home_dir_config_file;
    }
  }
  if (m_config) {
    config_file = m_config.value();
  }
  return config_file;
}

// Implement the send() method here, by wrapping the Admin rpc call
void CtaAdminCmdNonStreaming::send() {
  // Validate the Protocol Buffer
  try {
    cta::admin::validateCmd(m_request.admincmd());
  }
  catch (std::runtime_error& ex) {
    throwUsage(ex.what());
  }

  // now construct the Admin call
  // get a client stub in order to make it
  grpc::ClientContext context;
  grpc::Status status;
  cta::xrd::Response response;
  // get the grpc endpoint from the config? but for now, use 
  std::string endpoint("cta-frontend:10955");
  std::unique_ptr<cta::xrd::CtaRpc::Stub> client_stub = cta::xrd::CtaRpc::NewStub(grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()));

  // do all the filling in of the command to send
  status = client_stub->Admin(&context, m_request, &response);
  // then check the response result, if it's not an error response we are good to continue
  if (!status.ok()) {
    switch (response.type()) {
      case cta::xrd::Response::RSP_ERR_PROTOBUF:
      case cta::xrd::Response::RSP_ERR_CTA:
      case cta::xrd::Response::RSP_ERR_USER:
      default:
       throw std::runtime_error(response.message_txt());
       break;
    }
  }
}

void CtaAdminCmdNonStreaming::parseOptions(int start, int argc, const char* const* const argv, const cmd_val_t& options) {
  for (int i = start; i < argc; ++i) {
    int opt_num = i - start;

    cmd_val_t::const_iterator opt_it;

    // Scan options for a match

    for (opt_it = options.begin(); opt_it != options.end(); ++opt_it) {
      // Special case of OPT_CMD type has an implicit key
      if (opt_num-- == 0 && opt_it->get_type() == Option::OPT_CMD) {
        // Check if the value is '--all'
        if (std::string(argv[i]) == "--all" || std::string(argv[i]) == "-a") {
          // Find the OPT_FLAG type --all option explicitly
          auto flag_it = std::find_if(options.begin(), options.end(), [](const Option& opt) {
            return opt.get_type() == Option::OPT_FLAG && (opt == opt_all);
          });
          if (flag_it != options.end()) {
            addOption(*flag_it, "");  // Add --all as a flag option
            continue;                 // Move to the next argument
          }
          throwUsage("Invalid use of '--all'");
        }
        // Normal implicit key handling for CMD
        break;
      }
      // Regular matching
      if (*opt_it == argv[i]) {
        break;
      }
    }
    if (opt_it == options.end()) {
      throwUsage(std::string("Invalid option: ") + argv[i]);
    }
    if ((i += opt_it->num_params()) == argc) {
      throw std::runtime_error(std::string(argv[i - 1]) + " expects a parameter: " + opt_it->help());
    }

    addOption(*opt_it, argv[i]);
  }
}

void CtaAdminCmdNonStreaming::addOption(const Option& option, const std::string& value) {
  auto admincmd_ptr = m_request.mutable_admincmd();

  switch (option.get_type()) {
    case Option::OPT_CMD:
    case Option::OPT_STR: {
      auto key = strOptions.at(option.get_key());
      auto new_opt = admincmd_ptr->add_option_str();
      new_opt->set_key(key);
      if (option == opt_drivename_cmd && value == "first") {
        try {
          new_opt->set_value(cta::tape::daemon::common::TapedConfiguration::getFirstDriveName());
        } catch (cta::exception::Exception& ex) {
          throw std::runtime_error(
            "Could not find a taped configuration file. This option should only be run from a tapeserver.");
        }
      } else {
        new_opt->set_value(value);
      }
      break;
    }
    case Option::OPT_STR_LIST: {
      auto key = strListOptions.at(option.get_key());
      auto new_opt = admincmd_ptr->add_option_str_list();
      new_opt->set_key(key);
      readListFromFile(*new_opt, value);
      break;
    }
    case Option::OPT_FLAG:
    case Option::OPT_BOOL: {
      auto key = boolOptions.at(option.get_key());
      auto new_opt = admincmd_ptr->add_option_bool();
      new_opt->set_key(key);
      if (option.get_type() == Option::OPT_FLAG || value == "true") {
        new_opt->set_value(true);
      } else if (value == "false") {
        new_opt->set_value(false);
      } else {
        throw std::runtime_error(value + " is not a boolean value: " + option.help());
      }
      break;
    }
    case Option::OPT_UINT:
      try {
        auto key = uint64Options.at(option.get_key());
        int64_t val_int = std::stol(value);
        if (val_int < 0) {
          throw std::out_of_range("value is negative");
        }
        auto new_opt = admincmd_ptr->add_option_uint64();
        new_opt->set_key(key);
        new_opt->set_value(val_int);
        break;
      } catch (std::invalid_argument&) {
        throw std::runtime_error(value + " is not a valid uint64: " + option.help());
      } catch (std::out_of_range&) {
        throw std::runtime_error(value + " is out of range: " + option.help());
      }
  }
}

void CtaAdminCmdNonStreaming::readListFromFile(cta::admin::OptionStrList& str_list, const std::string& filename) {
  std::ifstream file(filename);
  if (file.fail()) {
    throw std::runtime_error("Unable to open file " + filename);
  }

  std::string line;

  while (std::getline(file, line)) {
    // Strip out comments
    auto pos = line.find('#');
    if (pos != std::string::npos) {
      line.resize(pos);
    }

    // Extract the list items
    std::stringstream ss(line);
    while (!ss.eof()) {
      std::string item;
      ss >> item;
      // skip blank lines or lines consisting only of whitespace
      if (item.empty()) {
        continue;
      }

      if (str_list.key() == OptionStrList::FILE_ID) {
        // Special handling for file id lists. The output from "eos find --fid <fid> /path" is:
        //   path=/path fid=<fid>
        // We discard everything except the list of fids. <fid> is a zero-padded hexadecimal number,
        // but in the CTA catalogue we store disk IDs as a decimal string, so we need to convert it.
        if (item.substr(0, 4) != "fid=") {
          continue;
        }
        auto fid = item.substr(4);
        if (!utils::isValidID(fid)) {
          throw std::runtime_error(fid + " is not a valid file ID");
        }
        str_list.add_item(fid);
      } else {
        // default case: add all items
        str_list.add_item(item);
      }
    }
  }
}

void CtaAdminCmdNonStreaming::throwUsage(const std::string& error_txt) const {
  std::stringstream help;
  const auto& admincmd = m_request.admincmd().cmd();

  if (error_txt != "") {
    help << error_txt << std::endl;
  }

  if (admincmd == AdminCmd::CMD_NONE) {  // clang-format off
   // Command has not been set: show generic help
   help << "CTA Administration Tool" << std::endl << std::endl
        << "Usage: " << m_execname << " [--json] [--config <configpath>] <command> [<subcommand> [<option>...]]" << std::endl
        << "       " << m_execname << " <command> help" << std::endl << std::endl
        << "By default, the output is in tabular format. If the --json option is supplied, the output is a JSON array." << std::endl
        << "Commands have a long and short version. Subcommands (add/ch/ls/rm/etc.) do not have short versions. For" << std::endl
        << "detailed help on the options of each subcommand, type: " << m_execname << " <command> help" << std::endl << std::endl;
    //clang-format off
   // List help for each command in lexicographic order
   std::set<std::string> helpSet;
   for(auto &helpPair : cmdHelp) {
     helpSet.insert(helpPair.second.short_help());
   }
   for(auto &helpItem : helpSet) {
     help << "  " << m_execname << ' ' << helpItem << std::endl;
   }
 } else {
   // Command has been set: show command-specific help
   help << m_execname << ' ' << cmdHelp.at(admincmd).help();
 }

 throw std::runtime_error(help.str());
}

} // namespace cta::admin



/*!
* Start here
*
* @param    argc[in]    The number of command-line arguments
* @param    argv[in]    The command-line arguments
*/

int main(int argc, const char** argv) {
  using namespace cta::admin;

 try {
   // Parse the command line arguments
   CtaAdminCmdNonStreaming cmd(argc, argv);

   // Send the protocol buffer
   cmd.send();

   // Delete all global objects allocated by libprotobuf
   google::protobuf::ShutdownProtobufLibrary();

   return 0;
 } catch (...) {
   std::cerr << "Caught some exception" << std::endl;
 }

 return 1;
}

// I need to keep:
// exceptionThrowingMain
// code that parses the command
// code that creates a client stub and then sends the Admin rpc call to the server (we expect the server to be up and running and ready to serve the requests, at a known address)
// figure out later how to notify of the server address, I guess via the config is a good way


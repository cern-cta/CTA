/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2023 CERN
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
 
 
#include "CtaAdminGrpcCmd.hpp"
#include "AsyncClient.hpp"
#include "ClientTapeLsRequestHandler.hpp"
#include "KerberosAuthenticator.hpp"
#include "ClientNegotiationRequestHandler.hpp"
#include "utils.hpp"

#include "cmdline/CtaAdminTextFormatter.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/LogLevel.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/log/SyslogLogger.hpp"
#include "common/log/Logger.hpp"
#include "common/log/LogContext.hpp"
#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"
#include "tapeserver/daemon/TapedConfiguration.hpp"

#include "XrdSsiPbException.hpp"
#include "version.h"

#include <cryptopp/base64.h>
#include <getopt.h>

std::atomic<bool> cta::frontend::grpc::client::CtaAdminGrpcCmd::m_abIsJson(false);

/*
 * Convert AdminCmd <Cmd, SubCmd> pair to an integer so that it can be used in a switch statement
 */
constexpr unsigned int cmd_pair(cta::admin::AdminCmd::Cmd cmd, cta::admin::AdminCmd::SubCmd subcmd) {
   return (cmd << 16) + subcmd;
}

cta::frontend::grpc::client::CtaAdminGrpcCmd::CtaAdminGrpcCmd(int argc, char** argv) : m_strExecname(argv[0]) {
  auto &admincmd = *(m_request.mutable_admincmd());
  
  m_request.set_client_version(CTA_VERSION);
  m_request.set_client_protobuf_version(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);
  
  // Strip path from execname

  size_t p = m_strExecname.find_last_of('/');
  if(p != std::string::npos) m_strExecname.erase(0, p+1);

  // Parse the command

  cta::admin::cmdLookup_t::const_iterator cmd_it;

  // Client-side only options

  int argno = 1;

  if(argc <= argno) throwUsage();

  if(std::string(argv[argno]) == "--json") { CtaAdminGrpcCmd::m_abIsJson = true; ++argno; }

  // Commands, subcommands and server-side options

  if(argc <= argno || (cmd_it = cta::admin::cmdLookup.find(argv[argno++])) == cta::admin::cmdLookup.end()) {
     throwUsage();
  } else {
     admincmd.set_cmd(cmd_it->second);
  }

  // Help is a special subcommand which suppresses errors and prints usage
  
  if(argc > argno && std::string(argv[argno]) == "help") {
     throwUsage();
  }

  // Parse the subcommand

  bool has_subcommand = cta::admin::cmdHelp.at(admincmd.cmd()).has_subcommand();

  if(has_subcommand)
  {
     cta::admin::subcmdLookup_t::const_iterator subcmd_it;

     if(argc <= argno) {
        throwUsage("Missing subcommand");
     } else if((subcmd_it = cta::admin::subcmdLookup.find(argv[argno])) == cta::admin::subcmdLookup.end()) {
        throwUsage(std::string("Invalid subcommand: ") + argv[argno]);
     } else {
        admincmd.set_subcmd(subcmd_it->second);
     }
  }

  // Parse the options

  auto option_list_it = cta::admin::cmdOptions.find(cta::admin::cmd_key_t{ admincmd.cmd(), admincmd.subcmd() });

  if(option_list_it == cta::admin::cmdOptions.end()) {
     throwUsage(std::string("Invalid subcommand: ") + argv[argno]);
  }

  parseOptions(has_subcommand ? argno+1 : argno, argc, argv, option_list_it->second);
  
}

void cta::frontend::grpc::client::CtaAdminGrpcCmd::exe(cta::log::Logger& log, const std::string& strSslRoot,
                                               const std::string& strSslKey, const std::string& strSslCert,
                                               const std::string& strGrpcHost, const unsigned int& uiGrpcPort) {
  
  cta::log::LogContext lc(log);
  //
  cta::admin::validateCmd(m_request.admincmd());
  // gRPC stream server
  const std::string GRPC_SERVER {strGrpcHost + ":" + std::to_string(uiGrpcPort)};
  // Service name
  const std::string GSS_SPN {"grpc/" + strGrpcHost};
  // Common text formatter for commands output 
  cta::admin::TextFormatter textFormatter {1000};
  // Storage for the KRB token
  std::string strToken {""};
  // Encoded token to be send as part of metadata
  std::string strEncodedToken {""};
  // Create a default SSL ChannelCredentials
  std::shared_ptr<::grpc::ChannelCredentials> spChannelCreds = ::grpc::SslCredentials({strSslRoot, strSslKey, strSslCert});
  // Create a channel to the KRB-GSI negotiation service 
  std::shared_ptr<::grpc::Channel> spChannelNegotiation {::grpc::CreateChannel(GRPC_SERVER, spChannelCreds)};
  cta::frontend::grpc::client::AsyncClient<cta::frontend::rpc::Negotiation> clientNeg(log, spChannelNegotiation);
  try {
    strToken = clientNeg.exe<cta::frontend::grpc::client::NegotiationRequestHandler>(GSS_SPN)->token();
    /*
     * TODO: ???
     *        Move encoder to client::NegotiationRequestHandler
     *        or server::NegotiationRequestHandler
     *        or KerberosAuthenticator
     *       ???
     */
    grpc::utils::encode(strToken, strEncodedToken);

  } catch(const cta::exception::Exception& e) {
    /*
     * In case of any problems with the negotiation service,
     * log and stop the execution
     */
    lc.log(cta::log::CRIT, "In cta::frontend::grpc::client::CtaAdminGrpcCmd::exe(): Problem with the negotiation service.");
    throw; // rethrow
  }
  /*
   * Proceed with the request
   */
  // Map the <Cmd, SubCmd> to a method
  switch(cmd_pair(m_request.admincmd().cmd(), m_request.admincmd().subcmd())) {
    case cmd_pair(cta::admin::AdminCmd::CMD_TAPE, cta::admin::AdminCmd::SUBCMD_LS):
      try {
        /*
         * Channel arguments can be overriden e.g: 
         * ::grpc::ChannelArguments channelArgs;ClientContext
         * channelArgs.SetString(GRPC_SSL_TARGET_NAME_OVERRIDE_ARG, "ok.org");
         * std::shared_ptr<::grpc::Channel> spChannel {::grpc::CreateCustomChannel("0.0.0.0:17017", spCredentials, channelArgs)};
         */
        //--- ref: https://grpc.io/docs/guides/auth/#credential-types
        /*
         * Credentials can be of two types:
         * - Channel credentials, which are attached to a Channel, such as SSL credentials.
         * - Call credentials, which are attached to a call (or ClientContext in C++).
         * CompositeChannelCredentials associates a ChannelCredentials and a CallCredentials
         * to create a new ChannelCredentials
         */
         /*
          * Individual CallCredentials can also be composed using CompositeCallCredentials.
          * The resulting CallCredentials when used in a call will trigger the sending of
          * the authentication data associated with the two CallCredentials.
          * !!! It dose not work with InsecureChannelCredentials !!!
          */
        //---
        /*
         * Call credentails can be set manually in a client's conspCallCredentialstext
         * e.g.:
         *   m_ctx.set_credentials(spCallCredentials);
         */
        std::shared_ptr<::grpc::CallCredentials> spCallCredentials =
          ::grpc::MetadataCredentialsFromPlugin(std::unique_ptr<::grpc::MetadataCredentialsPlugin>(
          new KerberosAuthenticator(strEncodedToken)));
        std::shared_ptr<::grpc::ChannelCredentials> spCompositeCredentials = ::grpc::CompositeChannelCredentials(spChannelCreds, spCallCredentials);
        std::shared_ptr<::grpc::Channel> spChannel {::grpc::CreateChannel(GRPC_SERVER, spCompositeCredentials)};
        cta::frontend::grpc::client::AsyncClient<cta::frontend::rpc::CtaRpcStream> client(log, spChannel);
        // Execute the TapeLs command
        client.exe<cta::frontend::grpc::client::TapeLsRequestHandler>(textFormatter, m_request);
      } catch(const cta::exception::Exception& e) {
        lc.log(cta::log::CRIT, "In cta::frontend::grpc::client::CtaAdminGrpcCmd::exe(): Problem with the execution of the tape ls command.");
        throw; // rethrow
      }
      break;
    default:
      throw XrdSsiPb::PbException(
        "Admin command pair <"
        + cta::admin::AdminCmd_Cmd_Name(m_request.admincmd().cmd())
        + ", "
        + cta::admin::AdminCmd_SubCmd_Name(m_request.admincmd().subcmd())
        + "> is not implemented."
      );
  }
    
}

void cta::frontend::grpc::client::CtaAdminGrpcCmd::parseOptions(int start, int argc, char** argv, const cta::admin::cmd_val_t& options) {
 for(int i = start; i < argc; ++i) {
    int opt_num = i-start;

    cta::admin::cmd_val_t::const_iterator opt_it;

    // Scan options for a match

    for(opt_it = options.begin(); opt_it != options.end(); ++opt_it) {
       // Special case of OPT_CMD type has an implicit key
       if(opt_num-- == 0 && opt_it->get_type() == cta::admin::Option::OPT_CMD) break;

       if(*opt_it == argv[i]) break;
    }
    if(opt_it == options.end()) {
       throwUsage(std::string("Invalid option: ") + argv[i]);
    }
    if((i += opt_it->num_params()) == argc) {
       throw std::runtime_error(std::string(argv[i-1]) + " expects a parameter: " + opt_it->help());
    }

    addOption(*opt_it, argv[i]);
 }
}

void cta::frontend::grpc::client::CtaAdminGrpcCmd::addOption(const cta::admin::Option& option, const std::string& strValue) {
 auto admincmd_ptr = m_request.mutable_admincmd();

 switch(option.get_type()) {
    case cta::admin::Option::OPT_CMD:
    case cta::admin::Option::OPT_STR: {
       auto key = cta::admin::strOptions.at(option.get_key());
       auto new_opt = admincmd_ptr->add_option_str();
       new_opt->set_key(key);
       if (option == cta::admin::opt_drivename_cmd && strValue == "first") {
           try {
           new_opt->set_value(
               cta::tape::daemon::TapedConfiguration::getFirstDriveName());
           } catch (cta::exception::Exception &ex){
               throw std::runtime_error("Could not find a taped configuration file. This option should only be run from a tapeserver.");
           }
       } else {
          new_opt->set_value(strValue);
       }
       break;
    }
    case cta::admin::Option::OPT_STR_LIST: {
       auto key = cta::admin::strListOptions.at(option.get_key());
       auto new_opt = admincmd_ptr->add_option_str_list();
       new_opt->set_key(key);
       readListFromFile(*new_opt, strValue);
       break;
    }
    case cta::admin::Option::OPT_FLAG:
    case cta::admin::Option::OPT_BOOL: {
       auto key = cta::admin::boolOptions.at(option.get_key());
       auto new_opt = admincmd_ptr->add_option_bool();
       new_opt->set_key(key);
       if(option.get_type() == cta::admin::Option::OPT_FLAG || strValue == "true") {
          new_opt->set_value(true);
       } else if(strValue == "false") {
          new_opt->set_value(false);
       } else {
          throw std::runtime_error(strValue + " is not a boolean value: " + option.help());
       }
       break;
    }
    case cta::admin::Option::OPT_UINT: try {
       auto key = cta::admin::uint64Options.at(option.get_key());
       int64_t val_int = std::stol(strValue);
       if(val_int < 0) throw std::out_of_range("value is negative");
       auto new_opt = admincmd_ptr->add_option_uint64();
       new_opt->set_key(key);
       new_opt->set_value(val_int);
       break;
    } catch(std::invalid_argument &) {
       throw std::runtime_error(strValue + " is not a valid uint64: " + option.help());
    } catch(std::out_of_range &) {
       throw std::runtime_error(strValue + " is out of range: " + option.help());
    }
 }
}

void cta::frontend::grpc::client::CtaAdminGrpcCmd::readListFromFile(cta::admin::OptionStrList &str_list, const std::string &filename)
{
  std::ifstream file(filename);
  if (file.fail()) {
     throw std::runtime_error("Unable to open file " + filename);
  }

  std::string line;

  while(std::getline(file, line)) {
     // Strip out comments
     auto pos = line.find('#');
     if(pos != std::string::npos) {
        line.resize(pos);
     }

     // Extract the list items
     std::stringstream ss(line);
     while(!ss.eof()) {
        std::string item;
        ss >> item;
        // skip blank lines or lines consisting only of whitespace
        if(item.empty()) continue;

        if(str_list.key() == cta::admin::OptionStrList::FILE_ID) {
           // Special handling for file id lists. The output from "eos find --fid <fid> /path" is:
           //   path=/path fid=<fid>
           // We discard everything except the list of fids. <fid> is a zero-padded hexadecimal number,
           // but in the CTA catalogue we store disk IDs as a decimal string, so we need to convert it.
           if(item.substr(0, 4) == "fid=") {
              auto fid = strtol(item.substr(4).c_str(), nullptr, 16);
              if(fid < 1 || fid == LONG_MAX) {
                throw std::runtime_error(item + " is not a valid file ID");
              }
              str_list.add_item(std::to_string(fid));
           } else {
              continue;
           }
        } else {
           // default case: add all items
           str_list.add_item(item);
        }
     }
  }
}

void cta::frontend::grpc::client::CtaAdminGrpcCmd::throwUsage(const std::string &strError) const {
  std::stringstream help;
  const auto &admincmd = m_request.admincmd().cmd();

  if(strError != "") {
     help << strError << std::endl;
  }

  if(admincmd == cta::admin::AdminCmd::CMD_NONE) {
     // Command has not been set: show generic help
     help << "CTA Administration Tool" << std::endl << std::endl
          << "Usage: " << m_strExecname << " [--json] <command> [<subcommand> [<option>...]]" << std::endl
          << "       " << m_strExecname << " <command> help" << std::endl << std::endl
          << "By default, the output is in tabular format. If the --json option is supplied, the output is a JSON array." << std::endl
          << "Commands have a long and short version. Subcommands (add/ch/ls/rm/etc.) do not have short versions. For" << std::endl
          << "detailed help on the options of each subcommand, type: " << m_strExecname << " <command> help" << std::endl << std::endl;

     // List help for each command in lexicographic order
     std::set<std::string> helpSet;
     for(auto &helpPair : cta::admin::cmdHelp) {
        helpSet.insert(helpPair.second.short_help());
     }
     for(auto &helpItem : helpSet) {
        help << "  " << m_strExecname << ' ' << helpItem << std::endl;
     }
  } else {
     // Command has been set: show command-specific help
     help << m_strExecname << ' ' << cta::admin::cmdHelp.at(admincmd).help();
  }

  throw std::runtime_error(help.str());
} 

int main(const int argc,  char** argv) {
  
  // Parse the command line arguments
  const int CMD_SSL_ARGC = 11;
  int iOptIdx = 0;
  char c = 0;
  std::string strExecName = argv[0];
  std::string strSslRootFile;
  std::string strSslKeyFile;
  std::string strSslCertFile;
  // cert
  std::string strSslRoot;
  std::string strSslKey;
  std::string strSslCert;
  unsigned int iSllAgrcEnd = ((CMD_SSL_ARGC > argc) ? argc : CMD_SSL_ARGC);
  unsigned int iCmdAgrcBegin = ((CMD_SSL_ARGC > argc) ? 0 : CMD_SSL_ARGC);
  
  std::string strGrpcHost;
  unsigned int uiGrpcPort = 0;
  
  /*
   * TODO: configuration script
   * !!! Parameters ONLY for test purpose:
   * - get ssl related cert/key
   * - get host & port of gRPC server 
   */
  std::vector<char*> vArgvSSLgRPC(argv, argv + iSllAgrcEnd);
  std::vector<char*> vArgvCMD(argv + iCmdAgrcBegin, argv + argc);
  // Copy exec name
  vArgvCMD.insert(vArgvCMD.begin(), argv[0]);
  
  static struct option longOpt[] = {
    { "cacert", required_argument, nullptr, 'r' }, // root
    { "key",    required_argument, nullptr, 'k' },
    { "cert",   required_argument, nullptr, 'c' },
    { "host",   required_argument, nullptr, 's' },
    { "port",   required_argument, nullptr, 'p' },
    { "help",   no_argument,       nullptr, 'h' },
    { nullptr,  0,                 nullptr, 0 }
  };
  
  while ((c = getopt_long(vArgvSSLgRPC.size(), vArgvSSLgRPC.data(), ":r:k:c:s:p:h", longOpt, &iOptIdx)) != -1) {
    switch(c) {
      case 'r':
        strSslRootFile = std::string(optarg);
        break;
      case 'k':
        strSslKeyFile = std::string(optarg);
        break;
      case 'c':
        strSslCertFile = std::string(optarg);
        break;
      case 's':
        strGrpcHost = std::string(optarg);
        break;
      case 'p':
        try {
          int iVal = std::stoi(optarg);
          if(iVal < 0) {
            throw std::invalid_argument("Invalid argument");
          }
          uiGrpcPort = static_cast<unsigned int>(iVal);
        } catch (const std::invalid_argument& ia) {
          std::cerr << strExecName << ": command line error: invalid argument for option -p" << std::endl;
          return EXIT_FAILURE;
        }
        break;
      case 'h':
        std::cout << "CTA Admin gRPC" << std::endl << std::endl
                  << "Usage: " << strExecName << " <SSL-OPTION>... --host <NAME> --port <NUMBER> <command>" << std::endl
                  << "Options:" << std::endl
                  << "  -r, --cacert <FILE>  ca-cert file location" << std::endl
                  << "  -k, --key <FILE>     key file location" << std::endl
                  << "  -c, --cert <FILE>    cert file location" << std::endl
                  << "  -s, --host <STRING>  gRPC server host name/adress"  << std::endl
                  // << "  -p, --port<NUMBER>  gRPC server TCP port (default: 17017)"  << std::endl
                  << "  -p, --port <NUMBER>  gRPC server TCP port"  << std::endl
                  // << "  -v, --version           print version and exit" << std::endl
                  << "  -h, --help          print this help and exit" << std::endl;
        return EXIT_SUCCESS;
      case ':': // Missing parameter
        std::cerr << strExecName << ": command line error: option: '-" << (char)optopt << "' requires an argument"<< std::endl
                  << "Try '" << strExecName << " -h' for more information." << std::endl;
        return EXIT_FAILURE;
        // break;
      default /* '?' */:
        std::cerr << strExecName << ": command line error: unrecognised option '-" << (char)optopt << "'"<< std::endl
                  << "Try '" << strExecName << " -h' for more information." << std::endl;
        return EXIT_FAILURE;
    }
  }
  
  if (strSslRootFile.empty()) {
    std::cerr << strExecName << ": the ca-cert file is unspecified" << std::endl
          << "Try '" << strExecName << " -h' for more information." << std::endl;
    return EXIT_FAILURE;
  }
  if (strSslKeyFile.empty()) {
    std::cerr << strExecName << ": the key file is unspecified" << std::endl
          << "Try '" << strExecName << " -h' for more information." << std::endl;
    return EXIT_FAILURE;
  }
  if (strSslCertFile.empty()) {
    std::cerr << strExecName << ": the cert file is unspecified" << std::endl
          << "Try '" << strExecName << " -h' for more information." << std::endl;
    return EXIT_FAILURE;
  }
  if (strGrpcHost.empty()) {
    std::cerr << strExecName << ": the gRPC server host name is unspecified" << std::endl
          << "Try '" << strExecName << " -h' for more information." << std::endl;
    return EXIT_FAILURE;
  }
  if (!uiGrpcPort) {
    std::cerr << strExecName << ": the gRPC server TCP port is unspecified" << std::endl
          << "Try '" << strExecName << " -h' for more information." << std::endl;
    return EXIT_FAILURE;
  }
  // Reading cert from file
  try {
    cta::frontend::grpc::utils::read(strSslRootFile, strSslRoot);
    cta::frontend::grpc::utils::read(strSslKeyFile, strSslKey);
    cta::frontend::grpc::utils::read(strSslCertFile, strSslCert);
  } catch(const cta::exception::Exception& ex) {
    std::cerr << strExecName << ": problem while reading ssl cert/key file - " << ex.getMessageValue() << std::endl;
    return EXIT_FAILURE;
  }
  // Command parsing
  cta::frontend::grpc::client::CtaAdminGrpcCmd ctaAdminGrpcCmd(vArgvCMD.size(), vArgvCMD.data());
  
  // Init logger
  const std::string strShortHostname = cta::utils::getShortHostname();
  
  // For now only StdoutLogger
  std::unique_ptr<cta::log::Logger> upLog = std::make_unique<cta::log::StdoutLogger>(strShortHostname, "grpc-streamserver");
  cta::log::LogContext lc(*upLog);
  // Execute the command
  try {
    ctaAdminGrpcCmd.exe(*upLog, strSslRoot, strSslKey, strSslCert, strGrpcHost, uiGrpcPort);
  } catch(const cta::exception::Exception& e) {
    lc.log(cta::log::CRIT, e.getMessageValue());
    return EXIT_FAILURE;
  } catch(const std::exception& e) {
    lc.log(cta::log::CRIT, e.what());
    return EXIT_FAILURE;
  }
  
  return EXIT_SUCCESS;
}

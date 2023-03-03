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

#include <XrdSsiPbLog.hpp>
#include <XrdSsiPbIStreamBuffer.hpp>

#include <cmdline/CtaAdminCmd.hpp>
#include <cmdline/CtaAdminTextFormatter.hpp>


// GLOBAL VARIABLES : used to pass information between main thread and stream handler thread

// global synchronisation flag
std::atomic<bool> isHeaderSent(false);

// initialise an output buffer of 1000 lines
cta::admin::TextFormatter formattedText(1000);

const std::filesystem::path DEFAULT_CLI_CONFIG = "/etc/cta/cta-cli.conf";
std::string tp_config_file = "/etc/cta/TPCONFIG";


namespace XrdSsiPb {

/*!
 * User error exception
 */
class UserException : public std::runtime_error
{
public:
  UserException(const std::string &err_msg) : std::runtime_error(err_msg) {}
};


/*!
 * Alert callback.
 *
 * Defines how Alert messages should be logged
 */
template<>
void RequestCallback<cta::xrd::Alert>::operator()(const cta::xrd::Alert &alert)
{
   std::cout << "AlertCallback():" << std::endl;
   Log::DumpProtobuf(Log::PROTOBUF, &alert);
}


/*!
 * Data/Stream callback.
 *
 * Defines how incoming records from the stream should be handled
 */
template<>
void IStreamBuffer<cta::xrd::Data>::DataCallback(cta::xrd::Data record) const
{
   using namespace cta::xrd;
   using namespace cta::admin;

   // Wait for primary response to be handled before allowing stream response
   while(!isHeaderSent) { std::this_thread::yield(); }

   // Output results in JSON format for parsing by a script
   if(CtaAdminCmd::isJson())
   {
      std::cout << CtaAdminCmd::jsonDelim();

      switch(record.data_case()) {
         case Data::kAdlsItem:      std::cout << Log::DumpProtobuf(&record.adls_item());    break;
         case Data::kAflsItem:      std::cout << Log::DumpProtobuf(&record.afls_item());    break;
         case Data::kAflsSummary:   std::cout << Log::DumpProtobuf(&record.afls_summary()); break;
         case Data::kArlsItem:      std::cout << Log::DumpProtobuf(&record.arls_item());    break;
         case Data::kDrlsItem:      std::cout << Log::DumpProtobuf(&record.drls_item());    break;
         case Data::kFrlsItem:      std::cout << Log::DumpProtobuf(&record.frls_item());    break;
         case Data::kFrlsSummary:   std::cout << Log::DumpProtobuf(&record.frls_summary()); break;
         case Data::kGmrlsItem:     std::cout << Log::DumpProtobuf(&record.gmrls_item());   break;
         case Data::kLpaItem:       std::cout << Log::DumpProtobuf(&record.lpa_item());     break;
         case Data::kLpaSummary:    std::cout << Log::DumpProtobuf(&record.lpa_summary());  break;
         case Data::kLprItem:       std::cout << Log::DumpProtobuf(&record.lpr_item());     break;
         case Data::kLprSummary:    std::cout << Log::DumpProtobuf(&record.lpr_summary());  break;
         case Data::kLllsItem:      std::cout << Log::DumpProtobuf(&record.llls_item());    break;
         case Data::kMplsItem:      std::cout << Log::DumpProtobuf(&record.mpls_item());    break;
         case Data::kRelsItem:      std::cout << Log::DumpProtobuf(&record.rels_item());    break;
         case Data::kRmrlsItem:     std::cout << Log::DumpProtobuf(&record.rmrls_item());   break;
         case Data::kAmrlsItem:     std::cout << Log::DumpProtobuf(&record.amrls_item());   break;
         case Data::kSqItem:        std::cout << Log::DumpProtobuf(&record.sq_item());      break;
         case Data::kSclsItem:      std::cout << Log::DumpProtobuf(&record.scls_item());    break;
         case Data::kTalsItem:      std::cout << Log::DumpProtobuf(&record.tals_item());    break;
         case Data::kTflsItem:      std::cout << Log::DumpProtobuf(&record.tfls_item());    break;
         case Data::kTplsItem:      std::cout << Log::DumpProtobuf(&record.tpls_item());    break;
         case Data::kDslsItem:      std::cout << Log::DumpProtobuf(&record.dsls_item());    break;
         case Data::kDilsItem:      std::cout << Log::DumpProtobuf(&record.dils_item());    break;
         case Data::kDislsItem:     std::cout << Log::DumpProtobuf(&record.disls_item());   break;
         case Data::kVolsItem:      std::cout << Log::DumpProtobuf(&record.vols_item());    break;
         case Data::kVersionItem:   std::cout << Log::DumpProtobuf(&record.version_item()); break;
         case Data::kMtlsItem:      std::cout << Log::DumpProtobuf(&record.mtls_item());    break;
         case Data::kRtflsItem:     std::cout << Log::DumpProtobuf(&record.rtfls_item());   break;
         case Data::kSilsItem:      std::cout << Log::DumpProtobuf(&record.sils_item());    break;
         default:
            throw std::runtime_error("Received invalid stream data from CTA Frontend.");
      }
   }
   // Format results in a tabular format for a human
   else switch(record.data_case()) {
         case Data::kAdlsItem:      formattedText.print(record.adls_item());    break;
         case Data::kArlsItem:      formattedText.print(record.arls_item());    break;
         case Data::kDrlsItem:      formattedText.print(record.drls_item());    break;
         case Data::kFrlsItem:      formattedText.print(record.frls_item());    break;
         case Data::kFrlsSummary:   formattedText.print(record.frls_summary()); break;
         case Data::kGmrlsItem:     formattedText.print(record.gmrls_item());   break;
         case Data::kLpaItem:       formattedText.print(record.lpa_item());     break;
         case Data::kLpaSummary:    formattedText.print(record.lpa_summary());  break;
         case Data::kLprItem:       formattedText.print(record.lpr_item());     break;
         case Data::kLprSummary:    formattedText.print(record.lpr_summary());  break;
         case Data::kLllsItem:      formattedText.print(record.llls_item());    break;
         case Data::kMplsItem:      formattedText.print(record.mpls_item());    break;
         case Data::kRelsItem:      formattedText.print(record.rels_item());    break;
         case Data::kRmrlsItem:     formattedText.print(record.rmrls_item());   break;
         case Data::kAmrlsItem:     formattedText.print(record.amrls_item());   break;
         case Data::kSqItem:        formattedText.print(record.sq_item());      break;
         case Data::kSclsItem:      formattedText.print(record.scls_item());    break;
         case Data::kTalsItem:      formattedText.print(record.tals_item());    break;
         case Data::kTflsItem:      formattedText.print(record.tfls_item());    break;
         case Data::kTplsItem:      formattedText.print(record.tpls_item());    break;
         case Data::kDslsItem:      formattedText.print(record.dsls_item());    break;
         case Data::kDilsItem:      formattedText.print(record.dils_item());    break;
         case Data::kDislsItem:     formattedText.print(record.disls_item());   break;
         case Data::kVolsItem:      formattedText.print(record.vols_item());    break;
         case Data::kVersionItem:   formattedText.print(record.version_item()); break;
         case Data::kMtlsItem:      formattedText.print(record.mtls_item());    break;
         case Data::kRtflsItem:     formattedText.print(record.rtfls_item());   break;
         default:
            throw std::runtime_error("Received invalid stream data from CTA Frontend.");
   }
}

} // namespace XrdSsiPb



namespace cta {
namespace admin {

std::atomic<bool> CtaAdminCmd::is_json(false);
std::atomic<bool> CtaAdminCmd::is_first_record(true);

CtaAdminCmd::CtaAdminCmd(int argc, const char *const *const argv) :
   m_execname(argv[0])
{
   auto &admincmd = *(m_request.mutable_admincmd());

   admincmd.set_client_version(CTA_VERSION);
   admincmd.set_protobuf_tag(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);

   // Strip path from execname

   size_t p = m_execname.find_last_of('/');
   if(p != std::string::npos) m_execname.erase(0, p+1);

   // Parse the command

   cmdLookup_t::const_iterator cmd_it;

   // Client-side only options

   int argno = 1;

   if(argc <= argno) throwUsage();

   if(std::string(argv[argno]) == "--json") { is_json = true; ++argno; }

   if(std::string(argv[argno]) == "--config") { m_config = argv[++argno]; ++argno; }

   // Commands, subcommands and server-side options

   if(argc <= argno || (cmd_it = cmdLookup.find(argv[argno++])) == cmdLookup.end()) {
      throwUsage();
   } else {
      admincmd.set_cmd(cmd_it->second);
   }

   // Help is a special subcommand which suppresses errors and prints usage

   if(argc > argno && std::string(argv[argno]) == "help") {
      throwUsage();
   }

   // Parse the subcommand

   bool has_subcommand = cmdHelp.at(admincmd.cmd()).has_subcommand();

   if(has_subcommand)
   {
      subcmdLookup_t::const_iterator subcmd_it;

      if(argc <= argno) {
         throwUsage("Missing subcommand");
      } else if((subcmd_it = subcmdLookup.find(argv[argno])) == subcmdLookup.end()) {
         throwUsage(std::string("Invalid subcommand: ") + argv[argno]);
      } else {
         admincmd.set_subcmd(subcmd_it->second);
      }
   }

   // Parse the options

   auto option_list_it = cmdOptions.find(cmd_key_t{ admincmd.cmd(), admincmd.subcmd() });

   if(option_list_it == cmdOptions.end()) {
      throwUsage(std::string("Invalid subcommand: ") + argv[argno]);
   }

   parseOptions(has_subcommand ? argno+1 : argno, argc, argv, option_list_it->second);
}

const std::string CtaAdminCmd::getConfigFilePath() const {
   std::filesystem::path config_file = DEFAULT_CLI_CONFIG;

   if(std::getenv("HOME")) {
      const std::filesystem::path home = std::getenv("HOME");
      const std::filesystem::path home_dir_config_file = home / ".cta/cta-cli.conf";
      if(std::filesystem::exists(home_dir_config_file)) { config_file = home_dir_config_file; }
   }
   if(m_config) {
    config_file = m_config.value();
   }
   return config_file;
}

void CtaAdminCmd::send() const
{
   // Validate the Protocol Buffer
   try {
      validateCmd(m_request.admincmd());
   } catch(std::runtime_error &ex) {
      throwUsage(ex.what());
   }

   const std::filesystem::path config_file = getConfigFilePath(); 

   // Set configuration options
   XrdSsiPb::Config config(config_file, "cta");
   config.set("resource", "/ctafrontend");
   config.set("response_bufsize", StreamBufferSize);         // default value = 1024 bytes
   config.set("request_timeout", DefaultRequestTimeout);     // default value = 10s

   // Allow environment variables to override config file
   config.getEnv("request_timeout", "XRD_REQUESTTIMEOUT");

   // If XRDDEBUG=1, switch on all logging
   if(getenv("XRDDEBUG")) {
      config.set("log", "all");
   }
   // If fine-grained control over log level is required, use XrdSsiPbLogLevel
   config.getEnv("log", "XrdSsiPbLogLevel");

   // Validate that endpoint was specified in the config file
   if(!config.getOptionValueStr("endpoint").first) {
      throw std::runtime_error("Configuration error: cta.endpoint missing from " + config_file.string());
   }

   // Check drive timeout value
   auto [driveTimeoutConfigExists, driveTimeoutVal] = config.getOptionValueInt("drive_timeout");
   if(driveTimeoutConfigExists) {
     if (driveTimeoutVal > 0) {
       formattedText.setDriveTimeout(driveTimeoutVal);
     } else {
       throw std::runtime_error("Configuration error: cta.drive_timeout not a positive value in " + config_file.string());
     }
   }

   // If the server is down, we want an immediate failure. Set client retry to a single attempt.
   XrdSsiProviderClient->SetTimeout(XrdSsiProvider::connect_N, 1);

   // Obtain a Service Provider
   XrdSsiPbServiceType cta_service(config);

   // Send the Request to the Service and get a Response
   cta::xrd::Response response;
   auto stream_future = cta_service.SendAsync(m_request, response);

   // Handle responses
   switch(response.type())
   {
      using namespace cta::xrd;
      using namespace cta::admin;

      case Response::RSP_SUCCESS:
         // Print message text
         std::cout << response.message_txt();
         // Print streaming response header
         if(!isJson()) switch(response.show_header()) {
            case HeaderType::ADMIN_LS:                     formattedText.printAdminLsHeader(); break;
            case HeaderType::ARCHIVEROUTE_LS:              formattedText.printArchiveRouteLsHeader(); break;
            case HeaderType::DRIVE_LS:                     formattedText.printDriveLsHeader(); break;
            case HeaderType::FAILEDREQUEST_LS:             formattedText.printFailedRequestLsHeader(); break;
            case HeaderType::FAILEDREQUEST_LS_SUMMARY:     formattedText.printFailedRequestLsSummaryHeader(); break;
            case HeaderType::GROUPMOUNTRULE_LS:            formattedText.printGroupMountRuleLsHeader(); break;
            case HeaderType::LISTPENDINGARCHIVES:          formattedText.printListPendingArchivesHeader(); break;
            case HeaderType::LISTPENDINGARCHIVES_SUMMARY:  formattedText.printListPendingArchivesSummaryHeader(); break;
            case HeaderType::LISTPENDINGRETRIEVES:         formattedText.printListPendingRetrievesHeader(); break;
            case HeaderType::LISTPENDINGRETRIEVES_SUMMARY: formattedText.printListPendingRetrievesSummaryHeader(); break;
            case HeaderType::LOGICALLIBRARY_LS:            formattedText.printLogicalLibraryLsHeader(); break;
            case HeaderType::MOUNTPOLICY_LS:               formattedText.printMountPolicyLsHeader(); break;
            case HeaderType::REPACK_LS:                    formattedText.printRepackLsHeader(); break;
            case HeaderType::REQUESTERMOUNTRULE_LS:        formattedText.printRequesterMountRuleLsHeader(); break;
            case HeaderType::ACTIVITYMOUNTRULE_LS:         formattedText.printActivityMountRuleLsHeader(); break;
            case HeaderType::SHOWQUEUES:                   formattedText.printShowQueuesHeader(); break;
            case HeaderType::STORAGECLASS_LS:              formattedText.printStorageClassLsHeader(); break;
            case HeaderType::TAPE_LS:                      formattedText.printTapeLsHeader(); break;
            case HeaderType::TAPEFILE_LS:                  formattedText.printTapeFileLsHeader(); break;
            case HeaderType::TAPEPOOL_LS:                  formattedText.printTapePoolLsHeader(); break;
            case HeaderType::DISKSYSTEM_LS:                formattedText.printDiskSystemLsHeader(); break;
            case HeaderType::DISKINSTANCE_LS:              formattedText.printDiskInstanceLsHeader(); break;
            case HeaderType::DISKINSTANCESPACE_LS:         formattedText.printDiskInstanceSpaceLsHeader(); break;
            case HeaderType::VIRTUALORGANIZATION_LS:       formattedText.printVirtualOrganizationLsHeader(); break;
            case HeaderType::VERSION_CMD:                  formattedText.printVersionHeader(); break;
            case HeaderType::MEDIATYPE_LS:                 formattedText.printMediaTypeLsHeader(); break;
            case HeaderType::RECYLETAPEFILE_LS:            formattedText.printRecycleTapeFileLsHeader(); break;
            case HeaderType::NONE:
            default:                                       break;
         }
         // Allow stream processing to commence
         isHeaderSent = true;
         break;
      case Response::RSP_ERR_PROTOBUF:                     throw XrdSsiPb::PbException(response.message_txt());
      case Response::RSP_ERR_USER:                         throw XrdSsiPb::UserException(response.message_txt());
      case Response::RSP_ERR_CTA:                          throw std::runtime_error(response.message_txt());
      default:                                             throw XrdSsiPb::PbException("Invalid response type.");
   }

   // If there is a Data/Stream payload, wait until it has been processed before exiting
   stream_future.wait();

   // JSON output is an array of structs, close bracket
   if(isJson()) { std::cout << jsonCloseDelim(); }
}



void CtaAdminCmd::parseOptions(int start, int argc, const char *const *const argv, const cmd_val_t &options)
{
   for(int i = start; i < argc; ++i)
   {
      int opt_num = i-start;

      cmd_val_t::const_iterator opt_it;

      // Scan options for a match

      for(opt_it = options.begin(); opt_it != options.end(); ++opt_it) {
         // Special case of OPT_CMD type has an implicit key
         if(opt_num-- == 0 && opt_it->get_type() == Option::OPT_CMD) break;

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

std::string CtaAdminCmd::getDriveFromTpConfig() {
   std::ifstream file(tp_config_file);
   if (file.fail()) {
      throw std::runtime_error("Unable to open file " + tp_config_file);
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

         std::string drivename = item.substr(0, item.find(" ")); // first word of line
         return drivename;
         
      }
   }
   throw std::runtime_error("File " + tp_config_file + " is empty");
}

void CtaAdminCmd::addOption(const Option &option, const std::string &value)
{
   auto admincmd_ptr = m_request.mutable_admincmd();

   switch(option.get_type())
   {
      case Option::OPT_CMD:
      case Option::OPT_STR: {
         auto key = strOptions.at(option.get_key());
         auto new_opt = admincmd_ptr->add_option_str();
         new_opt->set_key(key);
         if (option == opt_drivename_cmd && value == "first") {
            new_opt->set_value(getDriveFromTpConfig());
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
         if(option.get_type() == Option::OPT_FLAG || value == "true") {
            new_opt->set_value(true);
         } else if(value == "false") {
            new_opt->set_value(false);
         } else {
            throw std::runtime_error(value + " is not a boolean value: " + option.help());
         }
         break;
      }
      case Option::OPT_UINT: try {
         auto key = uint64Options.at(option.get_key());
         int64_t val_int = std::stol(value);
         if(val_int < 0) throw std::out_of_range("value is negative");
         auto new_opt = admincmd_ptr->add_option_uint64();
         new_opt->set_key(key);
         new_opt->set_value(val_int);
         break;
      } catch(std::invalid_argument &) {
         throw std::runtime_error(value + " is not a valid uint64: " + option.help());
      } catch(std::out_of_range &) {
         throw std::runtime_error(value + " is out of range: " + option.help());
      }
   }
}



void CtaAdminCmd::readListFromFile(cta::admin::OptionStrList &str_list, const std::string &filename)
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

         if(str_list.key() == OptionStrList::FILE_ID) {
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



void CtaAdminCmd::throwUsage(const std::string &error_txt) const
{
   std::stringstream help;
   const auto &admincmd = m_request.admincmd().cmd();

   if(error_txt != "") {
      help << error_txt << std::endl;
   }

   if(admincmd == AdminCmd::CMD_NONE)
   {
      // Command has not been set: show generic help
      help << "CTA Administration Tool" << std::endl << std::endl
           << "Usage: " << m_execname << " [--json] [--config <configpath>] <command> [<subcommand> [<option>...]]" << std::endl
           << "       " << m_execname << " <command> help" << std::endl << std::endl
           << "By default, the output is in tabular format. If the --json option is supplied, the output is a JSON array." << std::endl
           << "Commands have a long and short version. Subcommands (add/ch/ls/rm/etc.) do not have short versions. For" << std::endl
           << "detailed help on the options of each subcommand, type: " << m_execname << " <command> help" << std::endl << std::endl;

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

}} // namespace cta::admin



/*!
 * Start here
 *
 * @param    argc[in]    The number of command-line arguments
 * @param    argv[in]    The command-line arguments
 */

int main(int argc, const char **argv)
{
   using namespace cta::admin;

   try {    
      // Parse the command line arguments
      CtaAdminCmd cmd(argc, argv);

      // Send the protocol buffer
      cmd.send();

      // Delete all global objects allocated by libprotobuf
      google::protobuf::ShutdownProtobufLibrary();

      return 0;
   } catch (XrdSsiPb::PbException &ex) {
      std::cerr << "Error in Google Protocol Buffers: " << ex.what() << std::endl;
   } catch (XrdSsiPb::XrdSsiException &ex) {
      std::cerr << "Error from XRootD SSI Framework: " << ex.what() << std::endl;
   } catch (XrdSsiPb::UserException &ex) {
      if(CtaAdminCmd::isJson()) std::cout << CtaAdminCmd::jsonCloseDelim();
      std::cerr << ex.what() << std::endl;
      return 2;
   } catch (std::runtime_error &ex) {
      std::cerr << ex.what() << std::endl;
   } catch (std::exception &ex) {
      std::cerr << "Caught exception: " << ex.what() << std::endl;
   } catch (...) {
      std::cerr << "Caught an unknown exception" << std::endl;
   }

   return 1;
}

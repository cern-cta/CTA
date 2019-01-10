/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Command-line tool for CTA Admin commands
 * @description    CTA Admin command using Google Protocol Buffers and XRootD SSI transport
 * @copyright      Copyright 2017 CERN
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

#include <sstream>
#include <iostream>
#include <iomanip>

#include <XrdSsiPbLog.hpp>
#include <XrdSsiPbIStreamBuffer.hpp>

#include "CtaAdminCmd.hpp"

// Define XRootD SSI Alert message callback
namespace XrdSsiPb {

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

   // Output results in JSON format for parsing by a script
   if(CtaAdminCmd::isJson())
   {
      std::cout << CtaAdminCmd::jsonDelim();

      switch(record.data_case()) {
         case Data::kAflsItem:      std::cout << Log::DumpProtobuf(&record.afls_item());    break;
         case Data::kAflsSummary:   std::cout << Log::DumpProtobuf(&record.afls_summary()); break;
         case Data::kFrlsItem:      std::cout << Log::DumpProtobuf(&record.frls_item());    break;
         case Data::kFrlsSummary:   std::cout << Log::DumpProtobuf(&record.frls_summary()); break;
         case Data::kLpaItem:       std::cout << Log::DumpProtobuf(&record.lpa_item());     break;
         case Data::kLpaSummary:    std::cout << Log::DumpProtobuf(&record.lpa_summary());  break;
         case Data::kLprItem:       std::cout << Log::DumpProtobuf(&record.lpr_item());     break;
         case Data::kLprSummary:    std::cout << Log::DumpProtobuf(&record.lpr_summary());  break;
         case Data::kTplsItem:      std::cout << Log::DumpProtobuf(&record.tpls_item());    break;
         default:
            throw std::runtime_error("Received invalid stream data from CTA Frontend.");
      }
   }
   // Format results in a tabular format for a human
   else switch(record.data_case()) {
         case Data::kAflsItem:      CtaAdminCmd::print(record.afls_item());    break;
         case Data::kAflsSummary:   CtaAdminCmd::print(record.afls_summary()); break;
         case Data::kFrlsItem:      CtaAdminCmd::print(record.frls_item());    break;
         case Data::kFrlsSummary:   CtaAdminCmd::print(record.frls_summary()); break;
         case Data::kLpaItem:       CtaAdminCmd::print(record.lpa_item());     break;
         case Data::kLpaSummary:    CtaAdminCmd::print(record.lpa_summary());  break;
         case Data::kLprItem:       CtaAdminCmd::print(record.lpr_item());     break;
         case Data::kLprSummary:    CtaAdminCmd::print(record.lpr_summary());  break;
         case Data::kTplsItem:      CtaAdminCmd::print(record.tpls_item());    break;
         default:
            throw std::runtime_error("Received invalid stream data from CTA Frontend.");
   }
}

} // namespace XrdSsiPb



namespace cta {
namespace admin {

bool CtaAdminCmd::is_json         = false;
bool CtaAdminCmd::is_first_record = true;

CtaAdminCmd::CtaAdminCmd(int argc, const char *const *const argv) :
   m_execname(argv[0])
{
   auto &admincmd = *(m_request.mutable_admincmd());

   // Strip path from execname

   size_t p = m_execname.find_last_of('/');
   if(p != std::string::npos) m_execname.erase(0, p+1);

   // Parse the command

   cmdLookup_t::const_iterator cmd_it;

   // Client-side only options

   int argno = 1;

   if(argc <= argno) throwUsage();

   if(std::string(argv[argno]) == "--json") { is_json = true; ++argno; }

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



void CtaAdminCmd::send() const
{
   // Validate the Protocol Buffer
   try {
      validateCmd(m_request.admincmd());
   } catch(std::runtime_error &ex) {
      throwUsage(ex.what());
   }

   // Set configuration options
   const std::string config_file = "/etc/cta/cta-cli.conf";
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
      throw std::runtime_error("Configuration error: cta.endpoint missing from " + config_file);
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
            case HeaderType::ARCHIVEFILE_LS:               printAfLsHeader(); break;
            case HeaderType::ARCHIVEFILE_LS_SUMMARY:       printAfLsSummaryHeader(); break;
            case HeaderType::FAILEDREQUEST_LS:             printFrLsHeader(); break;
            case HeaderType::FAILEDREQUEST_LS_SUMMARY:     printFrLsSummaryHeader(); break;
            case HeaderType::LISTPENDINGARCHIVES:          printLpaHeader(); break;
            case HeaderType::LISTPENDINGARCHIVES_SUMMARY:  printLpaSummaryHeader(); break;
            case HeaderType::LISTPENDINGRETRIEVES:         printLprHeader(); break;
            case HeaderType::LISTPENDINGRETRIEVES_SUMMARY: printLprSummaryHeader(); break;
            case HeaderType::TAPEPOOL_LS:                  printTpLsHeader(); break;
            case HeaderType::NONE:
            default:                                       break;
         }
         break;
      case Response::RSP_ERR_PROTOBUF:                     throw XrdSsiPb::PbException(response.message_txt());
      case Response::RSP_ERR_USER:
      case Response::RSP_ERR_CTA:                          throw std::runtime_error(response.message_txt());
      default:                                             throw XrdSsiPb::PbException("Invalid response type.");
   }

   // If there is a Data/Stream payload, wait until it has been processed before exiting
   stream_future.wait();

   // JSON output is an array of structs, close bracket
   if(isJson()) { std::cout << ']'; }
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
         new_opt->set_value(value);
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
         auto new_opt = admincmd_ptr->add_option_uint64();
         new_opt->set_key(key);
         new_opt->set_value(std::stoul(value));
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
         if(!item.empty()) {
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

      help << "CTA Admin commands:"                                                          << std::endl << std::endl
           << "For each command there is a short version and a long one. Subcommands (add/ch/ls/rm/etc.)" << std::endl
           << "do not have short versions. For detailed help on the options of each subcommand, type:"    << std::endl
           << "  " << m_execname << " <command> help"                                        << std::endl << std::endl;

      for(auto cmd_it = cmdHelp.begin(); cmd_it != cmdHelp.end(); ++cmd_it)
      {
         help << "  " << m_execname << ' ' << cmd_it->second.short_help() << std::endl;
      }
   }
   else
   {
      // Command has been set: show command-specific help

      help << m_execname << ' ' << cmdHelp.at(admincmd).help();
   }

   throw std::runtime_error(help.str());
}



// static methods (for printing stream results)

void CtaAdminCmd::printAfLsHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(11) << std::right << "archive id"     << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "copy no"        << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "vid"            << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "fseq"           << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "block id"       << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "instance"       << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "disk id"        << ' '
             << std::setfill(' ') << std::setw(12) << std::right << "size"           << ' '
             << std::setfill(' ') << std::setw(13) << std::right << "checksum type"  << ' '
             << std::setfill(' ') << std::setw(14) << std::right << "checksum value" << ' '
             << std::setfill(' ') << std::setw(13) << std::right << "storage class"  << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "owner"          << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "group"          << ' '
             << std::setfill(' ') << std::setw(13) << std::right << "creation time"  << ' '
                                                                 << "path"
             << TEXT_NORMAL << std::endl;
}

void CtaAdminCmd::print(const cta::admin::ArchiveFileLsItem &afls_item)
{
   std::cout << std::setfill(' ') << std::setw(11) << std::right << afls_item.af().archive_id()    << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << afls_item.copy_nb()            << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << afls_item.tf().vid()           << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << afls_item.tf().f_seq()         << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << afls_item.tf().block_id()      << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << afls_item.af().disk_instance() << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << afls_item.af().disk_id()       << ' '
             << std::setfill(' ') << std::setw(12) << std::right << afls_item.af().size()          << ' '
             << std::setfill(' ') << std::setw(13) << std::right << afls_item.af().cs().type()     << ' '
             << std::setfill(' ') << std::setw(14) << std::right << afls_item.af().cs().value()    << ' '
             << std::setfill(' ') << std::setw(13) << std::right << afls_item.af().storage_class() << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << afls_item.af().df().owner()    << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << afls_item.af().df().group()    << ' '
             << std::setfill(' ') << std::setw(13) << std::right << afls_item.af().creation_time() << ' '
                                                                 << afls_item.af().df().path()
             << std::endl;
}

void CtaAdminCmd::printAfLsSummaryHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(13) << std::right << "total files" << ' '
             << std::setfill(' ') << std::setw(12) << std::right << "total size"  << ' '
             << TEXT_NORMAL << std::endl;
}

void CtaAdminCmd::print(const cta::admin::ArchiveFileLsSummary &afls_summary)
{
   std::cout << std::setfill(' ') << std::setw(13) << std::right << afls_summary.total_files() << ' '
             << std::setfill(' ') << std::setw(12) << std::right << afls_summary.total_size()  << ' '
             << std::endl;
}

void CtaAdminCmd::printFrLsHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(11) << std::right << "request type"   << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "copy no"        << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "vid"            << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "requester"      << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "group"          << ' '
                                                                 << "path"
             << TEXT_NORMAL << std::endl;
}

void CtaAdminCmd::print(const cta::admin::FailedRequestLsItem &frls_item)
{
   throw std::runtime_error("Not implemented.");
}

void CtaAdminCmd::printFrLsSummaryHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(12) << std::right << "request type"        << ' '
             << std::setfill(' ') << std::setw(13) << std::right << "total files"         << ' '
             << std::setfill(' ') << std::setw(20) << std::right << "total size (bytes)"  << ' '
             << TEXT_NORMAL << std::endl;
}

void CtaAdminCmd::print(const cta::admin::FailedRequestLsSummary &frls_summary)
{
   std::string request_type =
      frls_summary.request_type() == cta::admin::RequestType::ARCHIVE_REQUEST  ? "archive" :
      frls_summary.request_type() == cta::admin::RequestType::RETRIEVE_REQUEST ? "retrieve" : "total";

   std::cout << std::setfill(' ') << std::setw(11) << std::right << request_type               << ' '
             << std::setfill(' ') << std::setw(13) << std::right << frls_summary.total_files() << ' '
             << std::setfill(' ') << std::setw(20) << std::right << frls_summary.total_size()  << ' '
             << std::endl;
}

void CtaAdminCmd::printLpaHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(18) << std::right << "tapepool"       << ' '
             << std::setfill(' ') << std::setw(11) << std::right << "archive id"     << ' '
             << std::setfill(' ') << std::setw(13) << std::right << "storage class"  << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "copy no"        << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "disk id"        << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "instance"       << ' '
             << std::setfill(' ') << std::setw(13) << std::right << "checksum type"  << ' '
             << std::setfill(' ') << std::setw(14) << std::right << "checksum value" << ' '
             << std::setfill(' ') << std::setw(12) << std::right << "size"           << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "user"           << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "group"          << ' '
             <<                                                     "path"
             << TEXT_NORMAL << std::endl;
}

void CtaAdminCmd::print(const cta::admin::ListPendingArchivesItem &lpa_item)
{
   std::cout << std::setfill(' ') << std::setw(18) << std::right << lpa_item.tapepool()           << ' '
             << std::setfill(' ') << std::setw(11) << std::right << lpa_item.af().archive_id()    << ' '
             << std::setfill(' ') << std::setw(13) << std::right << lpa_item.af().storage_class() << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << lpa_item.copy_nb()            << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << lpa_item.af().disk_id()       << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << lpa_item.af().disk_instance() << ' '
             << std::setfill(' ') << std::setw(13) << std::right << lpa_item.af().cs().type()     << ' '
             << std::setfill(' ') << std::setw(14) << std::right << lpa_item.af().cs().value()    << ' '
             << std::setfill(' ') << std::setw(12) << std::right << lpa_item.af().size()          << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << lpa_item.af().df().owner()    << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << lpa_item.af().df().group()    << ' '
             <<                                                     lpa_item.af().df().path()
             << std::endl;
}

void CtaAdminCmd::printLpaSummaryHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(18) << std::right << "tapepool"    << ' '
             << std::setfill(' ') << std::setw(13) << std::right << "total files" << ' '
             << std::setfill(' ') << std::setw(12) << std::right << "total size"  << ' '
             << TEXT_NORMAL << std::endl;
}

void CtaAdminCmd::print(const cta::admin::ListPendingArchivesSummary &lpa_summary)
{
   std::cout << std::setfill(' ') << std::setw(18) << std::right << lpa_summary.tapepool()    << ' '
             << std::setfill(' ') << std::setw(13) << std::right << lpa_summary.total_files() << ' '
             << std::setfill(' ') << std::setw(12) << std::right << lpa_summary.total_size()  << ' '
             << std::endl;
}

void CtaAdminCmd::printLprHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(13) << std::right << "vid"        << ' '
             << std::setfill(' ') << std::setw(11) << std::right << "archive id" << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "copy no"    << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "fseq"       << ' '
             << std::setfill(' ') << std::setw(9)  << std::right << "block id"   << ' '
             << std::setfill(' ') << std::setw(12) << std::right << "size"       << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "user"       << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "group"      << ' '
             <<                                                     "path"
             << TEXT_NORMAL << std::endl;
}

void CtaAdminCmd::print(const cta::admin::ListPendingRetrievesItem &lpr_item)
{
   std::cout << std::setfill(' ') << std::setw(13) << std::right << lpr_item.tf().vid()        << ' '
             << std::setfill(' ') << std::setw(11) << std::right << lpr_item.af().archive_id() << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << lpr_item.copy_nb()         << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << lpr_item.tf().f_seq()      << ' '
             << std::setfill(' ') << std::setw(9)  << std::right << lpr_item.tf().block_id()   << ' '
             << std::setfill(' ') << std::setw(12) << std::right << lpr_item.af().size()       << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << lpr_item.af().df().owner() << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << lpr_item.af().df().group() << ' '
             <<                                                     lpr_item.af().df().path()
             << std::endl;
}

void CtaAdminCmd::printLprSummaryHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(13) << std::right << "vid"         << ' '
             << std::setfill(' ') << std::setw(13) << std::right << "total files" << ' '
             << std::setfill(' ') << std::setw(12) << std::right << "total size"  << ' '
             << TEXT_NORMAL << std::endl;
}

void CtaAdminCmd::print(const cta::admin::ListPendingRetrievesSummary &lpr_summary)
{
   std::cout << std::setfill(' ') << std::setw(13) << std::right << lpr_summary.vid()         << ' '
             << std::setfill(' ') << std::setw(13) << std::right << lpr_summary.total_files() << ' '
             << std::setfill(' ') << std::setw(12) << std::right << lpr_summary.total_size()  << ' '
             << std::endl;
}

void CtaAdminCmd::printTpLsHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(18) << std::right << "name"        << ' '
             << std::setfill(' ') << std::setw(10) << std::right << "vo"          << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "#tapes"      << ' '
             << std::setfill(' ') << std::setw(9)  << std::right << "#partial"    << ' '
             << std::setfill(' ') << std::setw(12) << std::right << "#phys files" << ' '
             << std::setfill(' ') << std::setw(5)  << std::right << "size"        << ' '
             << std::setfill(' ') << std::setw(5)  << std::right << "used"        << ' '
             << std::setfill(' ') << std::setw(6)  << std::right << "avail"       << ' '
             << std::setfill(' ') << std::setw(6)  << std::right << "use%"        << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "encrypt"     << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "c.user"      << ' '
             << std::setfill(' ') << std::setw(25) << std::right << "c.host"      << ' '
             << std::setfill(' ') << std::setw(24) << std::right << "c.time"      << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "m.user"      << ' '
             << std::setfill(' ') << std::setw(25) << std::right << "m.host"      << ' '
             << std::setfill(' ') << std::setw(24) << std::right << "m.time"      << ' '
             <<                                                     "comment"     << ' '
             << TEXT_NORMAL << std::endl;
}

void CtaAdminCmd::print(const cta::admin::TapePoolLsItem &tpls_item)
{
   std::string encrypt_str = tpls_item.encrypt() ? "true" : "false";
   uint64_t avail = tpls_item.capacity_bytes() > tpls_item.data_bytes() ?
      tpls_item.capacity_bytes()-tpls_item.data_bytes() : 0; 
   double use_percent = tpls_item.capacity_bytes() > 0 ?
      (static_cast<double>(tpls_item.data_bytes())/static_cast<double>(tpls_item.capacity_bytes()))*100.0 : 0.0;

   std::cout << std::setfill(' ') << std::setw(18) << std::right << tpls_item.name()                          << ' '
             << std::setfill(' ') << std::setw(10) << std::right << tpls_item.vo()                            << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << tpls_item.num_tapes()                     << ' '
             << std::setfill(' ') << std::setw(9)  << std::right << tpls_item.num_partial_tapes()             << ' '
             << std::setfill(' ') << std::setw(12) << std::right << tpls_item.num_physical_files()            << ' '
             << std::setfill(' ') << std::setw(4)  << std::right << tpls_item.capacity_bytes() / 1000000000   << "G "
             << std::setfill(' ') << std::setw(4)  << std::right << tpls_item.data_bytes()     / 1000000000   << "G "
             << std::setfill(' ') << std::setw(5)  << std::right << avail                      / 1000000000   << "G "
             << std::setfill(' ') << std::setw(5)  << std::right << std::fixed << std::setprecision(1) << use_percent << "% "
             << std::setfill(' ') << std::setw(8)  << std::right << encrypt_str                               << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << tpls_item.created().username()            << ' '
             << std::setfill(' ') << std::setw(25) << std::right << tpls_item.created().host()                << ' '
             << std::setfill(' ') << std::setw(24) << std::right << timeToString(tpls_item.created().time())  << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << tpls_item.modified().username()           << ' '
             << std::setfill(' ') << std::setw(25) << std::right << tpls_item.modified().host()               << ' '
             << std::setfill(' ') << std::setw(24) << std::right << timeToString(tpls_item.modified().time()) << ' '
             <<                                                     tpls_item.comment()
             << std::endl;
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
   } catch (std::runtime_error &ex) {
      std::cerr << ex.what() << std::endl;
   } catch (std::exception &ex) {
      std::cerr << "Caught exception: " << ex.what() << std::endl;
   } catch (...) {
      std::cerr << "Caught an unknown exception" << std::endl;
   }

   return 1;
}


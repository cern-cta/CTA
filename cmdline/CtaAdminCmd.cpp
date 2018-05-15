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

   Log::DumpProtobuf(Log::PROTOBUF, &record);

   switch(record.data_case())
   {
      case Data::kAfItemFieldNumber : switch(record.af_item().type())
      {
         case ArchiveFileItem::ARCHIVEFILE_LS:
            CtaAdminCmd::printAfLsItem(record.af_item());
            break;
         case ArchiveFileItem::LISTPENDINGARCHIVES:
         case ArchiveFileItem::LISTPENDINGRETRIEVES:
         default:
            throw std::runtime_error("Not implemented/received invalid stream data from CTA Frontend.");
      }

      case Data::kAfSummaryItemFieldNumber : switch(record.af_summary_item().type())
      {
         case ArchiveFileItem::ARCHIVEFILE_LS:
         case ArchiveFileItem::LISTPENDINGARCHIVES:
         case ArchiveFileItem::LISTPENDINGRETRIEVES:
         default:
            throw std::runtime_error("Not implemented/received invalid stream data from CTA Frontend.");
      }

      default:
         throw std::runtime_error("Received invalid stream data from CTA Frontend.");
   }
}

} // namespace XrdSsiPb



namespace cta {
namespace admin {

CtaAdminCmd::CtaAdminCmd(int argc, const char *const *const argv) :
   m_execname(argv[0])
{
   auto &admincmd = *(m_request.mutable_admincmd());

   // Strip path from execname

   size_t p = m_execname.find_last_of('/');
   if(p != std::string::npos) m_execname.erase(0, p+1);

   // Parse the command

   cmdLookup_t::const_iterator cmd_it;

   if(argc < 2 || (cmd_it = cmdLookup.find(argv[1])) == cmdLookup.end()) {
      throwUsage();
   } else {
      admincmd.set_cmd(cmd_it->second);
   }

   // Help is a special subcommand which suppresses errors and prints usage
   
   if(argc > 2 && std::string(argv[2]) == "help") {
      throwUsage();
   }

   // Parse the subcommand

   bool has_subcommand = cmdHelp.at(admincmd.cmd()).has_subcommand();

   if(has_subcommand)
   {
      subcmdLookup_t::const_iterator subcmd_it;

      if(argc < 3) {
         throwUsage("Missing subcommand");
      } else if((subcmd_it = subcmdLookup.find(argv[2])) == subcmdLookup.end()) {
         throwUsage(std::string("Invalid subcommand: ") + argv[2]);
      } else {
         admincmd.set_subcmd(subcmd_it->second);
      }
   }

   // Parse the options

   auto option_list_it = cmdOptions.find(cmd_key_t{ admincmd.cmd(), admincmd.subcmd() });

   if(option_list_it == cmdOptions.end()) {
      throwUsage(std::string("Invalid subcommand: ") + argv[2]);
   }

   parseOptions(has_subcommand ? 3 : 2, argc, argv, option_list_it->second);
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


   // Obtain a Service Provider
   XrdSsiPbServiceType cta_service(config);

   // Send the Request to the Service and get a Response
   cta::xrd::Response response;
   auto stream_future = cta_service.Send(m_request, response);

   // Handle responses
   switch(response.type())
   {
      using namespace cta::xrd;

      case Response::RSP_SUCCESS:         std::cout << response.message_txt(); break;
      case Response::RSP_ERR_PROTOBUF:    throw XrdSsiPb::PbException(response.message_txt());
      case Response::RSP_ERR_USER:
      case Response::RSP_ERR_CTA:         throw std::runtime_error(response.message_txt());
      default:                            throw XrdSsiPb::PbException("Invalid response type.");
   }

   // If there is a Data/Stream payload, wait until it has been processed before exiting
   stream_future.wait();
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



void CtaAdminCmd::printAfLsItem(const cta::admin::ArchiveFileItem &af_item)
{
   std::cout << std::setfill(' ') << std::setw(7)  << std::right << af_item.af().archive_id()    << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << af_item.copy_nb()            << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << af_item.tf().vid()           << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << af_item.tf().f_seq()         << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << af_item.tf().block_id()      << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << af_item.af().disk_instance() << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << af_item.af().disk_id()       << ' '
             << std::setfill(' ') << std::setw(12) << std::right << af_item.af().size()          << ' '
             << std::setfill(' ') << std::setw(13) << std::right << af_item.af().cs().type()     << ' '
             << std::setfill(' ') << std::setw(14) << std::right << af_item.af().cs().value()    << ' '
             << std::setfill(' ') << std::setw(13) << std::right << af_item.af().storage_class() << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << af_item.af().df().owner()    << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << af_item.af().df().group()    << ' '
             << std::setfill(' ') << std::setw(13) << std::right << af_item.af().creation_time() << ' '
             << af_item.af().df().path() << std::endl;
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


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

#include "cmdline/Configuration.hpp"
#include "CtaAdminCmd.hpp"
#include "XrdSsiPbDebug.hpp"

// Move to generic headers
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>


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
   OutputJsonString(std::cout, &alert);
}



/*!
 * Data callback.
 *
 * Defines how Data/Stream messages should be handled
 */
template<>
void XrdSsiPbRequestType::DataCallback(XrdSsiRequest::PRD_Xeq &post_process, char *response_bufptr, int response_buflen)
{
   google::protobuf::io::ArrayInputStream raw_stream(response_bufptr, response_buflen);
   google::protobuf::io::CodedInputStream coded_stream(&raw_stream);

   // coded_in->ReadVarint32(&n);
   // cout << "#" << n << endl;
   //
   // std::string s;
   //
   // for (uint32_t i = 0; i < n; ++i)
   // {
   // uint32_t msgSize;
   // coded_in->ReadVarint32(&msgSize);
   //
   // if ((msgSize > 0) &&
   // (coded_in->ReadString(&s, msgSize)))
   // {
   //
   // Person p;
   // p.ParseFromString(s);
   //
   // cout << "ID: " << p.id() << endl;
   // cout << "name: " << p.name() << endl;
   // cout << "gendre: " << gendres[p.gendre()-1] << endl;
   // if (p.has_email())
   // {
   // cout << "e-mail: " << p.email() << endl;
   // }
   //
   // cout << endl;
   // }
   // }
   //
   // delete coded_in;
   // delete raw_in;

   cta::admin::ArchiveFileLsItem item;

   for(int i = 0; i < 10; ++i)
   {
      uint32_t bytesize;
      coded_stream.ReadLittleEndian32(&bytesize);
std::cout << "Bytesize = " << bytesize << std::endl;

      const char *buf_ptr;
      int buf_len;
      coded_stream.GetDirectBufferPointer(reinterpret_cast<const void**>(&buf_ptr), &buf_len);
std::cout << "buf_len = " << buf_len << std::endl;
      item.ParseFromArray(buf_ptr, bytesize);
      coded_stream.Skip(bytesize);

      //coded_stream.Skip(bytesize);
      //item.ParseFromBoundedZeroCopyStream(&raw_stream, bytesize);

      OutputJsonString(std::cout, &item);

   std::cout << std::setfill(' ') << std::setw(7)  << std::right << item.af().archive_file_id() << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << item.copy_nb()              << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << item.tf().vid()             << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << item.tf().f_seq()           << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << item.tf().block_id()        << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << item.af().disk_instance()   << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << item.af().disk_file_id()    << ' '
             << std::setfill(' ') << std::setw(12) << std::right << item.af().file_size()       << ' '
             << std::setfill(' ') << std::setw(13) << std::right << item.af().cs().type()       << ' '
             << std::setfill(' ') << std::setw(14) << std::right << item.af().cs().value()      << ' '
             << std::setfill(' ') << std::setw(13) << std::right << item.af().storage_class()   << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << item.af().df().owner()      << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << item.af().df().group()      << ' '
             << std::setfill(' ') << std::setw(13) << std::right << item.af().creation_time()   << ' '
             << item.af().df().path() << std::endl;
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

#ifdef XRDSSI_DEBUG
   XrdSsiPb::OutputJsonString(std::cout, &m_request.admincmd());
#endif

   // Get socket address of CTA Frontend endpoint

   cta::cmdline::Configuration cliConf("/etc/cta/cta-cli.conf");
   std::string endpoint = cliConf.getFrontendHostAndPort();

   // Obtain a Service Provider

   std::string resource("/ctafrontend");

   XrdSsiPbServiceType cta_service(endpoint, resource);

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


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

#include "CtaAdminCmd.hpp"
#include "XrdSsiPbDebug.hpp"



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

   // Parse the subcommand

   bool has_subcommand = cmdHelp.at(admincmd.cmd()).has_subcommand();

   if(has_subcommand)
   {
      subcmdLookup_t::const_iterator subcmd_it;

      if(argc < 3 || (subcmd_it = subcmdLookup.find(argv[2])) == subcmdLookup.end()) {
         throwUsage();
      } else {
         admincmd.set_subcmd(subcmd_it->second);
      }
   }

   // Parse the options

   auto option_list_it = cmdOptions.find(cmd_key_t{ admincmd.cmd(), admincmd.subcmd() });

   if(option_list_it == cmdOptions.end()) {
      throwUsage();
   }

   parseOptions(has_subcommand ? 3 : 2, argc, argv, option_list_it->second);
}



void CtaAdminCmd::send() const
{
   // Validate the Protocol Buffer

   try {
      validateCommand(m_request.admincmd());
   } catch(std::runtime_error &ex) {
      throwUsage(ex.what());
   }

   // Send the Protocol Buffer
   XrdSsiPb::OutputJsonString(std::cout, &m_request.admincmd());
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

      help << "CTA Admin commands:"                                         << std::endl << std::endl
           << "For each command there is a short version and a long one. "
           << "Subcommands (add/ch/ls/rm/etc.) do not have short versions." << std::endl << std::endl;

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


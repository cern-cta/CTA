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

#include "CtaAdminCmdParse.hpp"
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

#if 0
   // Parse the subcommand

   int nextarg = 2;
   int &num_subcmds = cmd_it->second.sub_cmd.size();

   if(num_subcmds > 0)
   {
      if(argc < 3) throwUsage();

      int i;
      for(i = 0; i < num_subcmds; ++i)
      {
         if(argv[2] == cmd_it->second.sub_cmd.at(i))
         {
            admincmd.set_subcmd(subCmd.at(argv[2]));
            break;
         }
      }
      if(i == num_subcmds) throwUsage();

      ++nextarg;
   }
#endif

#if 0  
   // Tokenize the command

   for(int i = 1; i < argc; ++i)
   {
      m_requestTokens.push_back(argv[i]);
   }
#endif
}



void CtaAdminCmd::throwUsage()
{
   std::stringstream help;

   if(m_request.admincmd().cmd() == AdminCmd::CMD_NONE)
   {
      // Command has not been set: show generic help

      help << "CTA Admin commands:"                                            << std::endl << std::endl
           << "For each command there is a short version and a long one. "
           << "Subcommands (add/ch/ls/rm/etc.) do not have short versions." << std::endl << std::endl;

      for(auto lo_it = cmdHelp.begin(); lo_it != cmdHelp.end(); ++lo_it)
      {
         std::string cmd = lo_it->second.cmd_long + '/' + lo_it->second.cmd_short;
         cmd.resize(25, ' ');
         help << "  " << m_execname << " " << cmd;
   
         auto &sub_cmd = lo_it->second.sub_cmd;
         for(auto sc_it = sub_cmd.begin(); sc_it != sub_cmd.end(); ++sc_it)
         {
            help << (sc_it == sub_cmd.begin() ? ' ' : '/') << *sc_it;
         }
         help << std::endl;
      }
   }
   else
   {
      // Show command-specific help

      help << "hello" << std::endl;
   }

   throw std::runtime_error(help.str());
}



void CtaAdminCmd::send()
{
   XrdSsiPb::OutputJsonString(std::cout, &m_request.admincmd());
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
      // Tokenize and parse the command line arguments
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


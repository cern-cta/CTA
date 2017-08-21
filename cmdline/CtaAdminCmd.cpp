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

#include "XrdSsiPbException.hpp"



namespace cta {
namespace admin {

CtaAdminCmd::CtaAdminCmd(int argc, const char *const *const argv) :
   m_execname(argv[0])
{
   // Strip path from execname

   size_t p = m_execname.find_last_of('/');
   if(p != std::string::npos) m_execname.erase(0, p+1);

   // Parse the command

   if(argc < 2) throwUsage();

   auto cmd_it = shortCmd.find(argv[1]);

   if(cmd_it == shortCmd.end())
   {
      // Short version didn't match, check the long version

      auto longcmd_it = longCmd.find(argv[1]);

      if(longcmd_it == longCmd.end() ||
         (cmd_it = shortCmd.find(longcmd_it->second)) == shortCmd.end())
      {
         throwUsage();
      }
   }


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

   help << "CTA Admin commands:"                                            << std::endl << std::endl
        << "For each command there is a short version and a long one. "
        << "Subcommands (add/ch/ls/rm/etc.) do not have short versions." << std::endl << std::endl;

   for(auto lo_it = longCmd.begin(); lo_it != longCmd.end(); ++lo_it)
   {
      std::string cmd = lo_it->first + "/" + lo_it->second;
      cmd.resize(25, ' ');
      help << "  " << m_execname << " " << cmd;

      auto &sub_cmd = shortCmd.at(lo_it->second).sub_cmd;
      for(auto sc_it = sub_cmd.begin(); sc_it != sub_cmd.end(); ++sc_it)
      {
         help << (sc_it == sub_cmd.begin() ? ' ' : '/') << *sc_it;
      }
      help << std::endl;
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
      // Tokenize and parse the command line arguments

      CtaAdminCmd cmd(argc, argv);
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


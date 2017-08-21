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

   // Tokenize the command

   for(int i = 1; i < argc; ++i)
   {
      m_requestTokens.push_back(argv[i]);
   }

   // Check we have at least one parameter

   if(argc < 2) throwUsage();

#if 0  
   // Parse the command

   std::string &command = m_requestTokens.at(1);

   if     ("ad"   == command || "admin"                  == command) xCom_admin();
   else if("ah"   == command || "adminhost"              == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_adminhost();}
   else if("tp"   == command || "tapepool"               == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_tapepool();}
   else if("ar"   == command || "archiveroute"           == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_archiveroute();}
   else if("ll"   == command || "logicallibrary"         == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_logicallibrary();}
   else if("ta"   == command || "tape"                   == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_tape();}
   else if("sc"   == command || "storageclass"           == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_storageclass();}
   else if("rmr"  == command || "requestermountrule"     == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_requestermountrule();}
   else if("gmr"  == command || "groupmountrule"         == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_groupmountrule();}
   else if("mp"   == command || "mountpolicy"            == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_mountpolicy();}
   else if("re"   == command || "repack"                 == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_repack();}
   else if("sh"   == command || "shrink"                 == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_shrink();}
   else if("ve"   == command || "verify"                 == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_verify();}
   else if("af"   == command || "archivefile"            == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_archivefile();}
   else if("te"   == command || "test"                   == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_test();}
   else if("dr"   == command || "drive"                  == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_drive();}
   else if("lpa"  == command || "listpendingarchives"    == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_listpendingarchives();}
   else if("lpr"  == command || "listpendingretrieves"   == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_listpendingretrieves();}
   else if("sq"   == command || "showqueues"             == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_showqueues();}

   else throwUsage();
#endif
   throwUsage();
}



void CtaAdminCmd::throwUsage()
{
   std::stringstream help;

   help << "CTA Admin commands:"                                            << std::endl << std::endl
        << "For each command there is a short version and a long one. "
        << "Subcommands (add/ch/ls/rm/etc.) do not have short versions." << std::endl << std::endl;

   for(auto lo_it = longOptions.begin(); lo_it != longOptions.end(); ++lo_it)
   {
      std::string cmd = lo_it->first + "/" + lo_it->second;
      cmd.resize(30, ' ');
      help << "  " << m_execname << " " << cmd;

      auto &sub_cmd = options.at(lo_it->second).sub_cmd;
      for(auto sc_it = sub_cmd.begin(); sc_it != sub_cmd.end(); ++sc_it)
      {
         help << (sc_it == sub_cmd.begin() ? "" : "/") << *sc_it;
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


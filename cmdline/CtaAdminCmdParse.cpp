/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Definitions for parsing the options of the CTA Admin command-line tool
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

#include "CtaAdminCmdParse.hpp"

namespace cta {
namespace admin {

std::string CmdHelp::short_help() const {
   std::string help = m_cmd_long + '/' + m_cmd_short;
   help.resize(25, ' ');

   for(auto sc_it = m_sub_cmd.begin(); sc_it != m_sub_cmd.end(); ++sc_it) {
      help += (sc_it == m_sub_cmd.begin() ? ' ' : '/') + *sc_it;
   }
   return help;
}



std::string CmdHelp::help() const {
   const int INDENT      = 4;
   const int WRAP_MARGIN = 80;

   std::string help = m_cmd_short + '/' + m_cmd_long;

   for(auto sc_it = m_sub_cmd.begin(); sc_it != m_sub_cmd.end(); ++sc_it) {
      help += (sc_it == m_sub_cmd.begin() ? ' ' : '/') + *sc_it;
   }

   // Find the length of the longest subcommand (if there is one)
   auto max_sub_cmd = std::max_element(m_sub_cmd.begin(), m_sub_cmd.end(),
                      [](std::string const& lhs, std::string const& rhs) { return lhs.size() < rhs.size(); });

   // Terminate with a colon if there are subcommands to follow
   help += (max_sub_cmd != m_sub_cmd.end()) ? ":\n" : "\n";

   // Optional additional help
   help += m_help_txt;

   // Per-subcommand help
   for(auto sc_it = m_sub_cmd.begin(); sc_it != m_sub_cmd.end(); ++sc_it) {
      std::string cmd_line(INDENT, ' ');
      cmd_line += *sc_it;
      cmd_line.resize(INDENT + max_sub_cmd->size(), ' ');

      auto key = cmd_key_t{ cmdLookup.at(m_cmd_short), subcmdLookup.at(*sc_it) };
      auto options = cmdOptions.at(key);

      for(auto op_it = options.begin(); op_it != options.end(); ++op_it)
      {
         if(cmd_line.size() + op_it->help().size() > WRAP_MARGIN)
         {
             help += cmd_line + '\n';
             cmd_line = std::string(INDENT + max_sub_cmd->size(), ' ');
         }

         cmd_line += op_it->help();
      }
      help += cmd_line + '\n';
   }

   return help;
}

}} // namespace cta::admin


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

#include "CtaAdminCmdParse.hpp"

namespace cta::admin {

void Option::validateCmd(const AdminCmd &admincmd) const
{
   if(m_is_optional) return;

   switch(m_type)
   {
      case Option::OPT_CMD:
      case Option::OPT_STR:
         for(auto opt_it = admincmd.option_str().begin(); opt_it != admincmd.option_str().end(); ++opt_it) {
            if(opt_it->key() == strOptions.at(m_lookup_key)) return;
         }
         break;

      case Option::OPT_STR_LIST:
         for(auto opt_it = admincmd.option_str_list().begin(); opt_it != admincmd.option_str_list().end(); ++opt_it) {
            if(opt_it->key() == strListOptions.at(m_lookup_key)) return;
         }
         break;

      case Option::OPT_FLAG:
      case Option::OPT_BOOL: 
         for(auto opt_it = admincmd.option_bool().begin(); opt_it != admincmd.option_bool().end(); ++opt_it) {
            if(opt_it->key() == boolOptions.at(m_lookup_key)) return;
         }
         break;

      case Option::OPT_UINT:
         for(auto opt_it = admincmd.option_uint64().begin(); opt_it != admincmd.option_uint64().end(); ++opt_it) {
            if(opt_it->key() == uint64Options.at(m_lookup_key)) return;
         }
   }

   throw std::runtime_error("Missing option: " + (m_type == Option::OPT_CMD ? m_help_txt : m_long_opt));
}



std::string CmdHelp::short_help() const
{
   std::string help = m_cmd_long + '/' + m_cmd_short;
   help.resize(25, ' ');

   for(auto sc_it = m_sub_cmd.begin(); sc_it != m_sub_cmd.end(); ++sc_it) {
      help += (sc_it == m_sub_cmd.begin() ? ' ' : '/') + *sc_it;
   }
   return help;
}



std::string CmdHelp::help() const
{
   // As we lazy evaluate the full help text, normally the help text will not be set when we call this method
   if(m_help_full != "") return m_help_full;

   std::string cmd_line = m_cmd_short + '/' + m_cmd_long;

   for(auto sc_it = m_sub_cmd.begin(); sc_it != m_sub_cmd.end(); ++sc_it) {
      cmd_line += (sc_it == m_sub_cmd.begin() ? ' ' : '/') + *sc_it;
   }

   // Print main command options if there are any
   auto key = cmd_key_t{ cmdLookup.at(m_cmd_short), AdminCmd::SUBCMD_NONE };
   add_options(cmd_line, key, INDENT);

   // Find the length of the longest subcommand (if there is one)
   auto max_sub_cmd = std::max_element(m_sub_cmd.begin(), m_sub_cmd.end(),
                      [](std::string const& lhs, std::string const& rhs) { return lhs.size() < rhs.size(); });

   // Terminate with a colon if there are subcommands to follow
   m_help_full += (max_sub_cmd != m_sub_cmd.end()) ? cmd_line + ":\n" : "\n";

   // Add optional additional help
   m_help_full += m_help_extra;

   // Per-subcommand help
   for(auto sc_it = m_sub_cmd.begin(); sc_it != m_sub_cmd.end(); ++sc_it) {
      std::string cmd_line(INDENT, ' ');
      cmd_line += *sc_it;
      cmd_line.resize(INDENT + max_sub_cmd->size(), ' ');

      auto key = cmd_key_t{ cmdLookup.at(m_cmd_short), subcmdLookup.at(*sc_it) };
      add_options(cmd_line, key, INDENT + max_sub_cmd->size());
      m_help_full += '\n';
   }

   return m_help_full;
}



void CmdHelp::add_options(std::string &cmd_line, cmd_key_t &key, unsigned int indent) const
{
   auto options = cmdOptions.find(key);

   if(options == cmdOptions.end()) return;

   for(auto op_it = options->second.begin(); op_it != options->second.end(); ++op_it)
   {
      if(cmd_line.size() + op_it->help().size() > WRAP_MARGIN)
      {
         m_help_full += cmd_line + '\n';
         cmd_line = std::string(indent, ' ');
      }

      cmd_line += op_it->help();
   }
   m_help_full += cmd_line;
}



void validateCmd(const cta::admin::AdminCmd &admincmd)
{
   // Get the option list for this command
   auto option_list_it = cmdOptions.find(cmd_key_t{ admincmd.cmd(), admincmd.subcmd() });

   if(option_list_it == cmdOptions.end()) {
      throw std::runtime_error("Invalid command/subcommand");
   }

   // Iterate through the options and verify that they exist in the protocol buffer
   for(auto option_it = option_list_it->second.begin(); option_it != option_list_it->second.end(); ++option_it)
   {
      option_it->validateCmd(admincmd);
   }
}

} // namespace cta::admin


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

#pragma once

#include <map>
#include <set>
#include <string>

#include "xroot_plugins/messages/CtaFrontendApi.hpp"

namespace cta {
namespace admin {

using cmdLookup_t    = std::map<std::string, AdminCmd::Cmd>;
using subCmdLookup_t = std::map<std::string, AdminCmd::SubCmd>;



/*!
 * Map short and long command names to Protocol Buffer enum values
 */
const cmdLookup_t cmdLookup = {
   { "admin",                   AdminCmd::CMD_ADMIN },
   { "ad",                      AdminCmd::CMD_ADMIN },
   { "adminhost",               AdminCmd::CMD_ADMINHOST },
   { "ah",                      AdminCmd::CMD_ADMINHOST },
   { "archivefile",             AdminCmd::CMD_ARCHIVEFILE },
   { "af",                      AdminCmd::CMD_ARCHIVEFILE },
   { "archiveroute",            AdminCmd::CMD_ARCHIVEROUTE },
   { "ar",                      AdminCmd::CMD_ARCHIVEROUTE },
   { "drive",                   AdminCmd::CMD_DRIVE },
   { "dr",                      AdminCmd::CMD_DRIVE },
   { "groupmountrule",          AdminCmd::CMD_GROUPMOUNTRULE },
   { "gmr",                     AdminCmd::CMD_GROUPMOUNTRULE },
   { "listpendingarchives",     AdminCmd::CMD_LISTPENDINGARCHIVES },
   { "lpa",                     AdminCmd::CMD_LISTPENDINGARCHIVES },
   { "listpendingretrieves",    AdminCmd::CMD_LISTPENDINGRETRIEVES },
   { "lpr",                     AdminCmd::CMD_LISTPENDINGRETRIEVES },
   { "logicallibrary",          AdminCmd::CMD_LOGICALLIBRARY },
   { "ll",                      AdminCmd::CMD_LOGICALLIBRARY },
   { "mountpolicy",             AdminCmd::CMD_MOUNTPOLICY },
   { "mp",                      AdminCmd::CMD_MOUNTPOLICY },
   { "repack",                  AdminCmd::CMD_REPACK },
   { "re",                      AdminCmd::CMD_REPACK },
   { "requestermountrule",      AdminCmd::CMD_REQUESTERMOUNTRULE },
   { "rmr",                     AdminCmd::CMD_REQUESTERMOUNTRULE },
   { "showqueues",              AdminCmd::CMD_SHOWQUEUES },
   { "sq",                      AdminCmd::CMD_SHOWQUEUES },
   { "shrink",                  AdminCmd::CMD_SHRINK },
   { "sh",                      AdminCmd::CMD_SHRINK },
   { "storageclass",            AdminCmd::CMD_STORAGECLASS },
   { "sc",                      AdminCmd::CMD_STORAGECLASS },
   { "tape",                    AdminCmd::CMD_TAPE },
   { "ta",                      AdminCmd::CMD_TAPE },
   { "tapepool",                AdminCmd::CMD_TAPEPOOL },
   { "tp",                      AdminCmd::CMD_TAPEPOOL },
   { "test",                    AdminCmd::CMD_TEST },
   { "te",                      AdminCmd::CMD_TEST },
   { "verify",                  AdminCmd::CMD_VERIFY },
   { "ve",                      AdminCmd::CMD_VERIFY }
};



/*!
 * Map subcommand names to Protocol Buffer enum values
 */
const subCmdLookup_t subCmdLookup = {
   { "add",                     AdminCmd::SUBCMD_ADD },
   { "ch",                      AdminCmd::SUBCMD_CH },
   { "err",                     AdminCmd::SUBCMD_ERR },
   { "label",                   AdminCmd::SUBCMD_LABEL },
   { "ls",                      AdminCmd::SUBCMD_LS },
   { "reclaim",                 AdminCmd::SUBCMD_RECLAIM },
   { "rm",                      AdminCmd::SUBCMD_RM },
   { "up",                      AdminCmd::SUBCMD_UP },
   { "down",                    AdminCmd::SUBCMD_DOWN },
   { "read",                    AdminCmd::SUBCMD_READ },
   { "write",                   AdminCmd::SUBCMD_WRITE },
   { "write_auto",              AdminCmd::SUBCMD_WRITE_AUTO }
};



/*!
 * Map boolean options to Protocol Buffer enum values
 */
const std::map<std::string, OptionBoolean::Key> boolOptions = {
   // Boolean options
   { "--all",                   OptionBoolean::BOOL_ALL },
   { "--disabled",              OptionBoolean::BOOL_DISABLED },
   { "--encrypted",             OptionBoolean::BOOL_ENCRYPTED },
   { "--force",                 OptionBoolean::BOOL_FORCE },
   { "--full",                  OptionBoolean::BOOL_FULL },
   { "--lbp",                   OptionBoolean::BOOL_LBP },

   // hasOption options
   { "--checkchecksum",         OptionBoolean::BOOL_CHECK_CHECKSUM },
   { "--extended",              OptionBoolean::BOOL_EXTENDED },
   { "--header",                OptionBoolean::BOOL_SHOW_HEADER },
   { "--justexpand",            OptionBoolean::BOOL_JUSTEXPAND },
   { "--justrepack",            OptionBoolean::BOOL_JUSTREPACK },
   { "--summary",               OptionBoolean::BOOL_SUMMARY }
};



/*!
 * Map integer options to Protocol Buffer enum values
 */
const std::map<std::string, OptionInteger::Key> intOptions = {
   { "--archivepriority",       OptionInteger::INT_ARCHIVE_PRIORITY },
   { "--capacity",              OptionInteger::INT_CAPACITY },
   { "--copynb",                OptionInteger::INT_COPY_NUMBER },
   { "--firstfseq",             OptionInteger::INT_FIRST_FSEQ },
   { "--id",                    OptionInteger::INT_ARCHIVE_FILE_ID },
   { "--lastfseq",              OptionInteger::INT_LAST_FSEQ },
   { "--maxdrivesallowed",      OptionInteger::INT_MAX_DRIVES_ALLOWED },
   { "--minarchiverequestage",  OptionInteger::INT_MIN_ARCHIVE_REQUEST_AGE },
   { "--minretrieverequestage", OptionInteger::INT_MIN_RETRIEVE_REQUEST_AGE },
   { "--number",                OptionInteger::INT_NUMBER_OF_FILES },
   { "--partial",               OptionInteger::INT_PARTIAL }, 
   { "--partialtapesnumber",    OptionInteger::INT_PARTIAL_TAPES_NUMBER },
   { "--retrievepriority",      OptionInteger::INT_RETRIEVE_PRIORITY },
   { "--size",                  OptionInteger::INT_FILE_SIZE }                  
};



/*!
 * Map string options to Protocol Buffer enum values
 */
const std::map<std::string, OptionString::Key> strOptions = {
   { "--comment",               OptionString::STR_COMMENT },
   { "--diskid",                OptionString::STR_DISKID },
   { "--drive",                 OptionString::STR_DRIVE },
   { "--encryptionkey",         OptionString::STR_ENCRYPTION_KEY },
   { "--file",                  OptionString::STR_FILENAME },
   { "--group",                 OptionString::STR_GROUP },
   { "--hostname",              OptionString::STR_HOSTNAME },
   { "--input",                 OptionString::STR_INPUT },
   { "--instance",              OptionString::STR_INSTANCE },
   { "--logicallibrary",        OptionString::STR_LOGICAL_LIBRARY },
   { "--mountpolicy",           OptionString::STR_MOUNT_POLICY },
   { "--output",                OptionString::STR_OUTPUT },
   { "--owner",                 OptionString::STR_OWNER },
   { "--path",                  OptionString::STR_PATH },
   { "--storageclass",          OptionString::STR_STORAGE_CLASS },
   { "--tag",                   OptionString::STR_TAG },
   { "--tapepool",              OptionString::STR_TAPE_POOL },
   { "--username",              OptionString::STR_USERNAME },
   { "--vid",                   OptionString::STR_VID }
};



/*!
 * Help text structure
 */
struct CmdHelp
{
   std::string cmd_long;                //!< Command string (long version)
   std::string cmd_short;               //!< Command string (short version)
   std::vector<std::string> sub_cmd;    //!< Subcommands which are valid for this command, listed in
                                        //!< the order that they should be displayed in the help
   std::string help_str;                //!< Optional extra help text for the command

   /*!
    * Return the command name
    */
   std::string cmd_name() const {
      std::string cmd = cmd_long + '/' + cmd_short;
      cmd.resize(25, ' ');
      return cmd;
   }

   /*!
    * Return the list of command options
    */
   std::string option_list() const {
      std::string options;
      for(auto sc_it = sub_cmd.begin(); sc_it != sub_cmd.end(); ++sc_it) {
         options += (sc_it == sub_cmd.begin() ? ' ' : '/') + *sc_it;
      }
      options += '\n';
      return options;
   }

   /*!
    * Return the detailed help message
    */
   std::string help() const {
      std::string help = cmd_short + '/' + cmd_long;

      for(auto sc_it = sub_cmd.begin(); sc_it != sub_cmd.end(); ++sc_it) {
         help += (sc_it == sub_cmd.begin() ? ' ' : '/') + *sc_it;
      }
      if(help_str.size() > 0) {
         help += ' ' + help_str;
      }

      // Show per-option help
      if(sub_cmd.size() > 0) {
         help += ":\n";
      }
      for(auto sc_it = sub_cmd.begin(); sc_it != sub_cmd.end(); ++sc_it) {
         help += '\t' + *sc_it + '\n';
      }
      return help;
   }
};



/*!
 * Specify the help text for commands
 */
const std::map<AdminCmd::Cmd, CmdHelp> cmdHelp = {
   { AdminCmd::CMD_ADMIN,                { "admin",                "ad",  { "add", "ch", "rm", "ls" }, "" }},
   { AdminCmd::CMD_ADMINHOST,            { "adminhost",            "ah",  { "add", "ch", "rm", "ls" }, "" }},
   { AdminCmd::CMD_ARCHIVEFILE,          { "archivefile",          "af",  { "ls" }, "" }},
   { AdminCmd::CMD_ARCHIVEROUTE,         { "archiveroute",         "ar",  { "add", "ch", "rm", "ls" }, "" }},
   { AdminCmd::CMD_DRIVE,                { "drive",                "dr",  { "up", "down", "ls" },
                                           "(it is a synchronous command)" }},
   { AdminCmd::CMD_GROUPMOUNTRULE,       { "groupmountrule",       "gmr", { "add", "ch", "rm", "ls" }, "" }},
   { AdminCmd::CMD_LISTPENDINGARCHIVES,  { "listpendingarchives",  "lpa", { }, "" }},
   { AdminCmd::CMD_LISTPENDINGRETRIEVES, { "listpendingretrieves", "lpr", { }, "" }},
   { AdminCmd::CMD_LOGICALLIBRARY,       { "logicallibrary",       "ll",  { "add", "ch", "rm", "ls" }, "" }},
   { AdminCmd::CMD_MOUNTPOLICY,          { "mountpolicy",          "mp",  { "add", "ch", "rm", "ls" }, "" }},
   { AdminCmd::CMD_REPACK,               { "repack",               "re",  { "add", "rm", "ls", "err" }, "" }},
   { AdminCmd::CMD_REQUESTERMOUNTRULE,   { "requestermountrule",   "rmr", { "add", "ch", "rm", "ls" }, "" }},
   { AdminCmd::CMD_SHOWQUEUES,           { "showqueues",           "sq",  { }, "" }},
   { AdminCmd::CMD_SHRINK,               { "shrink",               "sh",  { }, "" }},
   { AdminCmd::CMD_STORAGECLASS,         { "storageclass",         "sc",  { "add", "ch", "rm", "ls" }, "" }},
   { AdminCmd::CMD_TAPE,                 { "tape",                 "ta",  { "add", "ch", "rm", "reclaim", "ls", "label" }, "" }},
   { AdminCmd::CMD_TAPEPOOL,             { "tapepool",             "tp",  { "add", "ch", "rm", "ls" }, "" }},
   { AdminCmd::CMD_TEST,                 { "test",                 "te",  { "read", "write", "write_auto" },
                                           "(to be run on an empty self-dedicated\n"
                                           "drive; it is a synchronous command that returns performance stats and errors;\n"
                                           "all locations are local to the tapeserver)" }},
   { AdminCmd::CMD_VERIFY,               { "verify",               "ve",  { "add", "rm", "ls", "err" }, "" }}
};



/*!
 * Command line options
 */
struct Option
{
   enum option_t { OPT_FLAG, OPT_BOOL, OPT_INT, OPT_STR };

   option_t    type;
   std::string long_opt;
   std::string short_opt;
   std::string help_txt;
   bool        optional;
};

/*
 * Enumerate options
 */
const Option opt_header   = { Option::OPT_FLAG, "--header",   "-h", "",              true  };
const Option opt_comment  = { Option::OPT_STR,  "--comment",  "-m", "<\"comment\">", false };
const Option opt_hostname = { Option::OPT_STR,  "--hostname", "-n", "<host_name>",   false };

using cmd_key_t = std::pair<AdminCmd::Cmd, AdminCmd::SubCmd>;
using cmd_val_t = std::vector<Option>;

/*!
 * Map valid options to commands
 */
const std::map<cmd_key_t, cmd_val_t> command = {
   { { AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_ADD }, { opt_hostname, opt_comment }},
   { { AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_CH  }, { opt_hostname, opt_comment }},
   { { AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_RM  }, { opt_hostname }},
   { { AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_LS  }, { opt_header }},
};

}} // namespace cta::admin

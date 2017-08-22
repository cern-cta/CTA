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

typedef std::map<std::string, AdminCmd::Cmd> cmdLookup_t;
typedef std::map<std::string, AdminCmd::SubCmd> subCmdLookup_t;



/*!
 * Map short and long command names to Protocol Buffer enum values
 */
const cmdLookup_t cmdLookup = {
   { "admin",                AdminCmd::CMD_ADMIN },
   { "ad",                   AdminCmd::CMD_ADMIN },
   { "adminhost",            AdminCmd::CMD_ADMINHOST },
   { "ah",                   AdminCmd::CMD_ADMINHOST },
   { "archivefile",          AdminCmd::CMD_ARCHIVEFILE },
   { "af",                   AdminCmd::CMD_ARCHIVEFILE },
   { "archiveroute",         AdminCmd::CMD_ARCHIVEROUTE },
   { "ar",                   AdminCmd::CMD_ARCHIVEROUTE },
   { "drive",                AdminCmd::CMD_DRIVE },
   { "dr",                   AdminCmd::CMD_DRIVE },
   { "groupmountrule",       AdminCmd::CMD_GROUPMOUNTRULE },
   { "gmr",                  AdminCmd::CMD_GROUPMOUNTRULE },
   { "listpendingarchives",  AdminCmd::CMD_LISTPENDINGARCHIVES },
   { "lpa",                  AdminCmd::CMD_LISTPENDINGARCHIVES },
   { "listpendingretrieves", AdminCmd::CMD_LISTPENDINGRETRIEVES },
   { "lpr",                  AdminCmd::CMD_LISTPENDINGRETRIEVES },
   { "logicallibrary",       AdminCmd::CMD_LOGICALLIBRARY },
   { "ll",                   AdminCmd::CMD_LOGICALLIBRARY },
   { "mountpolicy",          AdminCmd::CMD_MOUNTPOLICY },
   { "mp",                   AdminCmd::CMD_MOUNTPOLICY },
   { "repack",               AdminCmd::CMD_REPACK },
   { "re",                   AdminCmd::CMD_REPACK },
   { "requestermountrule",   AdminCmd::CMD_REQUESTERMOUNTRULE },
   { "rmr",                  AdminCmd::CMD_REQUESTERMOUNTRULE },
   { "showqueues",           AdminCmd::CMD_SHOWQUEUES },
   { "sq",                   AdminCmd::CMD_SHOWQUEUES },
   { "shrink",               AdminCmd::CMD_SHRINK },
   { "sh",                   AdminCmd::CMD_SHRINK },
   { "storageclass",         AdminCmd::CMD_STORAGECLASS },
   { "sc",                   AdminCmd::CMD_STORAGECLASS },
   { "tape",                 AdminCmd::CMD_TAPE },
   { "ta",                   AdminCmd::CMD_TAPE },
   { "tapepool",             AdminCmd::CMD_TAPEPOOL },
   { "tp",                   AdminCmd::CMD_TAPEPOOL },
   { "test",                 AdminCmd::CMD_TEST },
   { "te",                   AdminCmd::CMD_TEST },
   { "verify",               AdminCmd::CMD_VERIFY },
   { "ve",                   AdminCmd::CMD_VERIFY }
};



/*!
 * Map subcommand names to Protocol Buffer enum values
 */
const subCmdLookup_t subCmdLookup = {
   { "add",                  AdminCmd::SUBCMD_ADD },
   { "ch",                   AdminCmd::SUBCMD_CH },
   { "err",                  AdminCmd::SUBCMD_ERR },
   { "label",                AdminCmd::SUBCMD_LABEL },
   { "ls",                   AdminCmd::SUBCMD_LS },
   { "reclaim",              AdminCmd::SUBCMD_RECLAIM },
   { "rm",                   AdminCmd::SUBCMD_RM },
   { "up",                   AdminCmd::SUBCMD_UP },
   { "down",                 AdminCmd::SUBCMD_DOWN },
   { "read",                 AdminCmd::SUBCMD_READ },
   { "write",                AdminCmd::SUBCMD_WRITE },
   { "write_auto",           AdminCmd::SUBCMD_WRITE_AUTO }
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



struct Command
{
   bool tmp;
};

typedef std::pair<AdminCmd::Cmd, AdminCmd::SubCmd> cmd_key_t;

//const std::pair<cmd_key_t, Command> c{ { AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_ADD }, { true } };

const std::map<cmd_key_t, Command> command = {
   { { AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_ADD }, { true } }
};

}} // namespace cta::admin

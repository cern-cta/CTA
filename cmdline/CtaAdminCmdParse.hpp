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

struct SubCommand
{
   AdminCmd::Cmd cmd;
   std::set<std::string> sub_cmd;
};

const std::map<std::string, std::string> longCmd = {
   { "admin", "ad" },
   { "adminhost", "ah" },
   { "archivefile", "af" },
   { "archiveroute", "ar" },
   { "drive", "dr" },
   { "groupmountrule", "gmr" },
   { "listpendingarchives", "lpa" },
   { "listpendingretrieves", "lpr" },
   { "logicallibrary", "ll" },
   { "mountpolicy", "mp" },
   { "repack", "re" },
   { "requestermountrule", "rmr" },
   { "shrink", "sh" },
   { "storageclass", "sc" },
   { "tape", "ta" },
   { "tapepool", "tp" },
   { "test", "te" },
   { "verify",  "ve" }
};

const std::map<std::string, SubCommand> shortCmd = {
   { "ad",  { AdminCmd::CMD_ADMIN,                { "add", "ch", "rm", "ls" }} },
   { "ah",  { AdminCmd::CMD_ADMINHOST,            { "add", "ch", "rm", "ls" }} },
   { "af",  { AdminCmd::CMD_ARCHIVEFILE,          { "ls" }} },
   { "ar",  { AdminCmd::CMD_ARCHIVEROUTE,         { "add", "ch", "rm", "ls" }} },
   { "dr",  { AdminCmd::CMD_DRIVE,                { "up", "down", "ls" }} },
   { "gmr", { AdminCmd::CMD_GROUPMOUNTRULE,       { "add", "rm", "ls", "err" }} },
   { "lpa", { AdminCmd::CMD_LISTPENDINGARCHIVES,  {}} },
   { "lpr", { AdminCmd::CMD_LISTPENDINGRETRIEVES, {}} },
   { "ll",  { AdminCmd::CMD_LOGICALLIBRARY,       { "add", "ch", "rm", "ls" }} },
   { "mp",  { AdminCmd::CMD_MOUNTPOLICY,          { "add", "ch", "rm", "ls" }} },
   { "re",  { AdminCmd::CMD_REPACK,               { "add", "rm", "ls", "err" }} },
   { "rmr", { AdminCmd::CMD_REQUESTERMOUNTRULE,   { "add", "rm", "ls", "err" }} },
   { "sh",  { AdminCmd::CMD_SHOWQUEUES,           {}} },
   { "sc",  { AdminCmd::CMD_STORAGECLASS,         { "add", "ch", "rm", "ls" }} },
   { "ta",  { AdminCmd::CMD_TAPE,                 { "add", "ch", "rm", "reclaim", "ls", "label" }} },
   { "tp",  { AdminCmd::CMD_TAPEPOOL,             { "add", "ch", "rm", "ls" }} },
   { "te",  { AdminCmd::CMD_TEST,                 { "read", "write" }} },
   { "ve",  { AdminCmd::CMD_VERIFY,               { "add", "rm", "ls", "err" }} }
};

}} // namespace cta::admin


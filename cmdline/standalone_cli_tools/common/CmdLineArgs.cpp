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

#include "cmdline/standalone_cli_tools/common/CmdLineArgs.hpp"
#include "common/exception/CommandLineNotParsed.hpp"

#include <climits>
#include <fstream>
#include <getopt.h>
#include <map>
#include <string>

namespace cta {
namespace cliTool{


//------------------------------------------------------------------------------
// Options for each tool
//------------------------------------------------------------------------------
static struct option restoreFilesLongOption[] = {
  {"id", required_argument, nullptr, 'I'},
  {"instance", required_argument, nullptr, 'i'},
  {"fxid", required_argument, nullptr, 'f'},
  {"fxidfile", required_argument, nullptr, 'F'},
  {"vid", required_argument, nullptr, 'v'},
  {"copynb", required_argument, nullptr, 'c'},
  {"help", no_argument, nullptr, 'h'},
  {"debug", no_argument, nullptr, 'd'},
  {nullptr, 0, nullptr, 0}
};

static struct option sendFileLongOption[] = {
  {"instance", required_argument, nullptr, 'i'},
  {"eos.endpoint", required_argument, nullptr, 'e'},
  {"request.user", required_argument, nullptr, 'u'},
  {"request.group", required_argument, nullptr, 'g'},
  {nullptr, 0, nullptr, 0}
};

static struct option verifyFileLongOption[] = {
  {"instance", required_argument, nullptr, 'i'},
  {"request.user", required_argument, nullptr, 'u'},
  {"request.group", required_argument, nullptr, 'g'},
  {"vid", required_argument, nullptr, 'v'},
  {"help", no_argument, nullptr, 'h'},
  {nullptr, 0, nullptr, 0}
};

std::map<StandaloneCliTool, const option*> longopts = {
  {StandaloneCliTool::RESTORE_FILES, restoreFilesLongOption},
  {StandaloneCliTool::CTA_SEND_EVENT, sendFileLongOption},
  {StandaloneCliTool::CTA_VERIFY_FILE, verifyFileLongOption},
};

std::map<StandaloneCliTool, const char*> shortopts = {
  {StandaloneCliTool::RESTORE_FILES, "I:i:f:F:v:c:hd:"},
  {StandaloneCliTool::CTA_SEND_EVENT, "i:e:u:g:"},
  {StandaloneCliTool::CTA_VERIFY_FILE, "i:u:g:v:h:"},
};

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CmdLineArgs::CmdLineArgs(const int &argc, char *const *const &argv, const StandaloneCliTool &standaloneCliTool):
m_help(false), m_debug(false), m_standaloneCliTool{standaloneCliTool} {

  opterr = 0;
  int opt = 0;
  int opt_index = 0;

  while ((opt = getopt_long(argc, argv, shortopts[m_standaloneCliTool], longopts[m_standaloneCliTool], &opt_index)) != -1) {
    switch(opt) {
    case 'I':
      {
        m_archiveFileId = std::stol(std::string(optarg));
        break;
      }
    case 'i':
      {
        m_diskInstance = std::string(optarg);
        break;
      }
    case 'f':
      {
        if (! m_eosFids) {
          m_eosFids = std::list<uint64_t>();
        }
        auto fid = strtoul(optarg, nullptr, 16);
        if(fid < 1) {
          throw std::runtime_error(std::string(optarg) + " is not a valid file ID");
        }
        m_eosFids->push_back(fid);
        break;
      }
    case 'F':
      {
        if (! m_eosFids) {
          m_eosFids = std::list<uint64_t>();
        }
        readFidListFromFile(std::string(optarg), m_eosFids.value());
        break;
      }
    case 'v':
      {
        m_vid = std::string(optarg);
        break;
      }
    case 'c':
      {
        int64_t copyNumber = std::stol(std::string(optarg));
        if(copyNumber < 0) throw std::out_of_range("copy number value cannot be negative");
        m_copyNumber = copyNumber;
        break;
      }
    case 'h':
      {
        m_help = true;
        break;
      }
    case 'd':
      {
        m_debug = true;
        break;
      }
    case 'e':
      {
        m_eosEndpoint = std::string(optarg);
        break;
      }
    case 'u':
      {
        m_requestUser = std::string(optarg);
        break;
      }
    case 'g':
      {
        m_requestGroup = std::string(optarg);
        break;
      }
    case ':': // Missing parameter
      {
        exception::CommandLineNotParsed ex;
        ex.getMessage() << "The -" << static_cast<char>(optopt) << " option requires a parameter";
        throw ex;
      }
    case '?': // Unknown option
      {
        exception::CommandLineNotParsed ex;
        if(0 == optopt) {
          ex.getMessage() << "Unknown command-line option";
        } else {
          ex.getMessage() << "Unknown command-line option: -" << static_cast<char>(optopt) << std::endl;
        }
        printUsage(ex.getMessage());
        throw ex;
      }
    default:
      {
        exception::CommandLineNotParsed ex;
        ex.getMessage() <<
        "getopt_long returned the following unknown value: 0x" <<
        std::hex << static_cast<int>(optopt);
        throw ex;
      }
    }
  }
}

//------------------------------------------------------------------------------
// readFidListFromFile
//------------------------------------------------------------------------------
void CmdLineArgs::readFidListFromFile(const std::string &filename, std::list<std::uint64_t> &fidList) {
  std::ifstream file(filename);
  if (file.fail()) {
    throw std::runtime_error("Unable to open file " + filename);
  }

  std::string line;

  while(std::getline(file, line)) {
    // Strip out comments
    auto pos = line.find('#');
    if(pos != std::string::npos) {
      line.resize(pos);
    }

    // Extract the list items
    std::stringstream ss(line);
    while(!ss.eof()) {
      std::string item;
      ss >> item;
      // skip blank lines or lines consisting only of whitespace
      if(item.empty()) continue;

      // Special handling for file id lists. The output from "eos find --fid <fid> /path" is:
      //   path=/path fid=<fid>
      // We discard everything except the list of fids. <fid> is a zero-padded hexadecimal number,
      // but in the CTA catalogue we store disk IDs as a decimal string, so we need to convert it.
      if(item.substr(0, 4) == "fid=") {
        auto fid = strtol(item.substr(4).c_str(), nullptr, 16);
        if(fid < 1 || fid == LONG_MAX) {
          throw std::runtime_error(item + " is not a valid file ID");
        }
        fidList.push_back(fid);
      } else {
        continue;
      }
    }
  }
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void CmdLineArgs::printUsage(std::ostream &os) const {
  switch (m_standaloneCliTool) {
  case StandaloneCliTool::RESTORE_FILES: 
    os << "   Usage:" << std::endl <<
    "      cta-restore-deleted-files [--id/-I <archive_file_id>] [--instance/-i <disk_instance>]" << std::endl <<
    "                                [--fxid/-f <eos_fxid>] [--fxidfile/-F <filename>]" << std::endl <<
    "                                [--vid/-v <vid>] [--copynb/-c <copy_number>] [--debug/-d]" << std::endl; 
    break;
  case StandaloneCliTool::CTA_SEND_EVENT:
    os << "    Usage:" << std::endl <<
    "    eos --json fileinfo /eos/path | cta-send-event CLOSEW|PREPARE " << std::endl <<
    "                        -i/--eos.instance <instance> [-e/--eos.endpoint <url>]" << std::endl << 
    "                        -u/--request.user <user> -g/--request.group <group>" << std::endl;
    break;
  case StandaloneCliTool::CTA_VERIFY_FILE :
    os << "    Usage:" << std::endl <<
    "    cta-verify-file <archiveFileID> --vid/-v <vid> [--instance/-i <instance>] [--request.user/-u <user>] [request.group/-g <group>]\n" << std::endl;
    break;
  default:
    break;
  }
}

} // namespace admin
} // namespace cta
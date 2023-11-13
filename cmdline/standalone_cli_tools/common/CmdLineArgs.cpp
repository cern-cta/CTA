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
#include <iostream>
#include <map>
#include <string>

namespace cta::cliTool {

//------------------------------------------------------------------------------
// Options for each tool
//------------------------------------------------------------------------------
static struct option restoreFilesLongOption[] = {
  {"id", required_argument, nullptr, 'I'},
  {"instance", required_argument, nullptr, 'i'},
  {"fid", required_argument, nullptr, 'f'},
  {"filename", required_argument, nullptr, 'F'},
  {"vid", required_argument, nullptr, 'v'},
  {"copynb", required_argument, nullptr, 'c'},
  {"help", no_argument, nullptr, 'h'},
  {"debug", no_argument, nullptr, 'd'},
  {nullptr, 0, nullptr, 0}
};

static struct option sendEventLongOption[] = {
  {"eos.instance", required_argument, nullptr, 'i'},
  {"eos.endpoint", required_argument, nullptr, 'e'},
  {"request.user", required_argument, nullptr, 'u'},
  {"request.group", required_argument, nullptr, 'g'},
  {nullptr, 0, nullptr, 0}
};

static struct option verifyFileLongOption[] = {
  {"id", required_argument, nullptr, 'I'},
  {"filename", required_argument, nullptr, 'F'},
  {"instance", required_argument, nullptr, 'i'},
  {"request.user", required_argument, nullptr, 'u'},
  {"request.group", required_argument, nullptr, 'g'},
  {"vid", required_argument, nullptr, 'v'},
  {"help", no_argument, nullptr, 'h'},
  {nullptr, 0, nullptr, 0}
};

static struct option changeStorageClassLongOption[] = {
  {"id", required_argument, nullptr, 'I'},
  {"json", required_argument, nullptr, 'j'},
  {"storageclassname", required_argument, nullptr, 'n'},
  {"fid", required_argument, nullptr, 'f'},
  {"instance", required_argument, nullptr, 'i'},
  {"frequency", required_argument, nullptr, 't'},
  {"help", no_argument, nullptr, 'h'},
  {nullptr, 0, nullptr, 0}
};

static struct option eosNamespaceinjectionLongOption[] = {
  {"help", no_argument, nullptr, 'h'},
  {"json", required_argument, nullptr, 'j'},
  {nullptr, 0, nullptr, 0}
};

std::map<StandaloneCliTool, const option*> longopts = {
  {StandaloneCliTool::RESTORE_FILES, restoreFilesLongOption},
  {StandaloneCliTool::CTA_SEND_EVENT, sendEventLongOption},
  {StandaloneCliTool::CTA_VERIFY_FILE, verifyFileLongOption},
  {StandaloneCliTool::CTA_CHANGE_STORAGE_CLASS, changeStorageClassLongOption},
  {StandaloneCliTool::EOS_NAMESPACE_INJECTION, eosNamespaceinjectionLongOption},
};

std::map<StandaloneCliTool, const char*> shortopts = {
  {StandaloneCliTool::RESTORE_FILES, "I:i:f:F:v:c:hd:"},
  {StandaloneCliTool::CTA_SEND_EVENT, "i:e:u:g:"},
  {StandaloneCliTool::CTA_VERIFY_FILE, "I:F:i:u:g:v:h:"},
  {StandaloneCliTool::CTA_CHANGE_STORAGE_CLASS, "I:f:i:j:n:t:h:"},
  {StandaloneCliTool::EOS_NAMESPACE_INJECTION, "j:h:"},
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
    case 'c':
      {
        int64_t copyNumber = std::stol(std::string(optarg));
        if(copyNumber < 0) throw std::out_of_range("copy number value cannot be negative");
        m_copyNumber = copyNumber;
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
    case 'f':
      {
        if (! m_fids) {
          m_fids = std::list<std::string>();
        }
        m_fids->push_back(optarg);
        break;
      }
    case 'F':
      {
        readIdListFromFile(std::string(optarg));
        break;
      }
    case 'g':
      {
        m_requestGroup = std::string(optarg);
        break;
      }
    case 'h':
      {
        m_help = true;
        break;
      }
    case 'I':
      {
        m_archiveFileId = std::string(optarg);
        break;
      }
    case 'i':
      {
        m_diskInstance = std::string(optarg);
        break;
      }
    case 'j':
      {
        m_json = std::string(optarg);
        break;
      }
    case 'n':
      {
        m_storageClassName = std::string(optarg);
        break;
      }
    case 't':
      {
        m_frequency = std::stol(std::string(optarg));
        break;
      }
    case 'u':
      {
        m_requestUser = std::string(optarg);
        break;
      }
    case 'v':
      {
        m_vid = std::string(optarg);
        break;
      }
    case ':': // Missing parameter
      {
        exception::CommandLineNotParsed ex("", false);
        ex.getMessage() << "The -" << static_cast<char>(optopt) << " option requires a parameter";
        throw ex;
      }
    case '?': // Unknown option
      {
        exception::CommandLineNotParsed ex("", false);
        if(0 == optopt) {
          ex.getMessage() << "Unknown command-line option" << std::endl;
        } else {
          ex.getMessage() << "Unknown command-line option: -" << static_cast<char>(optopt) << std::endl;
        }
        printUsage(std::cout);
        throw ex;
      }
    default:
      {
        exception::CommandLineNotParsed ex("", false);
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
void CmdLineArgs::readIdListFromFile(const std::string &filename) {
  std::ifstream file(filename);
  if (file.fail()) {
    throw std::runtime_error("Unable to open file " + filename);
  }

  std::string line;

  while(file >> line) {
    switch (m_standaloneCliTool) {
      case StandaloneCliTool::RESTORE_FILES:
        if (!m_fids) {
          m_fids = std::list<std::string>();
        }
        m_fids.value().push_back(line);
        break;
      case StandaloneCliTool::CTA_VERIFY_FILE:
        if (!m_archiveFileIds) {
          m_archiveFileIds = std::list<std::string>();
        }
        m_archiveFileIds.value().push_back(line);
        break;
      case StandaloneCliTool::CTA_CHANGE_STORAGE_CLASS:
        if (!m_archiveFileIds) {
          m_archiveFileIds = std::list<std::string>();
        }
        m_archiveFileIds.value().push_back(line);
        break;
      default:
        break;
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
    "                                [--fid/-f <eos_fxid>] [--filename/-F <filename>]" << std::endl <<
    "                                [--vid/-v <vid>] [--copynb/-c <copy_number>] [--debug/-d]" << std::endl;
    break;
  case StandaloneCliTool::CTA_SEND_EVENT:
    os << "    Usage:" << std::endl <<
    "    eos --json fileinfo /eos/path | cta-send-event CLOSEW|PREPARE " << std::endl <<
    "                        -i/--eos.instance <instance> [-e/--eos.endpoint <url>]" << std::endl <<
    "                        -u/--request.user <user> -g/--request.group <group>" << std::endl << std::endl;
    break;
  case StandaloneCliTool::CTA_VERIFY_FILE :
    os <<
    "cta-verify-file --vid/-v <vid> [--id/-I <archiveFileID,...>] [--filename/-F <filename>] [--instance/-i <instance>] [--request.user/-u <user>] [request.group/-g <group>]" << std::endl << std::endl <<
    "  Submit a verification request for a given list of archive file IDs on a given tape." << std::endl <<
    "   * If the --filename option is used, each line in the provided file should contain exactly one <archiveFileID>." << std::endl << std::endl;
    break;
  case StandaloneCliTool::CTA_CHANGE_STORAGE_CLASS :
    os << "    Usage:" << std::endl <<
    "    cta-change-storage-class --id/-I <archiveFileID> | --json/-j <path> --storageclassname/-n <storageClassName> [--frequency/-t <eosRequestFrequency>]" << std::endl << std::endl;
    break;
  case StandaloneCliTool::EOS_NAMESPACE_INJECTION :
    os << "    Usage:" << std::endl <<
    "    cta-eos-namespace-inject --json/--j <jsonPath> [--help/--h] \n";
    break;
  default:
    break;
  }
}

} // namespace cta::cliTool

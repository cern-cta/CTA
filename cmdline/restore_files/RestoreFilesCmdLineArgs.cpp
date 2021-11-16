/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include "RestoreFilesCmdLineArgs.hpp"
#include "common/exception/CommandLineNotParsed.hpp"

#include <getopt.h>
#include <ostream>
#include <sstream>
#include <iostream>
#include <string>
#include <limits.h>
#include <fstream>


namespace cta {
namespace admin{

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RestoreFilesCmdLineArgs::RestoreFilesCmdLineArgs(const int argc, char *const *const argv):
m_help(false), m_debug(false) {
    
  static struct option longopts[] = {
    {"id", required_argument, NULL, 'I'},
    {"instance", required_argument, NULL, 'i'},
    {"fxid", required_argument, NULL, 'f'},
    {"fxidfile", required_argument, NULL, 'F'},
    {"vid", required_argument, NULL, 'v'},
    {"copynb", required_argument, NULL, 'c'},
    {"help", no_argument, NULL, 'h'},
    {"debug", no_argument, NULL, 'd'},
    {NULL, 0, NULL, 0}
  };

  opterr = 0;
  int opt = 0;
  int opt_index = 3;

  while ((opt = getopt_long(argc, argv, "I:i:f:F:v:c:hd", longopts, &opt_index)) != -1) {
    switch(opt) {
    case 'I':
      {
        int64_t archiveId = std::stol(std::string(optarg));
        if(archiveId < 0) throw std::out_of_range("archive id value cannot be negative");
        m_archiveFileId = archiveId;
        break;
      }
    case 'i':
      {
        m_diskInstance = std::string(optarg);
        break;
      }
    case 'f':
      {
        if (! m_eosFxids) {
          m_eosFxids = std::list<std::string>();
        }
        auto fid = strtol(optarg, nullptr, 16);
        if(fid < 1 || fid == LONG_MAX) {
          throw std::runtime_error(std::string(optarg) + " is not a valid file ID");
        }
        m_eosFxids->push_back(std::to_string(fid));
        break;
      }
    case 'F':
      {
        if (! m_eosFxids) {
          m_eosFxids = std::list<std::string>();
        }
        readFidListFromFile(std::string(optarg), m_eosFxids.value());
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
    case ':': // Missing parameter
      {
        exception::CommandLineNotParsed ex;
        ex.getMessage() << "The -" << (char)optopt << " option requires a parameter";
        throw ex;
      }
    case '?': // Unknown option
      {
        exception::CommandLineNotParsed ex;
        if(0 == optopt) {
          ex.getMessage() << "Unknown command-line option";
        } else {
          ex.getMessage() << "Unknown command-line option: -" << (char)optopt;
        }
        throw ex;
      }
    default:
      {
        exception::CommandLineNotParsed ex;
        ex.getMessage() <<
        "getopt_long returned the following unknown value: 0x" <<
        std::hex << (int)opt;
        throw ex;
      }
    }
  }
}

//------------------------------------------------------------------------------
// readFidListFromFile
//------------------------------------------------------------------------------
void RestoreFilesCmdLineArgs::readFidListFromFile(const std::string &filename, std::list<std::string> &optionList) {
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
        optionList.push_back(std::to_string(fid));
      } else {
        continue;
      }
    }
  }
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void RestoreFilesCmdLineArgs::printUsage(std::ostream &os) {
    os << "Usage:" << std::endl <<
    "  cta-restore-deleted-files [--id/-I <archive_file_id>] [--instance/-i <disk_instance>]" << std::endl <<
    "                            [--fxid/-f <eos_fxid>] [--fxidfile/-F <filename>]" << std::endl <<
    "                            [--vid/-v <vid>] [--copynb/-c <copy_number>] [--debug/-d]" << std::endl;
}

} // namespace admin
} // namespace cta
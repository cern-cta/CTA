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

#include "tapeserver/readtp/ReadtpCmdLineArgs.hpp"
#include "tapeserver/readtp/TapeFseqSequenceParser.hpp"
#include "common/exception/CommandLineNotParsed.hpp"
#include "common/utils/utils.hpp"
#include "common/Constants.hpp"

#include <getopt.h>
#include <ostream>

#include <string.h>

namespace cta::tapeserver::readtp {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ReadtpCmdLineArgs::ReadtpCmdLineArgs(const int argc, char *const *const argv) :
  help(false), m_vid(""), m_destinationFileListURL("") {
  if (argc < 3) {
    help = true;
    return;
  }
  if(strnlen(argv[1], CA_MAXVIDLEN+1) > CA_MAXVIDLEN) {
    exception::CommandLineNotParsed ex;
    ex.getMessage() << "The vid is too long";
    throw ex;
  }
  m_vid = std::string(argv[1]);
  utils::toUpper(m_vid);
  
  m_fSeqRangeList = TapeFileSequenceParser::parse(argv[2]);
  
  static struct option longopts[] = {
    {"destination_files",      required_argument, nullptr, 'f'},
    {"drive",                  required_argument, nullptr, 'u'},
    {"help",                   no_argument,       nullptr, 'h'},
    {nullptr,                  0,                 nullptr,   0}
  };

  opterr = 0;
  int opt = 0;
  int opt_index = 3;

  while ((opt = getopt_long(argc, argv, ":d:f:u:p:h", longopts, &opt_index)) != -1) {
    switch(opt) {
    case 'f':
      m_destinationFileListURL = std::string(optarg);
      break;
    case 'u':
      m_unitName = std::string(optarg);
      break;
    case 'h':
      help = true;
      break;
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
    } // switch(opt)
  } // while getopt_long()

  if (m_destinationFileListURL.empty()) {
    m_destinationFileListURL = "/dev/null"; // Equivalent to an empty file
  }         
}


//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void ReadtpCmdLineArgs::printUsage(std::ostream &os) {
  os <<
    "Usage:" << std::endl <<
    "  cta-readtp <VID> <SEQUENCE> [options]" << std::endl <<
    "Where:" << std::endl <<
    "  <VID>            The VID of the tape to be read" << std::endl <<
    "  <SEQUENCE>       A sequence of tape file sequence numbers. The syntax to be used is:" << std::endl <<
    "      f1-f2            Files f1 to f2 inclusive" << std::endl <<
    "      f1-              Files f1 to the last file on the tape" << std::endl <<
    "      f1-f2,f4,f6-     A series of non-consecutive ranges of files" << std::endl <<
    "Options:" <<std::endl <<
    "  -h, --help                              Print this help message and exit." << std::endl <<
    "  -u, --drive                             The unit name of the drive used (if absent, the first drive configuration file found is used)" << std::endl <<
    "  -f, --destination_files <FILE URL>      URL to file containing a list of destination files."  << std::endl <<
    "                                          If not set, all data read is written to file:///dev/null" << std::endl <<
    "                                          If there are less destination files than read files, the remaining" << std::endl <<
    "                                          files read will be written to file:///dev/null." << std::endl;  
}

} // namespace cta::tapeserver::readtp

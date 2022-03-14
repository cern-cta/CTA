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

#include "tapeserver/tapelabel/TapeLabelCmdLineArgs.hpp"
#include "common/exception/CommandLineNotParsed.hpp"
#include "common/utils/utils.hpp"
#include "common/Constants.hpp"

#include <getopt.h>
#include <ostream>

#include <string.h>

namespace cta {
namespace tapeserver {
namespace tapelabel {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeLabelCmdLineArgs::TapeLabelCmdLineArgs(const int argc, char *const *const argv):
  help(false), m_debug(false), m_force(false) {

  static struct option longopts[] = {
    {"vid", required_argument, nullptr, 'v'},
    {"oldlabel", required_argument, nullptr, 'o'},
    {"debug", no_argument, nullptr, 'd'},
    {"force", no_argument, nullptr, 'f'},
    {"help", no_argument, nullptr, 'h'},
    {nullptr,          0, nullptr,   0}
  };
 
  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;

  int opt = 0;

  while((opt = getopt_long(argc, argv, ":v:o:hdf", longopts, nullptr)) != -1) {
    switch(opt) {
    case 'v':
      if (strlen(optarg) > CA_MAXVIDLEN) {
        exception::CommandLineNotParsed ex;
        ex.getMessage() << "The -" << (char)opt << " option too big";
        throw ex;
      } else {
        m_vid = std::string(optarg);
        utils::toUpper(m_vid);
      }
      break;
    case 'o':
      if (strlen(optarg) > CA_MAXVIDLEN) {
        exception::CommandLineNotParsed ex;
        ex.getMessage() << "The -" << (char)opt << " option too big";
        throw ex;
      } else {
        m_oldLabel = std::string(optarg);
	utils::toUpper(m_oldLabel);
      }
      break;
    case 'h':
      help = true;
      break;
    case 'd':
      m_debug = true;
      break;
    case 'f':
      m_force = true;
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

  if (m_vid.empty() && !help) {
    exception::CommandLineNotParsed ex;
        ex.getMessage() <<
          "--vid/-v VID must be specified";
        throw ex;
  }
  // There is no need to continue parsing when the help option is set
  if(help) {
    return;
  }
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void TapeLabelCmdLineArgs::printUsage(std::ostream &os) {
  os <<
    "Usage:" << std::endl <<
    "  cta-tape-label [options] --vid/-v VID" << std::endl <<
    "Where:" << std::endl <<
    "  -v, --vid        The VID of the tape to be labeled" << std::endl <<
    "Options:" <<std::endl <<
    "  -o, --oldlabel   The vid of the current tape label on the tape if it is not the same as VID" << std::endl <<
    "  -h, --help       Print this help message and exit" << std::endl <<
    "  -d, --debug      Print more logs for label operations" << std::endl <<
    "  -f, --force      Force labeling for not-blank tapes for testing purpose and without label checks. Must only be used manually." << std::endl;  
}

} // namespace tapelabel
} // namespace catalogue
} // namespace cta

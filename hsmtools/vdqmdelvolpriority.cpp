/******************************************************************************
 *                      vdqmsetvolpriority.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <Cgetopt.h>
#include <iostream>
#include <string>


void usage(const std::string programName) {
  std::cerr << "Usage: " << programName << " [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t-v, --vid VID             Volume visual Identifier\n"
    "\t-a, --tapeAccessMode mode Tape access mode 0 or 1\n"
    "\t-h, --help                Print this help and exit\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}


static struct Coptions longopts[] = {
  {"vid"           , REQUIRED_ARGUMENT, NULL, 'v'},
  {"tapeAccessMode", REQUIRED_ARGUMENT, NULL, 'a'},
  {"help"          , NO_ARGUMENT      , NULL, 'h'}
};


void parseCommandLine(int argc, char **argv) {

  bool vidSet    = false;
  bool tpModeSet = false;
  char c;


  Coptind = 1;
  Copterr = 0;

  while ((c = Cgetopt_long (argc, argv, "v:a:h", longopts, NULL)) != -1) {
    switch (c) {
    case 'v':
      vidSet = true;
      break;
    case 'a':
      tpModeSet = true;
      break;
    case 'h':
      usage(argv[0]);
      exit(0);
    case '?':
      std::cerr
        << std::endl
        << "Error: Unknown command-line option: " << (char)Coptopt
        << std::endl << std::endl;
      usage(argv[0]);
      exit(1);
    case ':':
      std::cerr
        << std::endl
        << "Error: An option is missing a parameter"
        << std::endl << std::endl;
      usage(argv[0]);
      exit(1);
    default:
      std::cerr
        << std::endl
        << "Internal error: Cgetopt_long returned the following unknown value: "
        << "0x" << std::hex << (int)c << std::dec
        << std::endl << std::endl;
      exit(1);
    }
  }

  if(Coptind > argc) {
    std::cerr
      << std::endl
      << "Internal error.  Invalid value for Coptind: " << Coptind
      << std::endl;
    exit(1);
  }

  // Best to abort if there is some extra text on the command-line which has
  // not been parsed as it could indicate that a valid option never got parsed
  if(Coptind < argc)
  {
    std::cerr
      << std::endl
      << "Error:  Unexpected command-line argument: "
      << argv[Coptind]
      << std::endl << std::endl;
    usage(argv[0]);
    exit(1);
  }

  if(!(vidSet && tpModeSet)) {
    std::cerr
      << std::endl
      << "Error:  VID and tape access mode must be provided"
      << std::endl << std::endl;
    usage(argv[0]);
    exit(1);
  }
}


int main(int argc, char **argv) {
//std::string vid      = "";
//int         tpMode   = 0;

  parseCommandLine(argc, argv);
}

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

#include "mediachanger/CmdLine.hpp"
#include "common/exception/InvalidArgument.hpp"
#include "common/exception/MissingOperand.hpp"

#include <getopt.h>

//------------------------------------------------------------------------------
// handleMissingParameter
//------------------------------------------------------------------------------
void cta::mediachanger::CmdLine::handleMissingParameter(const int opt) {
  cta::exception::MissingOperand ex;
  ex.getMessage() << "The -" << (char) opt << " option requires a parameter";
  throw ex;
}

//------------------------------------------------------------------------------
// handleUnknownOption
//------------------------------------------------------------------------------
void cta::mediachanger::CmdLine::handleUnknownOption(const int opt) {
  cta::exception::InvalidArgument ex;
  if (0 == optopt) {
    ex.getMessage() << "Unknown command-line option";
  }
  else {
    ex.getMessage() << "Unknown command-line option: -" << (char) opt;
  }
  throw ex;
}

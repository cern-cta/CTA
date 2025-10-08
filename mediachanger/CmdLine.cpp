/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
  ex.getMessage() << "The -" << (char)opt << " option requires a parameter";
 throw ex;
}

//------------------------------------------------------------------------------
// handleUnknownOption
//------------------------------------------------------------------------------
void cta::mediachanger::CmdLine::handleUnknownOption(const int opt) {
  cta::exception::InvalidArgument ex;
  if(0 == optopt) {
    ex.getMessage() << "Unknown command-line option";
  } else {
    ex.getMessage() << "Unknown command-line option: -" << (char)opt;
  }
  throw ex;
}

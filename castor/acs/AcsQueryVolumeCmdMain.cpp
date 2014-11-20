/******************************************************************************
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
 
#include "castor/acs/AcsImpl.hpp"
#include "castor/acs/AcsQueryVolumeCmd.hpp"
#include "castor/acs/AcsQueryVolumeCmdLine.hpp"

#include <iostream>

/**
 * An exception throwing version of main().
 *
 * @param argc The number of command-line arguments including the program name.
 * @param argv The command-line arguments.
 */ 
static void exceptionThrowingMain(const int argc, char *const *const argv);
  
//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char *const *const argv) {
  using namespace castor;
  std::string errorMessage;

  try {
    exceptionThrowingMain(argc, argv);
    return 0;
  } catch(castor::exception::Exception &ex) {
    errorMessage = ex.getMessage().str();
  } catch(std::exception &se) {
    errorMessage = se.what();
  } catch(...) {
    errorMessage = "An unknown exception was thrown";
  }

  // Reaching this point means the command has failed, ane exception was throw
  // and errorMessage has been set accordingly

  std::cerr << "Aborting: " << errorMessage << std::endl;
  return 1;
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
static void exceptionThrowingMain(const int argc, char *const *const argv) {
  using namespace castor;

  acs::AcsImpl acs;
  acs::AcsQueryVolumeCmd cmd(std::cin, std::cout, std::cerr, acs);

  cmd.exceptionThrowingMain(argc, argv);
}

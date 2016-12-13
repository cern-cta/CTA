/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/DeleteAllCatalogueDataCmd.hpp"
#include "catalogue/DeleteAllCatalogueDataCmdLineArgs.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/CommandLineNotParsed.hpp"

#include <iostream>

/**
 * An exception throwing version of main().
 *
 * @param argc The number of command-line arguments including the program name.
 * @param argv The command-line arguments.
 * @return The exit value of the program.
 */
static int exceptionThrowingMain(const int argc, char *const *const argv);

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char *const *const argv) {
  using namespace cta;

  bool cmdLineNotParsed = false;
  std::string errorMessage;

  try {
    return exceptionThrowingMain(argc, argv);
  } catch(exception::CommandLineNotParsed &ue) {
    errorMessage = ue.getMessage().str();
    cmdLineNotParsed = true;
  } catch(exception::Exception &ex) {
    errorMessage = ex.getMessage().str();
  } catch(std::exception &se) {
    errorMessage = se.what();
  } catch(...) {
    errorMessage = "An unknown exception was thrown";
  }

  // Reaching this point means the command has failed, an exception was throw
  // and errorMessage has been set accordingly

  std::cerr << "Aborting: " << errorMessage << std::endl;
  if(cmdLineNotParsed) {
    std::cerr << std::endl;
    catalogue::DeleteAllCatalogueDataCmdLineArgs::printUsage(std::cerr);
  }
  return 1;
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const int argc, char *const *const argv) {
  using namespace cta;

  catalogue::DeleteAllCatalogueDataCmd cmd(std::cin, std::cout, std::cerr);

  return cmd.exceptionThrowingMain(argc, argv);
}

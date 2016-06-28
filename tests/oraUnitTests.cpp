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

#include "catalogue/CatalogueTest.hpp"
#include "tests/OraUnitTestsCmdLineArgs.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>

/**
 * Prints the usage message of this unit-test program to the specified output stream.
 */
static void printUsage(std::ostream &os) {
  os <<
    "Usage:" << std::endl <<
    '\t' << "cta-oraUnitTest [Google test options] databaseConnectionFile" << std::endl;
}

/**
 * Parses the specified command-line arguments.  This should be called after
 * Google test has consumed all of its command-line options from the
 * command-line.
 */
static CmdLineArgs parseCmdLine(const int argc, char ** argv) {
  if(argc != 2) {
    std::cerr << "Invalid number of command-line arguments";
    printUsage(std::cerr);
    exit(1);
  }

  CmdLineArgs cmdLineArgs;
  cmdLineArgs.oraDbConnFile = argv[1];

  return cmdLineArgs;
}

int main(int argc, char** argv) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);

  // Google test will consume its options from the command-line and leave everything else
  g_cmdLineArgs = parseCmdLine(argc, argv);

  const int ret = RUN_ALL_TESTS();

  // Close standard in, out and error so that valgrind can be used with the
  // following command-line to track open file-descriptors:
  //
  //     valgrind --track-fds=yes
  close(0);
  close(1);
  close(2);

  return ret;
}

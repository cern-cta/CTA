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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <sqlite3.h>
#include <google/protobuf/stubs/common.h>

int main(int argc, char** argv) {
  // The unit tests use SQLite it must be initialized before they are run
  if (SQLITE_OK != sqlite3_initialize()) {
    std::cerr << "Failed to initialize SQLite" << std::endl;
    return 1;  // Error
  }

  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  const int ret = RUN_ALL_TESTS();

  // Close standard in, out and error so that valgrind can be used with the
  // following command-line to track open file-descriptors:
  //
  //     valgrind --track-fds=yes
  close(0);
  close(1);
  close(2);

  // The unit tests used SQLite and so it should shutodwn in order to release
  // its resources
  if (SQLITE_OK != sqlite3_shutdown()) {
    std::cerr << "Failed to shutdown SQLite" << std::endl;
    return 1;  // Error
  }

  ::google::protobuf::ShutdownProtobufLibrary();

  return ret;
}

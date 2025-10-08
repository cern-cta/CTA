/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <sqlite3.h>
#include <google/protobuf/stubs/common.h>

int main(int argc, char** argv) {
  // The unit tests use SQLite it must be initialized before they are run
  if(SQLITE_OK != sqlite3_initialize()) {
    std::cerr << "Failed to initialize SQLite" << std::endl;
    return 1; // Error
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
  if(SQLITE_OK != sqlite3_shutdown()) {
    std::cerr << "Failed to shutdown SQLite" << std::endl;
    return 1; // Error
  }

  ::google::protobuf::ShutdownProtobufLibrary();

  return ret;
}

/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

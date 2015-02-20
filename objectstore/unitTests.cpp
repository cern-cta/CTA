#include <gtest/gtest.h>
#include <gmock/gmock.h>

int main(int argc, char** argv) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  int ret = RUN_ALL_TESTS();

  // Close standard in, out and error so that valgrind can be used with the
  // following command-line to track open file-descriptors:
  //
  //     valgrind --track-fds=yes
  close(0);
  close(1);
  close(2);

  return ret;
}
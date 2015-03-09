#include "Vfs.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_VfsTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_VfsTest, trimSlashes_emptyString) {
  using namespace cta;

  Vfs vfs;
  const std::string s;
}

}
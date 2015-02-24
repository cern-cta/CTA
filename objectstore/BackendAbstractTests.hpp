#pragma once

#include <gtest/gtest.h>
#include "Backend.hpp"

class BackendAbstractTest: public ::testing::TestWithParam<cta::objectstore::Backend *> {
protected:
  BackendAbstractTest() {}
  virtual void SetUp() {
    m_os = GetParam();
  }
  cta::objectstore::Backend * m_os;
};

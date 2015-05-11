#pragma once

#include <gtest/gtest.h>
#include "cta/MiddleTierAdmin.hpp"
#include "cta/MiddleTierUser.hpp"

namespace unitTests {
  
  class MiddleTierFull {
  public:
    cta::MiddleTierAdmin * admin;
    cta::MiddleTierUser * user;
    MiddleTierFull(cta::MiddleTierAdmin * a, cta::MiddleTierUser * u):
      admin(a), user(u) {}
    MiddleTierFull(): admin(NULL), user(NULL) {}
  };

class MiddleTierAdminAbstractTest: public ::testing::TestWithParam<MiddleTierFull> {
protected:
  MiddleTierAdminAbstractTest() {}
  virtual void SetUp() {
    m_middleTier = GetParam();
  }
  MiddleTierFull m_middleTier;
};

}


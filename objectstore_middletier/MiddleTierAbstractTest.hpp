#pragma once

#include <gtest/gtest.h>
#include "cta/MiddleTierAdmin.hpp"
#include "cta/MiddleTierUser.hpp"

namespace unitTests {
  class localMiddleTier {
  public:
    virtual cta::MiddleTierAdmin & admin() = 0;
    virtual cta::MiddleTierUser & user() = 0;
    virtual ~localMiddleTier() {}
  };
  
  class MiddleTierFactory {
  public:
    virtual localMiddleTier * allocateLocalMiddleTier() = 0;
    cta::MiddleTierAdmin & permanentAdmin () { return m_localMiddleTier->admin(); }
    cta::MiddleTierUser & permanentUser () { return m_localMiddleTier->user(); }
    virtual ~MiddleTierFactory() {}
  protected:
    localMiddleTier * m_localMiddleTier;
  };

  class MiddleTierAbstractTest: public ::testing::TestWithParam<MiddleTierFactory*> {
  };

}


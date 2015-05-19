#pragma once

#include <gtest/gtest.h>
#include "middletier/common/MiddleTierAdmin.hpp"
#include "middletier/common/MiddleTierUser.hpp"
#include <memory>

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
    std::auto_ptr<localMiddleTier> m_localMiddleTier;
  };

  class MiddleTierAbstractTest: public ::testing::TestWithParam<MiddleTierFactory*> {
  };

}


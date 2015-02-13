#pragma once

#include "MockMiddleTierAdmin.hpp"

namespace cta {

/**
 * Facilitates the unit testing of the MockMiddleTierAdmin class.
 */
class TestingMockMiddleTierAdmin: public MockMiddleTierAdmin {
public:

  using MockMiddleTierAdmin::getEnclosingDirPath;
  using MockMiddleTierAdmin::splitString;
  using MockMiddleTierAdmin::trimSlashes;

}; // class TestingMockMiddleTierAdmin

} // namespace cta


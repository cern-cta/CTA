#pragma once

#include "MockClientAPI.hpp"

namespace cta {

/**
 * Facilitates the unit testing of the MockClientAPI class.
 */
class TestingMockClientAPI: public MockClientAPI {
public:

  using MockClientAPI::getEnclosingDirPath;

  using MockClientAPI::splitString;

}; // class TestingMockClientAPI

} // namespace cta


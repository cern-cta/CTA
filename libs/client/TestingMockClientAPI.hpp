#pragma once

#include "MockClientAPI.hpp"

namespace cta {

class TestingMockClientAPI: public MockClientAPI {
public:

  using MockClientAPI::getEnclosingDirPath;

}; // class TestingMockClientAPI

} // namespace cta


/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include <gtest/gtest.h>
#include "Async.cpp"
namespace unitTests {
  using namespace cta;
TEST(cta_async, AsyncCallableTakesTime) {
  auto future = cta::Async::async([]{
    ::sleep(2);
  });
  ASSERT_NO_THROW(future.get());
}

TEST(cta_async, AsyncCallableDoNothing) {
  auto future = cta::Async::async([]{
    
  });
  ASSERT_NO_THROW(future.get());
}


TEST(cta_async,AsyncCallableThrowException){
  auto future = cta::Async::async([]{
    throw cta::exception::Exception("Chuck Norris writes code that optimizes itself.");
  });
  ASSERT_THROW(future.get(),cta::exception::Exception);
}

}
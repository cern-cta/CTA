/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "range.hpp"
#include <list>

#include <gtest/gtest.h>

namespace unitTests {

TEST(cta_rangeTest, basic) {
  using namespace cta;
  
  std::list<size_t> l;
  // Test forward
  for (const auto &i: range<size_t>(0, 4)) l.emplace_back(i);
  ASSERT_EQ(std::list<size_t>({0,1,2,3}), l);
  l.clear();
  // Test forward with step.
  for (const auto &i: range<size_t>(0, 10, 3)) l.emplace_back(i);
  ASSERT_EQ(std::list<size_t>({0,3,6,9}), l);
  l.clear();
  // Test forward overflow
  const auto max = std::numeric_limits<size_t>::max();
  for (const auto &i: range<size_t>(max - 10, max, 3)) l.emplace_back(i);
  ASSERT_EQ(std::list<size_t>({max-10, max-7, max-4, max-1}), l);
  l.clear();
  // Test backwards
  for (const auto &i: range<size_t>(10, 5)) l.emplace_back(i);
  ASSERT_EQ(std::list<size_t>({10, 9, 8, 7, 6}), l);
  l.clear();
  // Test backwards with step and underflow.
  for (const auto &i: range<size_t>(10, 0, 3)) l.emplace_back(i);
  ASSERT_EQ(std::list<size_t>({10, 7, 4, 1}), l);

}

}
/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
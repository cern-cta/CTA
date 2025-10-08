/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "GetOptThreadSafe.hpp"

#include <gtest/gtest.h>

namespace unitTests {
TEST (GetOptThreadSafe, BasicTest) {
  cta::utils::GetOpThreadSafe::Request request;
  request.argv = { "down", "-f" };
  request.optstring = { "f" };
  struct ::option longOptions[] = { { "force", no_argument, 0 , 'f' }, { 0, 0, 0, 0 } };
  request.longopts = longOptions;
  auto reply = cta::utils::GetOpThreadSafe::getOpt(request);
  ASSERT_EQ(0, reply.remainder.size());
  ASSERT_EQ(1, reply.options.size());
  ASSERT_EQ("f", reply.options.at(0).option);
  ASSERT_EQ("", reply.options.at(0).parameter);

  request.argv = { "down", "--force", "myDrive" };
  reply = cta::utils::GetOpThreadSafe::getOpt(request);
  ASSERT_EQ(1, reply.remainder.size());
  ASSERT_EQ("myDrive", reply.remainder.at(0));
  ASSERT_EQ(1, reply.options.size());
  ASSERT_EQ("f", reply.options.at(0).option);
  ASSERT_EQ("", reply.options.at(0).parameter);

  request.argv = { "down", "myDrive" , "--force"};
  reply = cta::utils::GetOpThreadSafe::getOpt(request);
  ASSERT_EQ(1, reply.remainder.size());
  ASSERT_EQ("myDrive", reply.remainder.at(0));
  ASSERT_EQ(1, reply.options.size());
  ASSERT_EQ("f", reply.options.at(0).option);
  ASSERT_EQ("", reply.options.at(0).parameter);
}
} // namespace

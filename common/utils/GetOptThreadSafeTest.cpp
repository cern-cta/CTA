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

#include "GetOptThreadSafe.hpp"

#include <gtest/gtest.h>

namespace unitTests {
TEST(GetOptThreadSafe, BasicTest) {
  cta::utils::GetOpThreadSafe::Request request;
  request.argv = {"down", "-f"};
  request.optstring = {"f"};
  struct ::option longOptions[] = {
    {"force", no_argument, 0, 'f'},
    {0,       0,           0, 0  }
  };
  request.longopts = longOptions;
  auto reply = cta::utils::GetOpThreadSafe::getOpt(request);
  ASSERT_EQ(0, reply.remainder.size());
  ASSERT_EQ(1, reply.options.size());
  ASSERT_EQ("f", reply.options.at(0).option);
  ASSERT_EQ("", reply.options.at(0).parameter);

  request.argv = {"down", "--force", "myDrive"};
  reply = cta::utils::GetOpThreadSafe::getOpt(request);
  ASSERT_EQ(1, reply.remainder.size());
  ASSERT_EQ("myDrive", reply.remainder.at(0));
  ASSERT_EQ(1, reply.options.size());
  ASSERT_EQ("f", reply.options.at(0).option);
  ASSERT_EQ("", reply.options.at(0).parameter);

  request.argv = {"down", "myDrive", "--force"};
  reply = cta::utils::GetOpThreadSafe::getOpt(request);
  ASSERT_EQ(1, reply.remainder.size());
  ASSERT_EQ("myDrive", reply.remainder.at(0));
  ASSERT_EQ(1, reply.options.size());
  ASSERT_EQ("f", reply.options.at(0).option);
  ASSERT_EQ("", reply.options.at(0).parameter);
}
}  // namespace unitTests

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

#include <gtest/gtest.h>
#include <algorithm>

#include "common/threading/SocketPair.hpp"
#include "common/exception/Errnum.hpp"

namespace unitTests {

TEST(cta_threading_SocketPair, BasicTest) {
  using cta::server::SocketPair;
  cta::server::SocketPair sp0, sp1;
  SocketPair::pollMap pollList;
  pollList["0"] = &sp0;
  pollList["1"] = &sp1;
  sp0.send("C2P0", SocketPair::Side::parent);
  sp0.send("P2C0", SocketPair::Side::child);
  // We should have something to read
  SocketPair::poll(pollList, 0, SocketPair::Side::parent);
  ASSERT_TRUE(sp0.pollFlag());
  ASSERT_FALSE(sp1.pollFlag());
  ASSERT_EQ("P2C0", sp0.receive(SocketPair::Side::parent));
  // Nothing to read (= timeout)
  ASSERT_THROW(SocketPair::poll(pollList, 0, SocketPair::Side::parent), cta::server::SocketPair::Timeout);
  // We should have something to read from child.
  SocketPair::poll(pollList, 0, SocketPair::Side::child);
  ASSERT_TRUE(sp0.pollFlag());
  ASSERT_FALSE(sp1.pollFlag());
  ASSERT_EQ("C2P0", sp0.receive(SocketPair::Side::child));
  ASSERT_THROW(sp0.receive(SocketPair::Side::child), SocketPair::NothingToReceive);
}

TEST(cta_threading_SocketPair, Multimessages) {
  using cta::server::SocketPair;
  cta::server::SocketPair sp;
  SocketPair::pollMap pollList;
  pollList["0"] = &sp;
  sp.send("C2P0", SocketPair::Side::parent);
  sp.send("C2P1", SocketPair::Side::parent);
  sp.send("C2P2", SocketPair::Side::parent);
  // We should have something to read
  SocketPair::poll(pollList, 0, SocketPair::Side::child);
  ASSERT_TRUE(sp.pollFlag());
  // Read 2 messages
  ASSERT_EQ("C2P0", sp.receive(SocketPair::Side::child));
  ASSERT_EQ("C2P1", sp.receive(SocketPair::Side::child));
  // We should still something to read
  SocketPair::poll(pollList, 0, SocketPair::Side::child);
  ASSERT_TRUE(sp.pollFlag());
  // Read 2 messages (2nd should fail)
  ASSERT_EQ("C2P2", sp.receive(SocketPair::Side::child));
  ASSERT_THROW(sp.receive(SocketPair::Side::child), SocketPair::NothingToReceive);
  // Nothing to read (= timeout)
  ASSERT_THROW(SocketPair::poll(pollList, 0, SocketPair::Side::child), cta::server::SocketPair::Timeout);
}

TEST(cta_threading_SocketPair, MaxLength) {
  // Try to send and receive messages up to 100kB
  std::string smallMessage = "Hello!";
  std::string bigMessage;
  int i = 0;
  bigMessage.resize(10 * 1024, '.');
  std::for_each(bigMessage.begin(), bigMessage.end(), [&](char& c) { c = 'A' + (i++ % 26); });
  std::string hugeMessage;
  hugeMessage.resize(100 * 1024, '.');
  std::for_each(hugeMessage.begin(), hugeMessage.end(), [&](char& c) { c = 'Z' - (i++ % 26); });
  // 2) send/receive them
  using cta::server::SocketPair;
  cta::server::SocketPair sp;
  sp.send(smallMessage, SocketPair::Side::parent);
  sp.send(bigMessage, SocketPair::Side::parent);
  sp.send(hugeMessage, SocketPair::Side::parent);
  sp.send(smallMessage, SocketPair::Side::parent);
  ASSERT_EQ(smallMessage, sp.receive(SocketPair::Side::child));
  ASSERT_EQ(bigMessage, sp.receive(SocketPair::Side::child));
  ASSERT_EQ(hugeMessage, sp.receive(SocketPair::Side::child));
  ASSERT_EQ(smallMessage, sp.receive(SocketPair::Side::child));
}

}  // namespace unitTests

/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/process/threading/SocketPair.hpp"

#include "common/exception/Errnum.hpp"

#include <algorithm>
#include <gtest/gtest.h>

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
  // 1) Try to send and receive messages up to 100 kB
  std::string smallMessage = "Hello!";
  std::string bigMessage;
  int i = 0;
  bigMessage.resize(10 * 1024, '.');
  std::for_each(bigMessage.begin(), bigMessage.end(), [&i](char& c) { c = 'A' + (i++ % 26); });
  std::string hugeMessage;
  hugeMessage.resize(100 * 1024, '.');
  std::for_each(hugeMessage.begin(), hugeMessage.end(), [&i](char& c) { c = 'Z' - (i++ % 26); });
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

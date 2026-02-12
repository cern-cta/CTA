/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "HealthServer.hpp"

#include "common/exception/TimeOut.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/LogContext.hpp"

#include <chrono>
#include <gtest/gtest.h>
#include <httplib.h>

namespace unitTests {

const int64_t HTTP_SERVER_STARTUP_TIMEOUT_SECS = 1;

//------------------------------------------------------------------------------
// Helpers
//------------------------------------------------------------------------------

httplib::Result httpGet(const std::string& host, int64_t port, const std::string& endpoint) {
  httplib::Client cli(host, port);
  cli.set_connection_timeout(1);
  cli.set_read_timeout(5);
  cli.set_write_timeout(5);
  return cli.Get(endpoint);
}

// Technically there could be a race condition here between the port being closed here and the server picking up the port
// For tests, this should be fine though as this should basically never happen TM
// and it's safer than the alternative of hardcoding the port
int getFreePort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error("socket failed");
  }

  sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);  // 127.0.0.1
  addr.sin_port = htons(0);                       // let kernel choose

  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    throw std::runtime_error("bind failed");
  }

  socklen_t len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
    ::close(fd);
    throw std::runtime_error("getsockname failed");
  }

  int port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

void assertUDSReady(const std::string& socketPath) {
  httplib::Client cli(socketPath);
  cli.set_address_family(AF_UNIX);
  cli.set_connection_timeout(1);
  cli.set_read_timeout(5);
  cli.set_write_timeout(5);
  auto res = cli.Get("/health/ready");
  ASSERT_TRUE(res) << "connection failed";
  EXPECT_EQ(res->status, 200);
  EXPECT_NE(res->body.find("ok"), std::string::npos);
}

//------------------------------------------------------------------------------
// Tests
//------------------------------------------------------------------------------

TEST(HealthServer, ServerNotRunningWhenNotStarted) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  const std::string host = "127.0.0.1";
  const int64_t port = getFreePort();
  cta::runtime::HealthServer hs(dl, host, port, []() { return true; }, []() { return true; });
  ASSERT_FALSE(hs.isRunning());

  auto res = httpGet(host, port, "/health/live");
  ASSERT_FALSE(res) << "connection failed";
}

TEST(HealthServer, ReadyEndpoint) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  const std::string host = "127.0.0.1";
  const int64_t port = getFreePort();
  cta::runtime::HealthServer hs(dl, host, port, []() { return true; }, []() { return true; });
  hs.start();

  auto res = httpGet(host, port, "/health/ready");
  ASSERT_TRUE(res) << "connection failed";
  EXPECT_EQ(res->status, 200);
  EXPECT_NE(res->body.find("ok"), std::string::npos);
}

TEST(HealthServer, ReadyEndpointNotReady) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  const std::string host = "127.0.0.1";
  const int64_t port = getFreePort();
  cta::runtime::HealthServer hs(dl, host, port, []() { return false; }, []() { return true; });
  hs.start();

  // Here we don't use isReady because we explicitly want to check the full response
  auto res = httpGet(host, port, "/health/ready");
  ASSERT_TRUE(res) << "connection failed";
  EXPECT_EQ(res->status, 503);
  EXPECT_NE(res->body.find("not ready"), std::string::npos);
}

TEST(HealthServer, LiveEndpointAvailable) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  const std::string host = "127.0.0.1";
  const int64_t port = getFreePort();
  cta::runtime::HealthServer hs(dl, host, port, []() { return true; }, []() { return true; });
  hs.start();

  auto res = httpGet(host, port, "/health/live");
  ASSERT_TRUE(res) << "connection failed";
  EXPECT_EQ(res->status, 200);
  EXPECT_NE(res->body.find("ok"), std::string::npos);
}

TEST(HealthServer, LiveEndpointNotLive) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  const std::string host = "127.0.0.1";
  const int64_t port = getFreePort();
  cta::runtime::HealthServer hs(dl, host, port, []() { return true; }, []() { return false; });
  hs.start();

  auto res = httpGet(host, port, "/health/live");
  ASSERT_TRUE(res) << "connection failed";
  EXPECT_EQ(res->status, 503);
  EXPECT_NE(res->body.find("not live"), std::string::npos);
}

TEST(HealthServer, EmptyHostThrowsUserError) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  const std::string host = "";
  const int64_t port = getFreePort();
  ASSERT_THROW(cta::runtime::HealthServer hs(
                 dl,
                 host,
                 port,
                 []() { return true; },
                 []() { return true; },
                 200),
               cta::exception::UserError);
}

TEST(HealthServer, InvalidHost) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  const std::string host = "garbage";
  const int64_t port = getFreePort();
  cta::runtime::HealthServer hs(dl, host, port, []() { return true; }, []() { return true; }, 200);
  hs.start();
  // At this point the server actually should have failed to start, but it should not crash anything
  // It should simply time out on trying the readiness/liveness endpoints
  ASSERT_FALSE(hs.isRunning());

  auto resReady = httpGet(host, port, "/health/ready");
  ASSERT_FALSE(resReady) << "connection failed";
  auto resLive = httpGet(host, port, "/health/live");
  ASSERT_FALSE(resLive) << "connection failed";
}

TEST(HealthServer, InvalidPort) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  const std::string host = "127.0.0.1";
  const int64_t port = -1;
  cta::runtime::HealthServer hs(dl, host, port, []() { return true; }, []() { return true; }, 200);
  hs.start();
  // At this point the server actually should have failed to start, but it should not crash anything
  // It should simply time out on trying the readiness/liveness endpoints
  ASSERT_FALSE(hs.isRunning());

  auto resReady = httpGet(host, port, "/health/ready");
  ASSERT_FALSE(resReady) << "connection failed";
  auto resLive = httpGet(host, port, "/health/live");
  ASSERT_FALSE(resLive) << "connection failed";
}

TEST(HealthServer, NoTwoServersOnSamePort) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  const std::string host = "127.0.0.1";
  const int64_t port = getFreePort();
  cta::runtime::HealthServer hs(dl, host, port, []() { return true; }, []() { return true; }, 200);
  hs.start();

  cta::runtime::HealthServer hs2(dl, host, port, []() { return true; }, []() { return true; }, 200);
  hs2.start();
  ASSERT_FALSE(hs2.isRunning());
}

TEST(HealthServer, StopsCorrectly) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  const std::string host = "127.0.0.1";
  const int64_t port = getFreePort();
  cta::runtime::HealthServer hs(dl, host, port, []() { return true; }, []() { return true; });
  hs.start();
  hs.stop();
  ASSERT_FALSE(hs.isRunning());
}

TEST(HealthServer, CanStartUnixDomainSocket) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  const std::string socketPath = "/tmp/health_test.sock";
  ::unlink(socketPath.c_str());
  cta::runtime::HealthServer hs(dl, socketPath, 80, []() { return true; }, []() { return true; });
  hs.start();

  assertUDSReady(socketPath);
}

TEST(HealthServer, CanStartUnixDomainSocketWithNon80Port) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  const std::string socketPath = "/tmp/health_test.sock";
  ::unlink(socketPath.c_str());
  cta::runtime::HealthServer hs(dl, socketPath, 8080, []() { return true; }, []() { return true; });
  hs.start();

  assertUDSReady(socketPath);
}

TEST(HealthServer, CanStartUnixDomainSocketOnSamePort) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  const std::string socketPath1 = "/tmp/health_test1.sock";
  ::unlink(socketPath1.c_str());
  cta::runtime::HealthServer hs1(dl, socketPath1, 80, []() { return true; }, []() { return true; });
  hs1.start();

  const std::string socketPath2 = "/tmp/health_test2.sock";
  ::unlink(socketPath2.c_str());
  cta::runtime::HealthServer hs2(dl, socketPath2, 80, []() { return true; }, []() { return true; });
  hs2.start();

  assertUDSReady(socketPath1);

  assertUDSReady(socketPath2);
}

}  // namespace unitTests

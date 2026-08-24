/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SignalReactor.hpp"

#include "SignalReactorBuilder.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/TimeOut.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/LogContext.hpp"
#include "common/utils/utils.hpp"

#include <chrono>
#include <cstdlib>
#include <functional>
#include <gtest/gtest.h>
#include <signal.h>
#include <stdexcept>
#include <thread>
#include <unistd.h>

namespace unitTests {

struct SignalReactorTestAccess {
  static std::jthread::native_handle_type nativeHandle(cta::runtime::SignalReactor& r) {
    return r.m_thread.native_handle();
  }
};

TEST(SignalReactor, HandlesSingleSignalCorrectly) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  std::atomic<int> calledHup {0}, calledTerm {0}, calledUsr1 {0};

  auto signalReactor = cta::runtime::SignalReactorBuilder()
                         .addSignalFunction(SIGHUP, [&]() { calledHup++; })
                         .addSignalFunction(SIGTERM, [&]() { calledTerm++; })
                         .addSignalFunction(SIGUSR1, [&]() { calledUsr1++; })
                         .withTimeoutMsecs(10)  // Check often
                         .build(dl);

  signalReactor.start();

  auto th = SignalReactorTestAccess::nativeHandle(signalReactor);
  ASSERT_EQ(0, ::pthread_kill(th, SIGUSR1));

  cta::utils::waitForCondition([&]() { return calledUsr1 >= 1; }, 1000, 10);
  EXPECT_EQ(1, calledUsr1);
  EXPECT_EQ(0, calledHup);
  EXPECT_EQ(0, calledTerm);

  signalReactor.stop();
}

TEST(SignalReactor, IgnoresSignalWithoutFunctionEvenIfInSigset) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  std::atomic<int> called {0};

  auto signalReactor = cta::runtime::SignalReactorBuilder()
                         .addSignalFunction(SIGUSR1, [&]() { called++; })
                         .addSignalFunction(SIGHUP, []() {})
                         .addSignalFunction(SIGTERM, []() {})
                         .withTimeoutMsecs(10)  // Check often
                         .build(dl);

  signalReactor.start();

  auto th = SignalReactorTestAccess::nativeHandle(signalReactor);
  ASSERT_EQ(0, ::pthread_kill(th, SIGHUP));

  // Give it a moment; should still remain 0.
  EXPECT_THROW(cta::utils::waitForCondition([&]() { return called != 0; }, 200, 10), cta::exception::TimeOut);
  EXPECT_EQ(0, called);

  signalReactor.stop();
}

TEST(SignalReactor, HandlesMultipleSignals) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  std::atomic<int> called {0};

  auto signalReactor = cta::runtime::SignalReactorBuilder()
                         .addSignalFunction(SIGUSR1, [&]() { called++; })
                         .addSignalFunction(SIGHUP, []() {})
                         .addSignalFunction(SIGTERM, []() {})
                         .withTimeoutMsecs(10)  // Check often
                         .build(dl);

  signalReactor.start();

  auto th = SignalReactorTestAccess::nativeHandle(signalReactor);

  constexpr int kN = 3;
  for (int i = 0; i < kN; ++i) {
    ASSERT_EQ(0, ::pthread_kill(th, SIGUSR1));
    // Give it a little bit of time between signal sending
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  // The signal reactor sleeps in between checking, so we need to give it sufficient time
  cta::utils::waitForCondition([&]() { return called >= kN; }, 500, 10);
  EXPECT_EQ(kN, called);

  signalReactor.stop();
}

TEST(SignalReactor, ContinuesAfterSignalCallbackThrows) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  std::atomic<int> calls {0};
  auto signalReactor = cta::runtime::SignalReactorBuilder()
                         .addSignalFunction(SIGUSR1,
                                            [&]() {
                                              calls++;
                                              if (calls == 1) {
                                                throw std::runtime_error("callback failure");
                                              }
                                            })
                         .withTimeoutMsecs(10)
                         .build(dl);

  signalReactor.start();
  auto th = SignalReactorTestAccess::nativeHandle(signalReactor);
  ASSERT_EQ(0, ::pthread_kill(th, SIGUSR1));
  cta::utils::waitForCondition([&]() { return calls >= 1; }, 500, 10);

  ASSERT_EQ(0, ::pthread_kill(th, SIGUSR1));
  cta::utils::waitForCondition([&]() { return calls >= 2; }, 500, 10);
  EXPECT_EQ(2, calls);
}

TEST(SignalReactor, CannotBeStartedMoreThanOnce) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  auto signalReactor =
    cta::runtime::SignalReactorBuilder().addSignalFunction(SIGUSR2, []() {}).withTimeoutMsecs(10).build(dl);
  signalReactor.start();
  signalReactor.stop();

  EXPECT_THROW(signalReactor.start(), cta::exception::Exception);
}

TEST(SignalReactor, StopWakesImmediatelyWithoutCallingSignalFunction) {
  cta::log::DummyLogger dl("dummy", "unitTest");

  std::atomic<int> calls {0};
  auto signalReactor = cta::runtime::SignalReactorBuilder()
                         .addSignalFunction(SIGUSR2, [&]() { calls++; })
                         .withTimeoutMsecs(5000)
                         .build(dl);
  signalReactor.start();

  const auto start = std::chrono::steady_clock::now();
  signalReactor.stop();
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(elapsed, std::chrono::seconds(2));
  EXPECT_EQ(0, calls);
}

TEST(SignalReactor, LeavesUnregisteredSignalsAlone) {
  ASSERT_EXIT(
    {
      cta::log::DummyLogger logger("dummy", "unitTest");
      // Change the default action of SIGUSR2 to exit with success
      ::signal(SIGUSR2, [](int) { ::_exit(EXIT_SUCCESS); });

      // Don't register anything for SIGUSR2, just to check we are not (accidentally) blocking
      // every single signal
      auto reactor = cta::runtime::SignalReactorBuilder().addSignalFunction(SIGUSR1, []() {}).build(logger);
      reactor.start();

      // This should now quit with EXIT_SUCCESS
      ::raise(SIGUSR2);
      ::_exit(EXIT_FAILURE);
    },
    ::testing::ExitedWithCode(EXIT_SUCCESS),
    "");
}

TEST(SignalReactor, RejectsInvalidAndUnhandleableSignals) {
  cta::runtime::SignalReactorBuilder builder;

  EXPECT_THROW(builder.addSignalFunction(0, []() {}), cta::exception::Exception);
  EXPECT_THROW(builder.addSignalFunction(NSIG, []() {}), cta::exception::Exception);
  EXPECT_THROW(builder.addSignalFunction(SIGKILL, []() {}), cta::exception::Exception);
  EXPECT_THROW(builder.addSignalFunction(SIGSTOP, []() {}), cta::exception::Exception);
  EXPECT_THROW(builder.addSignalFunction(SIGUSR1, {}), cta::exception::Exception);
}

}  // namespace unitTests

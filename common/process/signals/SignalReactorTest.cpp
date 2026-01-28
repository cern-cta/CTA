/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SignalReactor.hpp"

#include "SignalReactorBuilder.hpp"
#include "common/exception/TimeOut.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/LogContext.hpp"
#include "common/utils/utils.hpp"

#include <chrono>
#include <functional>
#include <gtest/gtest.h>
#include <thread>

namespace unitTests {

struct SignalReactorTestAccess {
  static std::jthread::native_handle_type nativeHandle(cta::process::SignalReactor& r) {
    return r.m_thread.native_handle();
  }
};

TEST(SignalReactor, HandlesSingleSignalCorrectly) {
  cta::log::DummyLogger dl("dummy", "unitTest");
  cta::log::LogContext lc(dl);

  std::atomic<int> calledHup {0}, calledTerm {0}, calledUsr1 {0};

  auto signalReactor = cta::process::SignalReactorBuilder(lc)
                         .addSignalFunction(SIGHUP, [&]() { calledHup++; })
                         .addSignalFunction(SIGTERM, [&]() { calledTerm++; })
                         .addSignalFunction(SIGUSR1, [&]() { calledUsr1++; })
                         .build();

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
  cta::log::LogContext lc(dl);

  std::atomic<int> called {0};

  auto signalReactor = cta::process::SignalReactorBuilder(lc)
                         .addSignalFunction(SIGUSR1, [&]() { called++; })
                         .addSignalFunction(SIGHUP, []() {})
                         .addSignalFunction(SIGTERM, []() {})
                         .build();

  signalReactor.start();

  auto th = SignalReactorTestAccess::nativeHandle(signalReactor);
  ASSERT_EQ(0, ::pthread_kill(th, SIGHUP));

  // Give it a moment; should still remain 0.
  EXPECT_THROW(cta::utils::waitForCondition([&]() { return called != 0; }, 300, 10), cta::exception::TimeOut);
  EXPECT_EQ(0, called);

  signalReactor.stop();
}

TEST(SignalReactor, HandlesMultipleSignals) {
  cta::log::DummyLogger dl("dummy", "unitTest");
  cta::log::LogContext lc(dl);

  std::atomic<int> called {0};

  auto signalReactor = cta::process::SignalReactorBuilder(lc)
                         .addSignalFunction(SIGUSR1, [&]() { called++; })
                         .addSignalFunction(SIGHUP, []() {})
                         .addSignalFunction(SIGTERM, []() {})
                         .build();

  signalReactor.start();

  auto th = SignalReactorTestAccess::nativeHandle(signalReactor);

  constexpr int kN = 3;
  for (int i = 0; i < kN; ++i) {
    ASSERT_EQ(0, ::pthread_kill(th, SIGUSR1));
    // Give it a little bit of time between signal sending
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  // The signal reactor sleeps in between checking, so we need to give it sufficient time
  // 3 signals, 1 second sleep between -> wait 4 sec
  cta::utils::waitForCondition([&]() { return called >= kN; }, 4000, 10);
  EXPECT_EQ(kN, called);

  signalReactor.stop();
}

TEST(SignalReactor, HandlesUnregisteredSignals) {
  cta::log::DummyLogger dl("dummy", "unitTest");
  cta::log::LogContext lc(dl);

  std::atomic<int> called {0};

  auto signalReactor = cta::process::SignalReactorBuilder(lc)
                         .addSignalFunction(SIGUSR1, [&]() { called++; })
                         .addSignalFunction(SIGHUP, []() {})
                         .addSignalFunction(SIGTERM, []() {})
                         .build();

  signalReactor.start();

  auto th = SignalReactorTestAccess::nativeHandle(signalReactor);

  // Send an unknown signal
  ASSERT_EQ(0, ::pthread_kill(th, SIGUSR2));
  // Give it a little bit
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Send a registered signal
  ASSERT_EQ(0, ::pthread_kill(th, SIGUSR1));

  cta::utils::waitForCondition([&]() { return called >= 1; }, 1000, 10);
  EXPECT_EQ(1, called);

  signalReactor.stop();
}

}  // namespace unitTests

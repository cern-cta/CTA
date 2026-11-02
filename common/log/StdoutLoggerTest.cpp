/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "StdoutLogger.hpp"

#include <gtest/gtest.h>
#include <iostream>
#include <json-c/json.h>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace cta::log;

namespace unitTests {

/// Redirects std::cout to a string buffer for the lifetime of the object
class CoutCapture {
public:
  CoutCapture() : m_oldBuf(std::cout.rdbuf(m_buffer.rdbuf())) {}

  ~CoutCapture() { std::cout.rdbuf(m_oldBuf); }

  std::string str() const { return m_buffer.str(); }

private:
  std::ostringstream m_buffer;
  std::streambuf* m_oldBuf;
};

std::vector<std::string> splitLines(const std::string& text) {
  std::vector<std::string> lines;
  std::istringstream iss(text);
  for (std::string line; std::getline(iss, line);) {
    lines.push_back(line);
  }
  return lines;
}

bool isValidJson(const std::string& line) {
  json_object* obj = json_tokener_parse(line.c_str());
  if (obj == nullptr) {
    return false;
  }
  json_object_put(obj);
  return true;
}

TEST(cta_log_StdoutLogger, basicTest) {
  std::string jat = "Just a test";
  std::string output;
  {
    CoutCapture capture;
    StdoutLogger sl("dummy", "cta_log_StdoutLogger");
    sl(INFO, jat);
    output = capture.str();
  }
  ASSERT_NE(std::string::npos, output.find(jat));
  ASSERT_EQ('\n', output.back());
  ASSERT_EQ(1, splitLines(output).size());
}

TEST(cta_log_StdoutLogger, jsonFormat) {
  std::string output;
  {
    CoutCapture capture;
    StdoutLogger sl("dummy", "cta_log_StdoutLogger");
    sl.setLogFormat("json");
    sl(INFO, "Just a test", {Param("key", "value")});
    output = capture.str();
  }
  const auto lines = splitLines(output);
  ASSERT_EQ(1, lines.size());
  ASSERT_TRUE(isValidJson(lines.front())) << lines.front();
}

TEST(cta_log_StdoutLogger, concurrentJsonLinesDoNotInterleave) {
  // 8 threads emitting 500 messages each should result in
  // a probability of collision close to 1
  constexpr int nbThreads = 8;
  constexpr int nbMsgsPerThread = 500;
  std::string output;
  {
    CoutCapture capture;
    StdoutLogger sl("dummy", "cta_log_StdoutLogger");
    sl.setLogFormat("json");
    std::vector<std::thread> threads;
    for (int t = 0; t < nbThreads; t++) {
      threads.emplace_back([&sl, t] {
        for (int i = 0; i < nbMsgsPerThread; i++) {
          sl(INFO, "Concurrent message", {Param("thread", t), Param("index", i)});
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    output = capture.str();
  }
  const auto lines = splitLines(output);
  ASSERT_EQ(nbThreads * nbMsgsPerThread, lines.size());
  for (const auto& line : lines) {
    ASSERT_TRUE(isValidJson(line)) << line;
  }
}

}  // namespace unitTests

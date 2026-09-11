/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <initializer_list>
#include <string>
#include <vector>

namespace unitTests {

// Since we want to initialise this with an initialiser list, we need a struct to own the argument-list storage.
struct Argv {
  int count = 0;
  std::vector<std::string> storage;  // owns the strings
  std::vector<char*> argv;           // points to the storage

  Argv(std::initializer_list<std::string> args) : storage(args) {
    count = static_cast<int>(storage.size());
    argv.reserve(storage.size() + 1);

    for (auto& s : storage) {
      argv.push_back(s.data());
    }
    argv.push_back(nullptr);  // argv must be null-terminated
  }

  char** data() { return argv.data(); }
};

}  // namespace unitTests

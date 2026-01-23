/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>
#include <vector>

namespace unitTests {

struct Argcv {
  template<typename... Args>
  explicit Argcv(Args&&... args)
      : argv_vec {std::make_unique<char*[]>(sizeof...(args) + 1)},
        argv_data {std::string {std::forward<Args>(args)}...},
        argc {sizeof...(args)},
        argv {argv_vec.get()} {
    for (int i = 0; i < argc; i++) {
      argv[i] = argv_data[i].data();
    }
    argv[argc] = nullptr;
  }

private:
  std::unique_ptr<char*[]> argv_vec;
  std::vector<std::string> argv_data;

public:
  int argc;
  char** argv;
};

}  // namespace unitTests

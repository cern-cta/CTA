/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <iostream>
#include <map>

namespace eos::client {

struct Namespace {
  Namespace(const std::string& ep, const std::string& tk) : endpoint(ep), token(tk) {
    std::cerr << "Created namespace endpoint " << endpoint << " with token " << token << std::endl;
  }

  std::string endpoint;
  std::string token;
};

using NamespaceMap_t = std::map<std::string, Namespace>;

}  // namespace eos::client

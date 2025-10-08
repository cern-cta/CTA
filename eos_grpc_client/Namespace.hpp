/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <map>
#include <iostream>

namespace eos::client {

struct Namespace
{
  Namespace(const std::string &ep, const std::string &tk) :
    endpoint(ep), token(tk) {
std::cerr << "Created namespace endpoint " << endpoint << " with token " << token << std::endl;
  }

  std::string endpoint;
  std::string token;
};

typedef std::map<std::string, Namespace> NamespaceMap_t;

} // namespace eos::client

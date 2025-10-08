/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <list>
#include <map>
#include <stdint.h>
#include <string>

namespace cta::common::dataStructures {

/**
 * This struct holds the information about who issued the CTA command and from which host
 */
struct SecurityIdentity {

  SecurityIdentity();

  SecurityIdentity(const std::string& username, const std::string& host);

  SecurityIdentity(const std::string& username, const std::string& host, const std::string& clientHost, const std::string& auth);

  bool operator==(const SecurityIdentity &rhs) const;

  bool operator!=(const SecurityIdentity &rhs) const;

  bool operator<(const SecurityIdentity &rhs) const;

  // Security protocol used to connect
  enum class Protocol { NONE, SSS, KRB5, GRPC_TOKEN, OTHER };
  const std::map<std::string, Protocol> m_authProtoMap = {
    { "sss",        Protocol::SSS  },
    { "krb5",       Protocol::KRB5 },
    { "grpc_token", Protocol::GRPC_TOKEN }
  };

  std::string username;
  std::string host;
  std::string clientHost;
  Protocol authProtocol;
}; // struct SecurityIdentity

std::ostream &operator<<(std::ostream &os, const SecurityIdentity &obj);

} // namespace cta::common::dataStructures

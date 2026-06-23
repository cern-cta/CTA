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
  // Security protocol used to connect
  enum class Protocol { NONE, SSS, KRB5, JWT, MTLS };

  SecurityIdentity() = default;

  SecurityIdentity(const std::string& username_, const std::string& host_) : username(username_), host(host_) {}

  SecurityIdentity(const std::string& username_, const std::string& host_, const std::string& clientHost_)
      : username(username_),
        host(host_),
        clientHost(clientHost_) {}

  SecurityIdentity(const std::string& username_,
                   const std::string& host_,
                   const std::string& clientHost_,
                   Protocol authProtocol_)
      : username(username_),
        host(host_),
        clientHost(clientHost_),
        authProtocol(authProtocol_) {}

  bool operator==(const SecurityIdentity& rhs) const;

  bool operator<(const SecurityIdentity& rhs) const;

  std::string username;
  std::string host;
  std::string clientHost;
  Protocol authProtocol = Protocol::NONE;
};  // struct SecurityIdentity

std::ostream& operator<<(std::ostream& os, const SecurityIdentity& obj);

}  // namespace cta::common::dataStructures

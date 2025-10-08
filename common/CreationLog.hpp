/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

#include "common/dataStructures/OwnerIdentity.hpp"

namespace cta {

/**
 * Class containing the security information necessary to authorise a user
 * submitting a requests from a specific host.
 */
struct CreationLog {

  /**
   * Constructor.
   */
  CreationLog();

  /**
   * Constructor.
   */
  CreationLog(const common::dataStructures::OwnerIdentity &user, const std::string &host,
    const time_t time,  const std::string & comment = "");

  /**
   * The identity of the creator.
   */
  common::dataStructures::OwnerIdentity user;

  /**
   * The network name of the host from which they are submitting a request.
   */
  std::string host;

  /**
   * The time of creation
   */
  time_t time;

  /**
   * The comment at creation time
   */
  std::string comment;

}; // struct CreationLog

} // namespace cta

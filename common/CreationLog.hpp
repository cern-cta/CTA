/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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

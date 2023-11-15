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

#include "rdbms/wrapper/ConnWrapper.hpp"

#include <memory>

namespace cta::rdbms::wrapper {

/**
 * Abstract class that specifies the interface of a factory of Conn objects.
 */
class ConnFactory {
public:

  /**
   * Destructor.
   */
  virtual ~ConnFactory() = 0;

  /**
   * Returns a newly created database connection.
   *
   * @return A newly created database connection.
   */
  virtual std::unique_ptr<ConnWrapper> create() = 0;

}; // class ConnFactory

} // namespace cta::rdbms::wrapper

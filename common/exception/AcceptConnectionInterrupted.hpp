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

#include "common/exception/Exception.hpp"

#include <sys/types.h>

namespace cta {
namespace exception {

/**
 * castor::io::acceptConnection() was interrupted.
 */
class AcceptConnectionInterrupted : public cta::exception::Exception {
public:
  /**
   * Constructor.
   *
   * @param remainingTime The number of remaining seconds when
   * castor::io::acceptConnection() was interrupted.
   */
  AcceptConnectionInterrupted(const time_t remainingTime);

  /**
   * Returns the number of remaining seconds when
   * castor::io::acceptConnection() was interrupted.
   */
  time_t remainingTime() const;

private:
  /**
   * The number of remaining seconds when
   * castor::io::acceptConnection() was interrupted.
   */
  const time_t m_remainingTime;

};  // class AcceptConnectionInterrupted

}  // end of namespace exception
}  // end of namespace cta

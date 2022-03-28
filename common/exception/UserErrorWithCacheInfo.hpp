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

#include "common/exception/UserError.hpp"

namespace cta {
namespace exception {

/**
 * A user error together with information about how the respective cached
 * value was obtained.
 */
class UserErrorWithCacheInfo : public exception::UserError {
public:
  std::string cacheInfo;

  /**
   * Constructor.
   *
   * @param cInfo Information about how the respective cached value was
   * obtained.
   * @param context optional context string added to the message
   * at initialisation time.
   * @param embedBacktrace whether to embed a backtrace of where the
   * exception was throw in the message
   */
  UserErrorWithCacheInfo(const std::string &cInfo, const std::string &context = "", const bool embedBacktrace = true):
    UserError(context, embedBacktrace),
    cacheInfo(cInfo) {
  }
}; // class UserErrorWithTimeBasedCacheInfo

} // namespace exception
} // namespace cta

/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/UserError.hpp"

namespace cta::exception {

/**
 * A user error together with information about how the respective cached value was obtained
 */
class UserErrorWithCacheInfo : public exception::UserError {
public:
  /**
   * Constructor
   *
   * @param cInfo Information about how the respective cached value was obtained
   * @param context optional context string added to the message at initialisation time
   * @param embedBacktrace whether to embed a backtrace of where the exception was thrown in the message
   */
  explicit UserErrorWithCacheInfo(const std::string& cInfo,
                                  const std::string& context = "",
                                  const bool embedBacktrace = true)
      : UserError(context, embedBacktrace),
        cacheInfo(cInfo) {}

  std::string cacheInfo;
};

}  // namespace cta::exception

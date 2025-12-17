/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

// Include Files
#include "common/exception/Exception.hpp"

namespace cta::exception {

/**
 * Invalid argument exception
 */
class NoEntry : public cta::exception::Exception {
public:
  /**
       * default constructor
       */
  NoEntry()
      :  // No backtrace for this exception
        cta::exception::Exception("", false) {}
};

}  // namespace cta::exception

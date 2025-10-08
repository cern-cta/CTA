/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/exception/Exception.hpp"

namespace cta::exception {

    /**
     * Mismatch.
     */
    class Mismatch : public cta::exception::Exception {

    public:

      /**
       * Constructor
       */
      Mismatch();

    }; // class Mismatch

} // namespace cta::exception

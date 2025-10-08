/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "Exception.hpp"

namespace cta::exception {

    /**
     * request failed.
     */
    class RequestFailed : public cta::exception::Exception {

    public:

      /**
       * Constructor
       */
      RequestFailed();

    }; // class RequestFailed

} // namespace cta::exception

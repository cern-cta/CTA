/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/exception/Exception.hpp"

namespace cta::exception {

    /**
     * Failed to dismount volume.
     */
    class DismountFailed : public cta::exception::Exception {

    public:

      /**
       * Constructor
       */
      DismountFailed();

    }; // class DismountFailed

} // namespace cta::exception

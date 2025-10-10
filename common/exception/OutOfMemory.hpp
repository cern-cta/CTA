/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

// Include Files
#include "common/exception/Exception.hpp"

namespace cta {

  namespace exception {

    /**
     * Invalid argument exception
     */
    class OutOfMemory : public cta::exception::Exception {

    public:

      /**
       * default constructor
       */
      OutOfMemory();

    };

  } // end of namespace exception

} // end of namespace cta


/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"

namespace cta::exception {

/**
 * This exception should be used by JSONObject inherited classes
 * to inform about a problem linked to the creation or json serialization of an object
 */
class JSONObjectException : public Exception {
  using Exception::Exception;
};

}  // namespace cta::exception

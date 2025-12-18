/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"

namespace cta::exception {

class NullPtrException : public cta::exception::Exception {
public:
  explicit NullPtrException(const std::string& context = "") : cta::exception::Exception(context) {}
};

}  // namespace cta::exception

/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/exception/Exception.hpp"

namespace castor::tape::tapeserver::daemon {

/**
 * Used to signal an error has happened during the migration process
 */
class ErrorFlag : public cta::exception::Exception {
public:
  ErrorFlag() : cta::exception::Exception("Internal exception, should not be seen") {}
  virtual ~ErrorFlag() = default;
};

} // namespace castor::tape::tapeserver::daemon

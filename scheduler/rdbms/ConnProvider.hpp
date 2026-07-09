/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/Conn.hpp"

namespace cta {

class ConnProvider {
public:
  virtual ~ConnProvider() = default;

  virtual cta::rdbms::Conn getConn() = 0;
};

}  // namespace cta

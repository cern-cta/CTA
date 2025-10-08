/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "DiskReporter.hpp"

namespace cta::disk {

class NullReporter: public DiskReporter {
public:
  NullReporter() { m_promise.set_value(); };
  void asyncReport() override {};
};

} // namespace cta::disk

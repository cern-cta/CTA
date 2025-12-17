/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <future>

namespace cta::disk {

class DiskReporter {
public:
  virtual void asyncReport() = 0;

  virtual void waitReport() { m_promise.get_future().get(); }

  virtual ~DiskReporter() = default;

protected:
  std::promise<void> m_promise;
};

}  // namespace cta::disk

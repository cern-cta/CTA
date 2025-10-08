/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <memory>

namespace cta::frontend::grpc::request {

class IHandler {

public:
  virtual ~IHandler() = default;
  /*
   * Provides way for an optional initialisation
   * and avoids throwing exceptions in the constructor
   */
  virtual void init() = 0; // can throw
  virtual bool next(const bool bOk) = 0; // can throw
};

using Tag = IHandler*;

} // namespace cta::frontend::grpc::request

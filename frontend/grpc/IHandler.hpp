/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::frontend::grpc::client {

/// @brief Handler of an asynchronous client request, driven by the completion queue of the AsyncClient. Its address
/// is the tag of the operations it starts on the queue.
class IHandler {
public:
  virtual ~IHandler() = default;
  /// Provides way for an optional initialisation
  /// and avoids throwing exceptions in the constructor
  ///
  /// can throw
  virtual void init() = 0;
  /// can throw
  virtual bool next(const bool bOk) = 0;
};

using Tag = IHandler*;

}  // namespace cta::frontend::grpc::client

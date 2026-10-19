/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::admin_api::rpc::kerberos {

/**
 * @brief Handler of an asynchronous Kerberos negotiation request, driven by the completion queue of the
 * NegotiationService. Its address is the tag of the operations it starts on the queue.
 */
class IHandler {
public:
  virtual ~IHandler() = default;
  /*
   * Provides way for an optional initialisation
   * and avoids throwing exceptions in the constructor
   */
  virtual void init() = 0;                // can throw
  virtual bool next(const bool bOk) = 0;  // can throw
};

using Tag = IHandler*;

}  // namespace cta::admin_api::rpc::kerberos

/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

#include "cta_admin.pb.h"
#include "cta_frontend.pb.h"

namespace cta::admin_api::commands::stream {

/// @brief Source of the data items streamed back to cta-admin as the result of one admin command
struct CtaAdminResponseStream {
  /// @param headerType    The header to send to the client before the items
  /// @param instanceName  CTA instance name, set on each streamed item
  CtaAdminResponseStream(cta::admin::HeaderType headerType, std::string instanceName)
      : m_headerType(headerType),
        m_instanceName(std::move(instanceName)) {}

  virtual ~CtaAdminResponseStream() = default;

  /// @brief The header to send to the client before the items
  cta::admin::HeaderType headerType() const { return m_headerType; }

  /// @brief True when next() has no more items to return
  virtual bool isDone() = 0;

  /// @brief Returns the next item. Must not be called once isDone() is true.
  virtual cta::xrd::Data next() = 0;

protected:
  cta::admin::HeaderType m_headerType;
  std::string m_instanceName;
};

}  // namespace cta::admin_api::commands::stream

/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "commands/stream/CtaAdminResponseStream.hpp"
#include "commands/stream/converters/StreamConverters.hpp"

#include <cstddef>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "cta_frontend.pb.h"

namespace cta::admin_api::commands::stream {

/**
 * @brief Streams the items of a vector already loaded from the catalogue/scheduler, one cta::xrd::Data each
 *
 * @tparam Item  Type of the items, converted to their protobuf representation by the matching toProto() overload
 * (see `converters/StreamConverters.hpp`)
 */
template<class Item>
class VectorResponseStream final : public CtaAdminResponseStream {
public:
  VectorResponseStream(cta::admin::HeaderType headerType, std::string instanceName, std::vector<Item> items)
      : CtaAdminResponseStream(headerType, std::move(instanceName)),
        m_items(std::move(items)) {}

  bool isDone() override { return m_idx >= m_items.size(); }

  cta::xrd::Data next() override {
    if (isDone()) {
      throw std::runtime_error("Stream is exhausted");
    }
    cta::xrd::Data data;
    toProto(m_items[m_idx++], data, m_instanceName);
    return data;
  }

private:
  std::vector<Item> m_items;  //<! The items which will be part of the response's body
  std::size_t m_idx = 0;      //<! Current index (items streamed so far)
};

}  // namespace cta::admin_api::commands::stream

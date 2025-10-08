/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RetrieveQueueAlgorithms.hpp"

namespace cta::objectstore {

template<>
const std::string ContainerTraits<RetrieveQueue,RetrieveQueueFailed>::c_containerTypeName = "RetrieveQueueFailed";

} // namespace cta::objectstore

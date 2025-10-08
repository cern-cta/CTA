/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "RetrieveQueueAlgorithms.hpp"

namespace cta::objectstore {

template<>
const std::string ContainerTraits<RetrieveQueue,RetrieveQueueToReportToRepackForFailure>::c_containerTypeName = "RetrieveQueueToReportToRepackForFailure";

} // namespace cta::objectstore

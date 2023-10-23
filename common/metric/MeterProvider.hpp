/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022-2023 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "common/metric/MeterProviderBackend.hpp"
#include "common/metric/MeterProviderBackendOTLP.hpp"
#include "common/metric/MeterCounter.hpp"
#include "common/metric/MeterHistogram.hpp"

namespace cta::metric {

class MeterProvider {
public:
  MeterProvider() = delete;
  static void setMeterBackend(std::unique_ptr<MeterProviderBackend> backend);
  static std::unique_ptr<MeterCounter> getMeterCounter(const std::string & topic, const std::string & counterName);
  static std::unique_ptr<MeterHistogram> getMeterHistogram(const std::string & topic, const std::string & histogramName);
private:
  //TODO: Change for no-op implementation
  static inline std::unique_ptr<MeterProviderBackend> s_backend = std::make_unique<MeterProviderBackendOTLP>();
};

}

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

#include <fstream>
#include "common/metric/MeterProviderBackend.hpp"
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h"

#undef USE_OTLP_EXPORTER
//#define USE_OTLP_EXPORTER

namespace cta::metric {

class MeterProviderBackendOTLP : public MeterProviderBackend {
public:
  static std::unique_ptr<MeterProviderBackendOTLP> CreateMeterProviderBackendOTLP_NoOp();
  static std::unique_ptr<MeterProviderBackendOTLP> CreateMeterProviderBackendOTLP_Cout();
  static std::unique_ptr<MeterProviderBackendOTLP> CreateMeterProviderBackendOTLP_File(const std::string & filePath);
#ifdef USE_OTLP_EXPORTER
  static std::unique_ptr<MeterProviderBackendOTLP> CreateMeterProviderBackendOTLP_OTLP_HTTP(const std::string & hostName, const std::string & port);
  static std::unique_ptr<MeterProviderBackendOTLP> CreateMeterProviderBackendOTLP_OTLP_gRPC(const std::string & hostName, const std::string & port);
#endif
  static std::unique_ptr<MeterProviderBackendOTLP> CreateMeterProviderBackendOTLP_Prometheus(const std::string & hostName, const std::string & port);
  std::unique_ptr<MeterCounter> getMeterCounter(const std::string & topic, const std::string & counterName) override;
  std::unique_ptr<MeterHistogram> getMeterHistogram(const std::string & topic, const std::string & histogramName) override;
  ~MeterProviderBackendOTLP() noexcept;
private:
  MeterProviderBackendOTLP() {}
  std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> getReader_Cout();
  std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> getReader_File(const std::string & filePath);
#ifdef USE_OTLP_EXPORTER
  std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> getReader_OTLP_HTTP(const std::string & hostName, const std::string & port);
  std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> getReader_OTLP_gRPC(const std::string & hostName, const std::string & port);
#endif
  std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> getReader_Prometheus(const std::string & hostName, const std::string & port);
  void setReader(std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> reader);
  std::ofstream m_ofs;
};

}

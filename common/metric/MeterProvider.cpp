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

#include "common/metric/MeterProvider.hpp"
#include "common/metric/MeterProviderBackendOTLP.hpp"
#include "common/metric/MeterProviderBackendNoOp.hpp"

#define DEFAULT_METER_LOG_FILE "/var/log/cta/cta-meter.log"
#define DEFAULT_HOST "localhost"
#define DEFAULT_PORT "8080"

namespace cta::metric {

void MeterProvider::setMeterBackend(std::unique_ptr<MeterProviderBackend> backend) {
  s_backend = std::move(backend);
}

std::unique_ptr<MeterCounter> MeterProvider::getMeterCounter(const std::string & topic, const std::string & counterName){
  return MeterProvider::s_backend->getMeterCounter(topic, counterName);
}

std::unique_ptr<MeterHistogram> MeterProvider::getMeterHistogram(const std::string & topic, const std::string & histogramName){
  return MeterProvider::s_backend->getMeterHistogram(topic, histogramName);
}

void MeterProvider::shutdown() noexcept {
  return MeterProvider::s_backend.reset();
}

std::unique_ptr<MeterProviderBackend> MeterProvider::selectBackend() {
  // TODO: Use somehting like cta::dataStructures::toString()
  enum BackendOptions {
    NOOP,
    OTLP_NOOP,
    OTLP_COUT,
    OTLP_FILE,
#ifdef USE_OTLP_EXPORTER
    OTLP_GRPC,
    OTLP_HTTP,
#endif
    OTLP_PROMETHEUS,
  };
  static const std::map<std::string, BackendOptions> optionToBackendOption {
          {"NOOP", NOOP},
          {"OTLP_NOOP", OTLP_NOOP},
          {"OTLP_COUT", OTLP_COUT},
          {"OTLP_FILE", OTLP_FILE},
#ifdef USE_OTLP_EXPORTER
          {"OTLP_GRPC", OTLP_GRPC},
          {"OTLP_HTTP", OTLP_HTTP},
#endif
          {"OTLP_PROMETHEUS", OTLP_PROMETHEUS},
  };
  const char * option = std::getenv("OTLP_BACKEND");
  if (!option) {
    // No option selected
    return std::make_unique<MeterProviderBackendNoOp>();
  }
  if (optionToBackendOption.count(option) == 0) {
    //TODO: Log this failure
    return std::make_unique<MeterProviderBackendNoOp>();
  }
  BackendOptions backendOption = optionToBackendOption.at(option);
  switch (backendOption) {
  case OTLP_NOOP:
    return MeterProviderBackendOTLP::CreateMeterProviderBackendOTLP_NoOp();
  case OTLP_COUT:
    return MeterProviderBackendOTLP::CreateMeterProviderBackendOTLP_Cout();
  case OTLP_FILE:
    {
      const char *filePath = getenv("OTLP_FILE_PATH");
      std::string filePathStr = filePath ? filePath : DEFAULT_METER_LOG_FILE;
      return MeterProviderBackendOTLP::CreateMeterProviderBackendOTLP_File(filePathStr);
    }
  case OTLP_PROMETHEUS:
    {
      const char *host = getenv("OTLP_HOST");
      const char *port = getenv("OTLP_PORT");
      std::string hostStr = host ? host : DEFAULT_HOST;
      std::string portStr = port ? port : DEFAULT_PORT;
      return MeterProviderBackendOTLP::CreateMeterProviderBackendOTLP_Prometheus(hostStr, portStr);
    }
#ifdef USE_OTLP_EXPORTER

#endif
  default:
    return std::make_unique<MeterProviderBackendNoOp>();
  }
}

}

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

#include "common/metric/MeterProviderBackendOTLP.hpp"
#include "common/metric/MeterCounterOTLP.hpp"
#include "common/metric/MeterHistogramOTLP.hpp"

#include "opentelemetry/metrics/provider.h"
#include "opentelemetry/exporters/ostream/metric_exporter_factory.h"
#ifdef USE_OTLP_EXPORTER
  #include "opentelemetry/exporters/otlp/otlp_http_metric_exporter_factory.h"
  #include "opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_factory.h"
#endif
#include "opentelemetry/exporters/prometheus/exporter_factory.h"
#include "opentelemetry/exporters/prometheus/exporter_options.h"
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h"
#include "opentelemetry/sdk/metrics/meter_context_factory.h"
#include "opentelemetry/sdk/metrics/meter_provider_factory.h"
#include "opentelemetry/sdk/metrics/push_metric_exporter.h"
#include "opentelemetry/nostd/shared_ptr.h"

namespace cta::metric {

std::unique_ptr<MeterProviderBackendOTLP> MeterProviderBackendOTLP::CreateMeterProviderBackendOTLP_NoOp() {
  // By default, a No-Op meter provider will be returned by OTLP
  return std::unique_ptr<MeterProviderBackendOTLP>(new MeterProviderBackendOTLP());
}

std::unique_ptr<MeterProviderBackendOTLP> MeterProviderBackendOTLP::CreateMeterProviderBackendOTLP_Cout() {
  auto meterProvider = std::unique_ptr<MeterProviderBackendOTLP>(new MeterProviderBackendOTLP());
  meterProvider->setReader(meterProvider->getReader_Cout());
  return meterProvider;
}

std::unique_ptr<MeterProviderBackendOTLP> MeterProviderBackendOTLP::CreateMeterProviderBackendOTLP_File(const std::string & filePath) {
  auto meterProvider = std::unique_ptr<MeterProviderBackendOTLP>(new MeterProviderBackendOTLP());
  meterProvider->setReader(meterProvider->getReader_File(filePath));
  return meterProvider;
}

#ifdef USE_OTLP_EXPORTER
std::unique_ptr<MeterProviderBackendOTLP> MeterProviderBackendOTLP::CreateMeterProviderBackendOTLP_OTLP_HTTP(const std::string & hostName, const std::string & port) {
  auto meterProvider = std::unique_ptr<MeterProviderBackendOTLP>(new MeterProviderBackendOTLP());
  meterProvider->setReader(meterProvider->getReader_OTLP_HTTP(hostName, port));
  return meterProvider;
}

std::unique_ptr<MeterProviderBackendOTLP> MeterProviderBackendOTLP::CreateMeterProviderBackendOTLP_OTLP_gRPC(const std::string & hostName, const std::string & port) {
  auto meterProvider = std::unique_ptr<MeterProviderBackendOTLP>(new MeterProviderBackendOTLP());
  meterProvider->setReader(meterProvider->getReader_OTLP_gRPC(hostName, port));
  return meterProvider;
}
#endif

std::unique_ptr<MeterProviderBackendOTLP> MeterProviderBackendOTLP::CreateMeterProviderBackendOTLP_Prometheus(const std::string & hostName, const std::string & port) {
  auto meterProvider = std::unique_ptr<MeterProviderBackendOTLP>(new MeterProviderBackendOTLP());
  meterProvider->setReader(meterProvider->getReader_Prometheus(hostName, port));
  return meterProvider;
}

std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> MeterProviderBackendOTLP::getReader_Cout() {
  auto exporter = opentelemetry::exporter::metrics::OStreamMetricExporterFactory::Create();
  opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions options;
  options.export_interval_millis = std::chrono::milliseconds(1000);
  options.export_timeout_millis = std::chrono::milliseconds(500);
  return opentelemetry::sdk::metrics::PeriodicExportingMetricReaderFactory::Create(std::move(exporter), options);
}

std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> MeterProviderBackendOTLP::getReader_File(const std::string & filePath) {
  m_ofs = std::ofstream(filePath, std::ofstream::out | std::ofstream::app);
  auto exporter = opentelemetry::exporter::metrics::OStreamMetricExporterFactory::Create(m_ofs);
  opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions options;
  options.export_interval_millis = std::chrono::milliseconds(1000);
  options.export_timeout_millis = std::chrono::milliseconds(500);
  return opentelemetry::sdk::metrics::PeriodicExportingMetricReaderFactory::Create(std::move(exporter), options);
}

#ifdef USE_OTLP_EXPORTER
std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> MeterProviderBackendOTLP::getReader_OTLP_HTTP(const std::string & hostName, const std::string & port) {
  //TODO: For example, check: https://opentelemetry.io/docs/instrumentation/cpp/exporters/
}

std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> MeterProviderBackendOTLP::getReader_OTLP_gRPC(const std::string & hostName, const std::string & port) {
  //TODO: For example, check: https://opentelemetry.io/docs/instrumentation/cpp/exporters/
}
#endif

std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> MeterProviderBackendOTLP::getReader_Prometheus(const std::string & hostName, const std::string & port) {
  opentelemetry::exporter::metrics::PrometheusExporterOptions prometheusOptions;
  prometheusOptions.url = hostName + ":" + port;
  return opentelemetry::exporter::metrics::PrometheusExporterFactory::Create(prometheusOptions);
}

void MeterProviderBackendOTLP::setReader(std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> reader) {
  auto context = opentelemetry::sdk::metrics::MeterContextFactory::Create();
  context->AddMetricReader(std::move(reader));
  auto u_provider = opentelemetry::sdk::metrics::MeterProviderFactory::Create(std::move(context));
  std::shared_ptr<opentelemetry::metrics::MeterProvider> provider(std::move(u_provider));
  opentelemetry::metrics::Provider::SetMeterProvider(provider);
}

std::unique_ptr<MeterCounter> MeterProviderBackendOTLP::getMeterCounter(const std::string & topic, const std::string & counterName) {
  auto meter = std::make_unique<MeterCounterOTLP>();
  auto provider = opentelemetry::metrics::Provider::GetMeterProvider();
  meter->_meter = provider->GetMeter(topic);
  meter->_counter = meter->_meter->CreateUInt64Counter(counterName);
  return meter;
}
std::unique_ptr<MeterHistogram> MeterProviderBackendOTLP::getMeterHistogram(const std::string & topic, const std::string & histogramName) {
  auto meter = std::make_unique<MeterHistogramOTLP>();
  auto provider = opentelemetry::metrics::Provider::GetMeterProvider();
  meter->_meter = provider->GetMeter(topic);
  meter->_histogram = meter->_meter->CreateDoubleHistogram(histogramName);
  return meter;
}

}

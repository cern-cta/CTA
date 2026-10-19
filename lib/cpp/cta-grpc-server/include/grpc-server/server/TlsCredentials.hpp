/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "grpc-server/config/TlsConfig.hpp"

#include <grpcpp/security/server_credentials.h>
#include <memory>

namespace cta::grpc_server {

/**
 * @brief Create the TLS server credentials for a gRPC server, loading the key, certificate and optional chain
 * from the files given in the TLS configuration.
 *
 * The server key/certificate are watched so that they are reloaded when the files change, and the same for the
 * root certificates.
 *
 * @param tls The TLS configuration (paths to the key, certificate and optional CA chain)
 * @param requireClientCertificate If true, clients must present a certificate which is verified (mTLS).
 *                                 If false, no client certificate is requested (plain server-side TLS).
 * @param lc Log context used to report the loaded configuration
 * @return The server credentials to pass to ServerBuilder::AddListeningPort()
 */
std::shared_ptr<::grpc::ServerCredentials>
createTlsServerCredentials(const TlsConfig& tls, bool requireClientCertificate, cta::log::LogContext& lc);

}  // namespace cta::grpc_server

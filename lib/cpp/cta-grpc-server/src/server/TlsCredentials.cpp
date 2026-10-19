/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/server/TlsCredentials.hpp"

#include "common/log/LogContext.hpp"
#include "common/utils/utils.hpp"

#include <grpcpp/security/tls_certificate_provider.h>
#include <grpcpp/security/tls_credentials_options.h>
#include <vector>

namespace cta::grpc_server {

std::shared_ptr<::grpc::ServerCredentials>
createTlsServerCredentials(const TlsConfig& tls, bool requireClientCertificate, cta::log::LogContext& lc) {
  using namespace ::grpc::experimental;

  IdentityKeyCertPair cert;
  cert.private_key = cta::utils::readFileAsString(tls.server_key_path);
  cert.certificate_chain = cta::utils::readFileAsString(tls.server_cert_path);

  std::shared_ptr<StaticDataCertificateProvider> provider;
  {
    cta::log::ScopedParamContainer spc(lc);
    spc.add("tls_key", tls.server_key_path).add("tls_cert", tls.server_cert_path);

    // if we have a CA certificate chain, use it
    if (auto caChain = tls.chain_cert_path; caChain.has_value()) {
      spc.add("tls_chain", caChain.value());
      provider = std::make_shared<StaticDataCertificateProvider>(cta::utils::readFileAsString(caChain.value()),
                                                                 std::vector {cert});
    } else {
      spc.add("tls_chain", "<no chain file>");
      provider = std::make_shared<StaticDataCertificateProvider>(std::vector {cert});
    }
    lc.log(cta::log::INFO, "TLS configuration loaded");
  }

  const grpc_ssl_client_certificate_request_type certRequestType =
    requireClientCertificate ? GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY :
                               GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE;

  TlsServerCredentialsOptions tlsOptions {provider};
  tlsOptions.set_cert_request_type(certRequestType);
  tlsOptions.watch_root_certs();
  tlsOptions.watch_identity_key_cert_pairs();
  return TlsServerCredentials(tlsOptions);
}

}  // namespace cta::grpc_server

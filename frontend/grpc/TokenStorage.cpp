/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TokenStorage.hpp"

#include "common/utils/Base64.hpp"
#include "utils.hpp"

void cta::frontend::grpc::server::TokenStorage::store(const std::string& strToken,
                                                      const std::string& strClientPrincipal) {
  std::unique_lock<std::shared_mutex> lck(m_mtxLockStorage);
  m_umapTokens[strToken] = strClientPrincipal;
}

bool cta::frontend::grpc::server::TokenStorage::validate(const std::string& strEncodedToken) const {
  std::shared_lock<std::shared_mutex> lck(m_mtxLockStorage);

  if (std::string strDecodedToken = cta::utils::base64decode(strEncodedToken); m_umapTokens.contains(strDecodedToken)) {
    return true;
  }
  return false;
}

std::string cta::frontend::grpc::server::TokenStorage::getClientPrincipal(const std::string& strEncodedToken) const {
  std::string strDecodedToken = cta::utils::base64decode(strEncodedToken);

  std::shared_lock<std::shared_mutex> lck(m_mtxLockStorage);
  auto it = m_umapTokens.find(strDecodedToken);
  if (it != m_umapTokens.end()) {
    return it->second;
  }
  return "";
}

void cta::frontend::grpc::server::TokenStorage::remove(const std::string& strEncodedToken) {
  std::unique_lock<std::shared_mutex> lck(m_mtxLockStorage);

  std::string strDecodedToken = cta::utils::base64decode(strEncodedToken);
  m_umapTokens.erase(strDecodedToken);
}

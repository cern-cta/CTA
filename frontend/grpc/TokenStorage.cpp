/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TokenStorage.hpp"

#include "utils.hpp"
#include "common/utils/Base64.hpp"

void cta::frontend::grpc::server::TokenStorage::store(const std::string& strToken, const std::string& strSpn) {
  std::lock_guard<std::mutex> lck(m_mtxLockStorage);
  m_umapTokens[strToken] = strSpn;
}

bool cta::frontend::grpc::server::TokenStorage::validate(const std::string& strToken) const {
  std::lock_guard<std::mutex> lck(m_mtxLockStorage);
  std::string strDecodedToken = cta::utils::base64decode(strToken);

  if (m_umapTokens.find(strDecodedToken) != m_umapTokens.end()) {
    return true;
  }
  return false;
}

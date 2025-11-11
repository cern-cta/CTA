/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2023 CERN
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

#include "TokenStorage.hpp"

#include "utils.hpp"
#include "common/utils/Base64.hpp"

void cta::frontend::grpc::server::TokenStorage::store(const std::string& strToken,
                                                       const std::string& strClientPrincipal,
                                                       const std::string& strServicePrincipal) {
  std::lock_guard<std::mutex> lck(m_mtxLockStorage);
  TokenInfo info;
  info.clientPrincipal = strClientPrincipal;
  info.servicePrincipal = strServicePrincipal;
  m_umapTokens[strToken] = info;
}

bool cta::frontend::grpc::server::TokenStorage::validate(const std::string& strToken) const {
  std::lock_guard<std::mutex> lck(m_mtxLockStorage);
  std::string strDecodedToken = cta::utils::base64decode(strToken);

  if (m_umapTokens.find(strDecodedToken) != m_umapTokens.end()) {
    return true;
  }
  return false;
}

std::string cta::frontend::grpc::server::TokenStorage::getClientPrincipal(const std::string& strToken) const {
  std::lock_guard<std::mutex> lck(m_mtxLockStorage);
  std::string strDecodedToken = cta::utils::base64decode(strToken);

  auto it = m_umapTokens.find(strDecodedToken);
  if (it != m_umapTokens.end()) {
    return it->second.clientPrincipal;
  }
  return "";
}

void cta::frontend::grpc::server::TokenStorage::remove(const std::string& strToken) {
  std::lock_guard<std::mutex> lck(m_mtxLockStorage);
  std::string strDecodedToken = cta::utils::base64decode(strToken);
  m_umapTokens.erase(strDecodedToken);
}
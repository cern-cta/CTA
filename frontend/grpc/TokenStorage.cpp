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
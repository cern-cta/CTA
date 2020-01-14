/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          CTA Frontend Tape Namespace query class
 * @copyright      Copyright 2019 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <xroot_plugins/GrpcEndpoint.hpp>


std::string cta::grpc::Endpoint::getPath(const std::string &diskFileId) const {
  // diskFileId is sent to CTA as a uint64_t, but we store it as a decimal string, cf.:
  //   XrdSsiCtaRequestMessage.cpp: request.diskFileID = std::to_string(notification.file().fid());
  // Here we convert it back to make the namespace query:
  uint64_t id = strtoul(diskFileId.c_str(), NULL, 0);
  if(id == 0) return ("Invalid disk ID");
  auto response = m_grpcClient->GetMD(eos::rpc::FILE, id, "");

  return response.fmd().name().empty() ? "Bad response from nameserver" : response.fmd().path();
}

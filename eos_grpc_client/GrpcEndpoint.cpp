/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include "GrpcEndpoint.hpp"

#include "common/exception/UserError.hpp"
#include "common/exception/Exception.hpp"


std::string eos::client::Endpoint::getPath(const std::string &diskFileId) const {
  // diskFileId is sent to CTA as a uint64_t, but we store it as a decimal string, cf.:
  //   XrdSsiCtaRequestMessage.cpp: request.diskFileID = std::to_string(notification.file().fid());
  // Here we convert it back to make the namespace query:
  uint64_t id = strtoull(diskFileId.c_str(), NULL, 0);
  if(id == 0) throw cta::exception::UserError("Invalid disk ID");
  auto response = m_grpcClient->GetMD(eos::rpc::FILE, id, "");

  if (response.fmd().name().empty()) {
    throw cta::exception::Exception("Bad response from nameserver");
  } else {
    return response.fmd().path();
  }
}

eos::rpc::InsertReply eos::client::Endpoint::fileInsert(const eos::rpc::FileMdProto &file) const {
  std::vector<eos::rpc::FileMdProto> paths;
  paths.push_back(file);
  eos::rpc::InsertReply replies;
  m_grpcClient->FileInsert(paths, replies);
  return replies;

}

eos::rpc::InsertReply eos::client::Endpoint::containerInsert(const eos::rpc::ContainerMdProto &container) const {
  std::vector<eos::rpc::ContainerMdProto> paths;
  paths.push_back(container);
  eos::rpc::InsertReply replies;
  m_grpcClient->ContainerInsert(paths, replies);
  return replies;

}

void eos::client::Endpoint::getCurrentIds(uint64_t &cid, uint64_t &fid) const {
  m_grpcClient->GetCurrentIds(cid, fid);
}

eos::rpc::MDResponse eos::client::Endpoint::getMD(eos::rpc::TYPE type, uint64_t id, const std::string &path, bool showJson) const {
  return m_grpcClient->GetMD(type, id, path, showJson);
}
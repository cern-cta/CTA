/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "GrpcEndpoint.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"

std::string eos::client::Endpoint::getPath(const std::string& diskFileId) const {
  // diskFileId is sent to CTA as a uint64_t, but we store it as a decimal string, cf.:
  //   XrdSsiCtaRequestMessage.cpp: request.diskFileID = std::to_string(notification.file().fid());
  // Here we convert it back to make the namespace query:
  uint64_t id = strtoull(diskFileId.c_str(), nullptr, 0);
  if (id == 0) {
    throw cta::exception::UserError("Invalid disk ID");
  }
  auto response = m_grpcClient->GetMD(eos::rpc::FILE, id, "");

  if (response.fmd().name().empty()) {
    throw cta::exception::Exception("Bad response from nameserver");
  } else {
    return response.fmd().path();
  }
}

eos::rpc::InsertReply eos::client::Endpoint::fileInsert(const eos::rpc::FileMdProto& file) const {
  std::vector<eos::rpc::FileMdProto> paths;
  paths.push_back(file);
  eos::rpc::InsertReply replies;
  m_grpcClient->FileInsert(paths, replies);
  return replies;
}

eos::rpc::InsertReply eos::client::Endpoint::containerInsert(const eos::rpc::ContainerMdProto& container) const {
  std::vector<eos::rpc::ContainerMdProto> paths;
  paths.push_back(container);
  eos::rpc::InsertReply replies;
  m_grpcClient->ContainerInsert(paths, replies);
  return replies;
}

void eos::client::Endpoint::getCurrentIds(uint64_t& cid, uint64_t& fid) const {
  m_grpcClient->GetCurrentIds(cid, fid);
}

eos::rpc::MDResponse
eos::client::Endpoint::getMD(eos::rpc::TYPE type, uint64_t id, const std::string& path, bool showJson) const {
  return m_grpcClient->GetMD(type, id, path, showJson);
}

grpc::Status eos::client::Endpoint::setXAttr(const std::string& path,
                                             const std::string& attrKey,
                                             const std::string& attrValue) const {
  eos::rpc::NSRequest request;
  eos::rpc::NSResponse reply;

  request.mutable_xattr()->mutable_id()->set_path(path);
  (*(request.mutable_xattr()->mutable_xattrs()))[attrKey] = attrValue;

  return m_grpcClient->Exec(request);
}

/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <grpc++/grpc++.h>
#include <memory>

#include "Rpc.grpc.pb.h"

namespace eos::client {

class GrpcClient {
public:
  explicit GrpcClient(std::shared_ptr<grpc::Channel> channel) : stub_(eos::rpc::Eos::NewStub(channel)) {}

  // factory function
  static std::unique_ptr<GrpcClient> Create(const std::string& endpoint, const std::string& token);

  std::string ping(const std::string& payload);

  int FileInsert(const std::vector<eos::rpc::FileMdProto>& paths, eos::rpc::InsertReply& replies);

  int ContainerInsert(const std::vector<eos::rpc::ContainerMdProto>& dirs, eos::rpc::InsertReply& replies);

  // Obtain current container ID and current file ID
  void GetCurrentIds(uint64_t& cid, uint64_t& fid);

  // Obtain container or file metadata
  eos::rpc::MDResponse GetMD(eos::rpc::TYPE type, uint64_t id, const std::string& path, bool showJson = false);

  grpc::Status Exec(eos::rpc::NSRequest& request);

  void set_ssl(bool onoff) { m_SSL = onoff; }

  bool ssl() const { return m_SSL; }

  void set_token(std::string_view token) { m_token = token; }

  const std::string& token() const { return m_token; }

  void* nextTag() { return reinterpret_cast<void*>(++m_tag); }

private:
  std::unique_ptr<eos::rpc::Eos::Stub> stub_;
  std::string m_token;
  bool m_SSL = false;
  uint64_t m_tag = 0;
  uint64_t m_eos_cid = 0;  //!< EOS current container ID
  uint64_t m_eos_fid = 0;  //!< EOS current file ID
};

}  // namespace eos::client

/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#pragma once

#include <memory>
#include <grpc++/grpc++.h>
#include "Rpc.grpc.pb.h"

namespace eos::client {

class GrpcClient
{
public:
  explicit GrpcClient(std::shared_ptr<grpc::Channel> channel) :
    stub_(eos::rpc::Eos::NewStub(channel)) { }

  // factory function
  static std::unique_ptr<GrpcClient> Create(std::string endpoint, std::string token);

  std::string ping(const std::string& payload);

  int FileInsert(const std::vector<eos::rpc::FileMdProto> &paths, eos::rpc::InsertReply &replies);

  int ContainerInsert(const std::vector<eos::rpc::ContainerMdProto> &dirs, eos::rpc::InsertReply &replies);

  // Obtain current container ID and current file ID
  void GetCurrentIds(uint64_t &cid, uint64_t &fid);

  // Obtain container or file metadata
  eos::rpc::MDResponse GetMD(eos::rpc::TYPE type, uint64_t id, const std::string &path, bool showJson = false);

  grpc::Status Exec(eos::rpc::NSRequest& request);

  void set_ssl(bool onoff) { m_SSL = onoff; }

  bool ssl() const { return m_SSL; }

  void set_token(std::string_view token) {
    m_token = token;
  }

  const std::string& token() const { return m_token; }

  void *nextTag() {
    return reinterpret_cast<void*>(++m_tag);
  }

private:
  std::unique_ptr<eos::rpc::Eos::Stub> stub_;
  std::string m_token;
  bool m_SSL = false;
  uint64_t m_tag = 0;
  uint64_t m_eos_cid = 0;   //!< EOS current container ID
  uint64_t m_eos_fid = 0;   //!< EOS current file ID
};

} // namespace eos::client

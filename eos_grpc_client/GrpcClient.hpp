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

namespace eos {
namespace client {

class GrpcClient
{
public:
  explicit GrpcClient(std::shared_ptr<grpc::Channel> channel) :
    stub_(eos::rpc::Eos::NewStub(channel)),
    m_SSL(false),
    m_tag(0),
    m_eos_cid(0),
    m_eos_fid(0) { }

  // factory function
  static std::unique_ptr<GrpcClient> Create(std::string endpoint, std::string token);

  std::string ping(const std::string& payload);

  int FileInsert(const std::vector<eos::rpc::FileMdProto> &paths, eos::rpc::InsertReply &replies);

  int ContainerInsert(const std::vector<eos::rpc::ContainerMdProto> &dirs, eos::rpc::InsertReply &replies);

  // Obtain current container ID and current file ID
  void GetCurrentIds(uint64_t &cid, uint64_t &fid);

  // Obtain container or file metadata
  eos::rpc::MDResponse GetMD(eos::rpc::TYPE type, uint64_t id, const std::string &path, bool showJson = false);

  void set_ssl(bool onoff) {
    m_SSL = onoff;
  }

  bool ssl() const {
    return m_SSL;
  }

  void set_token(const std::string &token) {
    m_token = token;
  }

  std::string token() const {
    return m_token;
  }

  void *nextTag() {
    return reinterpret_cast<void*>(++m_tag);
  }

private:
  std::unique_ptr<eos::rpc::Eos::Stub> stub_;
  bool m_SSL;
  std::string m_token;
  uint64_t m_tag;
  uint64_t m_eos_cid;   //!< EOS current container ID
  uint64_t m_eos_fid;   //!< EOS current file ID
};

}} // namespace eos::client

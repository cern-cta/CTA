/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Obtain metadata from EOS namespace using gRPC
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

#pragma once

#include <grpc++/grpc++.h>
#include "Rpc.grpc.pb.h"

namespace eos {
namespace client {

class GrpcClient
{
public:
  explicit GrpcClient(std::shared_ptr<grpc::Channel> channel) :
    stub_(eos::rpc::Eos::NewStub(channel)),
    m_tag(0) { }

  // factory function
  static std::unique_ptr<GrpcClient> Create(std::string endpoint, std::string token);

  // Obtain container or file metadata
  eos::rpc::MDResponse GetMD(eos::rpc::TYPE type, uint64_t id, const std::string &path);

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
  std::string m_token;
  uint64_t m_tag;
};

}} // namespace eos::client

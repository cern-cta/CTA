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

#include "GrpcClient.hpp"

namespace eos {
namespace client {

std::unique_ptr<GrpcClient> GrpcClient::Create(std::string endpoint, std::string token) {
  std::unique_ptr<eos::client::GrpcClient> p(
    new eos::client::GrpcClient(grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials())));
  p->set_token(token);
  return p;
}

eos::rpc::MDResponse GrpcClient::GetMD(eos::rpc::TYPE type, uint64_t id, const std::string& path) {
  eos::rpc::MDRequest request;

  request.set_type(type);
  request.mutable_id()->set_id(id);
  request.mutable_id()->set_path(path);
  request.set_authkey(token());

  grpc::ClientContext context;
  grpc::CompletionQueue cq;

  auto tag = nextTag();
  std::unique_ptr<grpc::ClientAsyncReader<eos::rpc::MDResponse>> rpc(stub_->AsyncMD(&context, request, &cq, tag));

  eos::rpc::MDResponse response;
  while (true) {
    void* got_tag;
    bool ok = false;
    bool ret = cq.Next(&got_tag, &ok);
    if (!ret || !ok || got_tag != tag) {
      break;
    }
    rpc->Read(&response, tag);
  }
  return response;
}

}  // namespace client
}  // namespace eos

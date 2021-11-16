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

#include <sys/stat.h>
#include <sstream>
#include "GrpcClient.hpp"

namespace eos {
namespace client {


std::unique_ptr<GrpcClient>
GrpcClient::Create(std::string endpoint, std::string token)
{
  std::unique_ptr<eos::client::GrpcClient> p(
    new eos::client::GrpcClient(grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()))
  );
  p->set_ssl(false);
  p->set_token(token);
  return p;
}


std::string GrpcClient::ping(const std::string& payload)
{
  eos::rpc::PingRequest request;
  request.set_message(payload);
  request.set_authkey(token());
  eos::rpc::PingReply reply;
  grpc::ClientContext context;
  // The producer-consumer queue we use to communicate asynchronously with the
  // gRPC runtime.
  grpc::CompletionQueue cq;
  grpc::Status status;
  // stub_->AsyncPing() performs the RPC call, returning an instance we
  // store in "rpc". Because we are using the asynchronous API, we need to
  // hold on to the "rpc" instance in order to get updates on the ongoing RPC.
  std::unique_ptr<grpc::ClientAsyncResponseReader<eos::rpc::PingReply>> rpc(
    stub_->AsyncPing(&context, request, &cq));
  // Request that, upon completion of the RPC, "reply" be updated with the
  // server's response; "status" with the indication of whether the operation
  // was successful. Tag the request.
  auto tag = nextTag();
  rpc->Finish(&reply, &status, tag);
  void* got_tag;
  bool ok = false;
  // Block until the next result is available in the completion queue "cq".
  // The return value of Next should always be checked. This return value
  // tells us whether there is any kind of event or the cq_ is shutting down.
  GPR_ASSERT(cq.Next(&got_tag, &ok));
  // Verify that the result from "cq" corresponds, by its tag, our previous
  // request.
  GPR_ASSERT(got_tag == tag);
  // ... and that the request was completed successfully. Note that "ok"
  // corresponds solely to the request for updates introduced by Finish().
  GPR_ASSERT(ok);

  // Act upon the status of the actual RPC.
  if(status.ok()) {
    return reply.message();
  } else {
    throw std::runtime_error("Ping failed with error: " + status.error_message());
  }
}


int GrpcClient::FileInsert(const std::vector<eos::rpc::FileMdProto> &files, eos::rpc::InsertReply &replies)
{
  eos::rpc::FileInsertRequest request;
  for(auto &file : files) {
    if(file.id() >= m_eos_fid) {
      std::stringstream err;
      err << "FATAL ERROR: attempt to inject file with id=" << file.id()
          << ", which exceeds EOS current file id=" << m_eos_fid;
      throw std::runtime_error(err.str());
    }
    *(request.add_files()) = file;
  }

  request.set_authkey(token());
  grpc::ClientContext context;
  // The producer-consumer queue we use to communicate asynchronously with the gRPC runtime.
  grpc::CompletionQueue cq;
  grpc::Status status;

  std::unique_ptr<grpc::ClientAsyncResponseReader<eos::rpc::InsertReply>> rpc(
    stub_->AsyncFileInsert(&context, request, &cq));
  // Request that, upon completion of the RPC, "replies" be updated with the
  // server's response; "status" with the indication of whether the operation
  // was successful. Tag the request.
  auto tag = nextTag();
  rpc->Finish(&replies, &status, tag);
  void* got_tag;
  bool ok = false;
  // Block until the next result is available in the completion queue "cq".
  // The return value of Next should always be checked. This return value
  // tells us whether there is any kind of event or the cq_ is shutting down.
  GPR_ASSERT(cq.Next(&got_tag, &ok));
  // Verify that the result from "cq" corresponds, by its tag, our previous
  // request.
  GPR_ASSERT(got_tag == tag);
  // ... and that the request was completed successfully. Note that "ok"
  // corresponds solely to the request for updates introduced by Finish().
  GPR_ASSERT(ok);

  // Act upon the status of the actual RPC.
  if(status.ok()) {
    int num_errors = 0;
    for(auto &retc : replies.retc()) {
      if(retc != 0) ++num_errors;
    }
    return num_errors;
  } else {
    throw std::runtime_error("FileInsert failed with error: " + status.error_message());
  }
}


int GrpcClient::ContainerInsert(const std::vector<eos::rpc::ContainerMdProto> &dirs, eos::rpc::InsertReply &replies)
{
  eos::rpc::ContainerInsertRequest request;

  // Tell EOS gRPC to behave like "eos mkdir": inherit xattrs from parent dir
  request.set_inherit_md(true);

  for(auto &dir : dirs) {
    if(dir.id() >= m_eos_cid) {
      std::stringstream err;
      err << "FATAL ERROR: attempt to inject container with id=" << dir.id()
          << ", which exceeds EOS current container id=" << m_eos_cid;
      throw std::runtime_error(err.str());
    }
    *(request.add_container()) = dir;
  }

  request.set_authkey(token());
  grpc::ClientContext context;
  // The producer-consumer queue we use to communicate asynchronously with the gRPC runtime
  grpc::CompletionQueue cq;
  grpc::Status status;

  std::unique_ptr<grpc::ClientAsyncResponseReader<eos::rpc::InsertReply>> rpc(
    stub_->AsyncContainerInsert(&context, request, &cq));
  // Request that, upon completion of the RPC, "replies" be updated with the
  // server's response; "status" with the indication of whether the operation
  // was successful. Tag the request.
  auto tag = nextTag();
  rpc->Finish(&replies, &status, tag);
  void* got_tag;
  bool ok = false;
  // Block until the next result is available in the completion queue "cq".
  // The return value of Next should always be checked. This return value
  // tells us whether there is any kind of event or the cq_ is shutting down.
  GPR_ASSERT(cq.Next(&got_tag, &ok));
  // Verify that the result from "cq" corresponds, by its tag, our previous
  // request.
  GPR_ASSERT(got_tag == tag);
  // ... and that the request was completed successfully. Note that "ok"
  // corresponds solely to the request for updates introduced by Finish().
  GPR_ASSERT(ok);

  // Return the status of the RPC
  if(status.ok()) {
    int num_errors = 0;
    for(auto &retc : replies.retc()) {
      if(retc != 0) ++num_errors;
    }
    return num_errors;
  } else {
    throw std::runtime_error("ContainerInsert failed with error: " + status.error_message());
  }
}


void GrpcClient::GetCurrentIds(uint64_t &cid, uint64_t &fid)
{
  eos::rpc::NsStatRequest request;
  request.set_authkey(token());

  grpc::ClientContext context;
  grpc::CompletionQueue cq;

  std::unique_ptr<grpc::ClientAsyncResponseReader<eos::rpc::NsStatResponse>> rpc(
    stub_->AsyncNsStat(&context, request, &cq));

  eos::rpc::NsStatResponse response;
  grpc::Status status;
  auto tag = nextTag();
  rpc->Finish(&response, &status, tag);

  void* got_tag;
  bool ok = false;
  GPR_ASSERT(cq.Next(&got_tag, &ok));
  GPR_ASSERT(got_tag == tag);
  GPR_ASSERT(ok);

  // Act upon the status of the actual RPC
  if(status.ok()) {
    cid = m_eos_cid = response.current_cid();
    fid = m_eos_fid = response.current_fid();
  } else {
    throw std::runtime_error("EOS namespace query failed with error: " + status.error_message());
  }
}


eos::rpc::MDResponse GrpcClient::GetMD(eos::rpc::TYPE type, uint64_t id, const std::string &path, bool showJson)
{
  eos::rpc::MDRequest request;

  request.set_type(type);
  request.mutable_id()->set_id(id);
  request.mutable_id()->set_path(path);
  request.set_authkey(token());

  if(showJson) {
    google::protobuf::util::JsonPrintOptions options;
    options.add_whitespace = true;
    options.always_print_primitive_fields = true;
    std::string logstring;
    google::protobuf::util::MessageToJsonString(request, &logstring, options);
    std::cout << logstring;
  }

  grpc::ClientContext context;
  grpc::CompletionQueue cq;

  auto tag = nextTag();
  std::unique_ptr<grpc::ClientAsyncReader<eos::rpc::MDResponse>> rpc(
    stub_->AsyncMD(&context, request, &cq, tag));

  eos::rpc::MDResponse response;
  while(true) {
    void *got_tag;
    bool ok = false;
    bool ret = cq.Next(&got_tag, &ok);
    if(!ret || !ok || got_tag != tag) break;
    rpc->Read(&response, tag);
  }
  if(showJson) {
    google::protobuf::util::JsonPrintOptions options;
    options.add_whitespace = true;
    options.always_print_primitive_fields = true;
    std::string logstring;
    google::protobuf::util::MessageToJsonString(response, &logstring, options);
    std::cout << logstring;
  }

  return response;
}

}} // namespace eos::client

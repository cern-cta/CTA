
#pragma once

#include "version.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/server_credentials.h>

#include <scheduler/Scheduler.hpp>
#include "common/log/Logger.hpp"
#include "cta_dcache.grpc.pb.h"

using cta::Scheduler;
using cta::catalogue::Catalogue;
using cta::dcache::rpc::CtaRpc;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

static const std::string CTA_DCACHE_VERSION = "cta-dcache-" + std::string(CTA_VERSION);

class CtaRpcImpl : public CtaRpc::Service {

private:
    std::unique_ptr <cta::catalogue::Catalogue> m_catalogue;
    std::unique_ptr <cta::Scheduler> m_scheduler;
    cta::log::Logger  *m_log;

public:
    CtaRpcImpl(cta::log::Logger *logger, std::unique_ptr<cta::catalogue::Catalogue> &catalogue, std::unique_ptr<cta::Scheduler> &scheduler);

    Status Version(::grpc::ServerContext *context, const ::google::protobuf::Empty *request, ::cta::admin::Version *response);

    Status Archive(::grpc::ServerContext* context, const ::cta::dcache::rpc::ArchiveRequest* request, ::cta::dcache::rpc::ArchiveResponse* response);
    Status Retrieve(::grpc::ServerContext* context, const ::cta::dcache::rpc::RetrieveRequest* request, ::cta::dcache::rpc::RetrieveResponse* response);
    Status Delete(::grpc::ServerContext* context, const ::cta::dcache::rpc::DeleteRequest* request, ::google::protobuf::Empty* response);
    Status CancelRetrieve(::grpc::ServerContext* context, const ::cta::dcache::rpc::CancelRetrieveRequest* request, ::google::protobuf::Empty* response);
};


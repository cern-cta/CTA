/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @copyright      Copyright(C) 2021 DESY
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
#include "FrontendGRpcSvc.h"
#include "version.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using cta::rpc::CtaRpc;

#include "common/log/SyslogLogger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/LogLevel.hpp"


class CtaRpcImpl final : public CtaRpc::Service {

    Status Version(::grpc::ServerContext* context, const ::google::protobuf::Empty* request, ::cta::admin::Version* response) override {
        response->set_cta_version(CTA_VERSION);
        response->set_xrootd_ssi_protobuf_interface_version("gPRC-frontend v0.0.1");
        return Status::OK;
    }
};


using namespace cta;

int main(const int argc, char *const *const argv) {

    std::unique_ptr <cta::log::Logger> m_log;
    m_log.reset(new log::StdoutLogger("cta-dev", "cta-grpc-frontend"));

    log::LogContext lc(*m_log);

    std::string server_address("0.0.0.0:17017");

    CtaRpcImpl svc;

//    grpc::EnableDefaultHealthCheckService(true);
//    grpc::reflection::InitProtoReflectionServerBuilderPlugin();

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&svc);
    // Finally assemble the server.
    std::unique_ptr <Server> server(builder.BuildAndStart());

    lc.log(log::INFO, "Listening on socket address: " + server_address);

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();


}
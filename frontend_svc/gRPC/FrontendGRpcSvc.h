
#ifndef CTA_FRONTENDGRPCSVC_H
#define CTA_FRONTENDGRPCSVC_H


#include <grpcpp/grpcpp.h>

#include "catalogue/CatalogueFactoryFactory.hpp"
#include "rdbms/Login.hpp"
#include <common/Configuration.hpp>
#include <common/utils/utils.hpp>
#include <scheduler/Scheduler.hpp>
#include <scheduler/OStoreDB/OStoreDBInit.hpp>
#include "common/log/SyslogLogger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/LogLevel.hpp"

#include "cta_rpc.grpc.pb.h"

using cta::Scheduler;
using cta::SchedulerDBInit_t;
using cta::SchedulerDB_t;
using cta::catalogue::Catalogue;
using cta::rdbms::Login;
using cta::rpc::CtaRpc;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

class CtaRpcImpl : public CtaRpc::Service {

private:
    std::unique_ptr <cta::catalogue::Catalogue> m_catalogue;
    std::unique_ptr <cta::SchedulerDB_t> m_scheddb;
    std::unique_ptr <cta::SchedulerDBInit_t> m_scheddb_init;
    std::unique_ptr <cta::Scheduler> m_scheduler;
    cta::log::LogContext  *m_log;

public:
    CtaRpcImpl(cta::log::LogContext *lc, std::unique_ptr<cta::catalogue::Catalogue> &catalogue, std::unique_ptr<cta::Scheduler> &scheduler);
    void run(const std::string server_address);
    Status Version(::grpc::ServerContext *context, const ::google::protobuf::Empty *request, ::cta::admin::Version *response);
    Status GetTapes(::grpc::ServerContext* context, const ::google::protobuf::Empty* request, ::grpc::ServerWriter<::cta::admin::TapeLsItem>* writer);
    Status GetStorageClasses(::grpc::ServerContext* context, const ::google::protobuf::Empty* request, ::grpc::ServerWriter<::cta::admin::StorageClassLsItem>* writer);
};

#endif //CTA_FRONTENDGRPCSVC_H


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


#endif //CTA_FRONTENDGRPCSVC_H

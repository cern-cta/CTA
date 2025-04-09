// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "disk/DiskSystem.hpp"
#include "TemplateAdminCmdStream.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class DiskSystemLsWriteReactor : public TemplateAdminCmdStream<disk::DiskSystem, cta::admin::DiskSystemLsItem, decltype(&fillDiskSystemItem)> {
    public:
        DiskSystemLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        cta::admin::DiskSystemLsItem* getMessageField(cta::xrd::Data* data) override;
};

DiskSystemLsWriteReactor::DiskSystemLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                   cta::Scheduler& scheduler,
                                                   const std::string& instanceName,
                                                   const cta::xrd::Request* request)
    : TemplateAdminCmdStream<disk::DiskSystem, cta::admin::DiskSystemLsItem, decltype(&fillDiskSystemItem)>(
        catalogue,
        scheduler,
        instanceName,
        catalogue.DiskSystem()->getAllDiskSystems(),
        cta::admin::HeaderType::DISKSYSTEM_LS,
        &fillDiskSystemItem) {}

cta::admin::DiskSystemLsItem* DiskSystemLsWriteReactor::getMessageField(cta::xrd::Data* data) {
    return data->mutable_dsls_item();
}
} // namespace cta::frontend::grpc
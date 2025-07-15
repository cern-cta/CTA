// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/DiskInstanceSpace.hpp"
#include "TemplateAdminCmdStream.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class DiskInstanceSpaceLsWriteReactor : public TemplateAdminCmdStream<cta::common::dataStructures::DiskInstanceSpace, cta::admin::DiskInstanceSpaceLsItem, decltype(&fillDiskInstanceSpaceItem)> {
    public:
        DiskInstanceSpaceLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        cta::admin::DiskInstanceSpaceLsItem* getMessageField(cta::xrd::Data* data) override;
};

DiskInstanceSpaceLsWriteReactor::DiskInstanceSpaceLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                                 cta::Scheduler& scheduler,
                                                                 const std::string& instanceName,
                                                                 const cta::xrd::Request* request)
    : TemplateAdminCmdStream<cta::common::dataStructures::DiskInstanceSpace,
                             cta::admin::DiskInstanceSpaceLsItem,
                             decltype(&fillDiskInstanceSpaceItem)>(
        catalogue,
        scheduler,
        instanceName,
        catalogue.DiskInstanceSpace()->getAllDiskInstanceSpaces(),
        cta::admin::HeaderType::DISKINSTANCESPACE_LS,
        &fillDiskInstanceSpaceItem) {}

cta::admin::DiskInstanceSpaceLsItem* DiskInstanceSpaceLsWriteReactor::getMessageField(cta::xrd::Data* data) {
    return data->mutable_disls_item();
}

} // namespace cta::frontend::grpc
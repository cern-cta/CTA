// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "TemplateAdminCmdStream.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class DiskInstanceLsWriteReactor : public TemplateAdminCmdStream<cta::common::dataStructures::DiskInstance, cta::admin::DiskInstanceLsItem, decltype(&fillDiskInstanceItem)> {
    public:
        DiskInstanceLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        cta::admin::DiskInstanceLsItem* getMessageField(cta::xrd::Data* data) override;
};

DiskInstanceLsWriteReactor::DiskInstanceLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                       cta::Scheduler& scheduler,
                                                       const std::string& instanceName,
                                                       const cta::xrd::Request* request)
    : TemplateAdminCmdStream<cta::common::dataStructures::DiskInstance,
                             cta::admin::DiskInstanceLsItem,
                             decltype(&fillDiskInstanceItem)>(catalogue,
                                                              scheduler,
                                                              instanceName,
                                                              catalogue.DiskInstance()->getAllDiskInstances(),
                                                              cta::admin::HeaderType::DISKINSTANCE_LS,
                                                              &fillDiskInstanceItem) {}


cta::admin::DiskInstanceLsItem* DiskInstanceLsWriteReactor::getMessageField(cta::xrd::Data* data) {
    return data->mutable_dils_item();
}
} // namespace cta::frontend::grpc
// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"
#include "TemplateAdminCmdStream.hpp"

namespace cta::frontend::grpc {

class AdminLsWriteReactor : public TemplateAdminCmdStream<cta::common::dataStructures::AdminUser, cta::admin::AdminLsItem, decltype(&fillAdminItem)> {
    public:
        AdminLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        cta::admin::AdminLsItem* getMessageField(cta::xrd::Data* data) override;
};

AdminLsWriteReactor::AdminLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                         cta::Scheduler& scheduler,
                                         const std::string& instanceName,
                                         const cta::xrd::Request* request)
    : TemplateAdminCmdStream<cta::common::dataStructures::AdminUser, cta::admin::AdminLsItem, decltype(&fillAdminItem)>(
        catalogue,
        scheduler,
        instanceName,
        catalogue.AdminUser()->getAdminUsers(),
        cta::admin::HeaderType::ADMIN_LS,
        &fillAdminItem) {}

cta::admin::AdminLsItem* AdminLsWriteReactor::getMessageField(cta::xrd::Data* data) {
    return data->mutable_adls_item();
}
} // namespace cta::frontend::grpc
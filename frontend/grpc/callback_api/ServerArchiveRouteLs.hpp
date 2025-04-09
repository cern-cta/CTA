// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include <common/dataStructures/ArchiveRouteTypeSerDeser.hpp>
#include "TemplateAdminCmdStream.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class ArchiveRouteLsWriteReactor : public TemplateAdminCmdStream<cta::common::dataStructures::ArchiveRoute, cta::admin::ArchiveRouteLsItem, decltype(&fillArchiveRouteItem)> {
    public:
        ArchiveRouteLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        cta::admin::ArchiveRouteLsItem* getMessageField(cta::xrd::Data* data) override;
};

ArchiveRouteLsWriteReactor::ArchiveRouteLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                       cta::Scheduler& scheduler,
                                                       const std::string& instanceName,
                                                       const cta::xrd::Request* request)
    : TemplateAdminCmdStream<cta::common::dataStructures::ArchiveRoute,
                             cta::admin::ArchiveRouteLsItem,
                             decltype(&fillArchiveRouteItem)>(catalogue,
                                                              scheduler,
                                                              instanceName,
                                                              catalogue.ArchiveRoute()->getArchiveRoutes(),
                                                              cta::admin::HeaderType::ARCHIVEROUTE_LS,
                                                              &fillArchiveRouteItem) {}

cta::admin::ArchiveRouteLsItem* ArchiveRouteLsWriteReactor::getMessageField(cta::xrd::Data* data) {
    return data->mutable_arls_item();
}

} // namespace cta::frontend::grpc
// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/PhysicalLibrary.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"
#include "TemplateAdminCmdStream.hpp"

namespace cta::frontend::grpc {

class PhysicalLibraryLsWriteReactor : public TemplateAdminCmdStream<cta::common::dataStructures::PhysicalLibrary, cta::admin::PhysicalLibraryLsItem, decltype(&fillPhysicalLibraryItem)> {
    public:
        cta::admin::PhysicalLibraryLsItem* getMessageField(cta::xrd::Data* data) override;
        PhysicalLibraryLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
};

PhysicalLibraryLsWriteReactor::PhysicalLibraryLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : TemplateAdminCmdStream<cta::common::dataStructures::PhysicalLibrary,
      cta::admin::PhysicalLibraryLsItem,
      decltype(&fillPhysicalLibraryItem)>
      (catalogue, scheduler, instanceName, catalogue.PhysicalLibrary()->getPhysicalLibraries(), cta::admin::HeaderType::PHYSICALLIBRARY_LS, &fillPhysicalLibraryItem) {}

cta::admin::PhysicalLibraryLsItem* PhysicalLibraryLsWriteReactor::getMessageField(cta::xrd::Data* data) {
    return data->mutable_plls_item();
}
} // namespace cta::frontend::grpc
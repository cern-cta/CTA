#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "../RequestMessage.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "TemplateAdminCmdStream.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class LogicalLibraryLsWriteReactor : public TemplateAdminCmdStream<cta::common::dataStructures::LogicalLibrary, cta::admin::LogicalLibraryLsItem, decltype(&fillLogicalLibraryItem)> {
    public:
        LogicalLibraryLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        cta::admin::LogicalLibraryLsItem* getMessageField(cta::xrd::Data* data) override;
    private:
        static std::list<common::dataStructures::LogicalLibrary> buildLogicalLibraryList(cta::catalogue::Catalogue& catalogue, const cta::xrd::Request* request) {
            using namespace cta::admin;
            request::RequestMessage requestMsg(*request);
            std::optional<bool> disabled = requestMsg.getOptional(admin::OptionBoolean::DISABLED);
            std::list<cta::common::dataStructures::LogicalLibrary> logicalLibraryList = catalogue.LogicalLibrary()->getLogicalLibraries();
            std::list<cta::common::dataStructures::LogicalLibrary>::iterator next_ll;
            
            next_ll = logicalLibraryList.begin();
            while (next_ll != logicalLibraryList.end()) {
                if (disabled && disabled.value() != (*next_ll).isDisabled) {
                    // pop this entry from the list
                    next_ll = logicalLibraryList.erase(next_ll); // erase returns the next valid element
                } else {
                    ++next_ll;
                }
            }
            return logicalLibraryList;
        }
};

LogicalLibraryLsWriteReactor::LogicalLibraryLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : TemplateAdminCmdStream<cta::common::dataStructures::LogicalLibrary,
        cta::admin::LogicalLibraryLsItem,
        decltype(&fillLogicalLibraryItem)>
        (catalogue, scheduler, instanceName, buildLogicalLibraryList(catalogue, request), cta::admin::HeaderType::LOGICALLIBRARY_LS, &fillLogicalLibraryItem) {}

cta::admin::LogicalLibraryLsItem* LogicalLibraryLsWriteReactor::getMessageField(cta::xrd::Data* data) {
    return data->mutable_llls_item();
}

} // namespace cta::frontend::grpc
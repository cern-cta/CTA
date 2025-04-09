// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include "../RequestMessage.hpp"
#include <grpcpp/grpcpp.h>
#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class RepackLsWriteReactor : public TemplateAdminCmdStream<common::dataStructures::RepackInfo,
                                                           cta::admin::RepackLsItem,
                                                           decltype(&fillRepackRequestItem),
                                                           cta::common::dataStructures::VidToTapeMap> {
public:
  RepackLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                       cta::Scheduler& scheduler,
                       const std::string& instanceName,
                       const cta::xrd::Request* request)
      : TemplateAdminCmdStream<common::dataStructures::RepackInfo,
                               cta::admin::RepackLsItem,
                               decltype(&fillRepackRequestItem),
                               cta::common::dataStructures::VidToTapeMap>(
          catalogue,
          scheduler,
          instanceName,
          buildRepackList(catalogue, scheduler, request).first,
          cta::admin::HeaderType::REPACK_LS,
          &fillRepackRequestItem,
          buildRepackList(catalogue, scheduler, request).second) {}

  cta::admin::RepackLsItem* getMessageField(cta::xrd::Data* data) override { return data->mutable_rels_item(); }

private:
    static std::pair<std::list<common::dataStructures::RepackInfo>, cta::common::dataStructures::VidToTapeMap> 
    buildRepackList(cta::catalogue::Catalogue &catalogue, 
                   cta::Scheduler &scheduler, 
                   const cta::xrd::Request* request) {
        
        request::RequestMessage requestMsg(*request);
        
        std::optional<std::string> vid = requestMsg.getOptional(cta::admin::OptionString::VID);
        std::list<common::dataStructures::RepackInfo> repackList;
        
        if(!vid.has_value()){
            repackList = scheduler.getRepacks();
            repackList.sort([](const cta::common::dataStructures::RepackInfo& repackInfo1, 
                              const cta::common::dataStructures::RepackInfo& repackInfo2){
                return repackInfo1.creationLog.time < repackInfo2.creationLog.time;
            });
        } else {
            repackList.push_back(scheduler.getRepack(vid.value()));
        }

        // Build the tape VID map
        std::set<std::string, std::less<>> tapeVids;
        std::transform(repackList.begin(), repackList.end(),
                      std::inserter(tapeVids, tapeVids.begin()),
                      [](const cta::common::dataStructures::RepackInfo &ri) {return ri.vid;});
        
        cta::common::dataStructures::VidToTapeMap tapeVidMap = catalogue.Tape()->getTapesByVid(tapeVids);
        
        return std::make_pair(std::move(repackList), std::move(tapeVidMap));
    }
};
} // namespace cta::frontend::grpc
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

class RepackLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        RepackLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        void NextWrite() override;
    private:
        std::optional<std::string> m_vid;
        std::list<common::dataStructures::RepackInfo> m_repackList;
        cta::common::dataStructures::VidToTapeMap m_tapeVidMap;
        std::list<cta::common::dataStructures::RepackInfo>::const_iterator next_repack;
};

RepackLsWriteReactor::RepackLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName) {
    request::RequestMessage requestMsg(*request);

    m_vid = requestMsg.getOptional(cta::admin::OptionString::VID);
    if(!m_vid.has_value()){
        m_repackList = scheduler.getRepacks();
        m_repackList.sort([](cta::common::dataStructures::RepackInfo repackInfo1, cta::common::dataStructures::RepackInfo repackInfo2){
            return repackInfo1.creationLog.time < repackInfo2.creationLog.time;
        });
    } else {
        m_repackList.push_back(scheduler.getRepack(m_vid.value()));
    }

    std::set<std::string, std::less<>> tapeVids;
    std::transform(m_repackList.begin(), m_repackList.end(),
                std::inserter(tapeVids, tapeVids.begin()),
                [](cta::common::dataStructures::RepackInfo &ri) {return ri.vid;});
    m_tapeVidMap = catalogue.Tape()->getTapesByVid(tapeVids);
    next_repack = m_repackList.cbegin();
    NextWrite();
}

void RepackLsWriteReactor::NextWrite() {
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::REPACK_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(next_repack != m_repackList.cend()) {
            const auto& repackRequest = *next_repack;
            ++next_repack;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::RepackLsItem *repackRequestItem = data->mutable_rels_item();

            fillRepackRequestItem(repackRequest, repackRequestItem, m_tapeVidMap, m_instanceName);

            m_response.set_allocated_data(data);
            StartWrite(&m_response);
            return; // because we will be called in a loop by OnWriteDone()
        } // end while
        std::cout << "Finishing the call on the server side" << std::endl;
        // Finish the call
        Finish(::grpc::Status::OK);
    }
}
} // namespace cta::frontend::grpc
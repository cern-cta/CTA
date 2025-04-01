// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include "../RequestMessage.hpp"
#include <grpcpp/grpcpp.h>

namespace cta::frontend::grpc {

class RepackLsWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
    public:
        RepackLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request);
        void OnWriteDone(bool ok) override {
            if (!ok) {
                std::cout << "Unexpected failure in OnWriteDone" << std::endl;
                Finish(Status(::grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone"));
            }
            NextWrite();
        }
        void OnDone() override;
        void NextWrite();
    private:
        std::optional<std::string> m_vid;
        std::list<common::dataStructures::RepackInfo> m_repackList;
        cta::common::dataStructures::VidToTapeMap m_tapeVidMap;
        bool m_isHeaderSent; // or could be a static variable in the function NextWrite()
        cta::xrd::StreamResponse m_response;
        std::list<cta::common::dataStructures::RepackInfo>::const_iterator next_repack;
};

void RepackLsWriteReactor::OnDone() {
    delete this;
}

RepackLsWriteReactor::RepackLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request)
    : m_isHeaderSent(false) {
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
            next_repack++;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::RepackLsItem *repackRequestItem = data->mutable_rels_item();

            uint64_t filesLeftToRetrieve = repackRequest.totalFilesToRetrieve - repackRequest.retrievedFiles;
            uint64_t filesLeftToArchive = repackRequest.totalFilesToArchive - repackRequest.archivedFiles;
            uint64_t totalFilesToRetrieve = repackRequest.totalFilesToRetrieve;
            uint64_t totalFilesToArchive = repackRequest.totalFilesToArchive;

            repackRequestItem->set_vid(repackRequest.vid);
            repackRequestItem->set_tapepool(m_tapeVidMap[repackRequest.vid].tapePoolName);
            repackRequestItem->set_repack_buffer_url(repackRequest.repackBufferBaseURL);
            repackRequestItem->set_user_provided_files(repackRequest.userProvidedFiles);
            repackRequestItem->set_total_files_on_tape_at_start(repackRequest.totalFilesOnTapeAtStart);
            repackRequestItem->set_total_bytes_on_tape_at_start(repackRequest.totalBytesOnTapeAtStart);
            repackRequestItem->set_all_files_selected_at_start(repackRequest.allFilesSelectedAtStart);
            repackRequestItem->set_total_files_to_retrieve(totalFilesToRetrieve);
            repackRequestItem->set_total_bytes_to_retrieve(repackRequest.totalBytesToRetrieve);
            repackRequestItem->set_total_files_to_archive(totalFilesToArchive);
            repackRequestItem->set_total_bytes_to_archive(repackRequest.totalBytesToArchive);
            repackRequestItem->set_retrieved_files(repackRequest.retrievedFiles);
            repackRequestItem->set_retrieved_bytes(repackRequest.retrievedBytes);
            repackRequestItem->set_files_left_to_retrieve(filesLeftToRetrieve);
            repackRequestItem->set_files_left_to_archive(filesLeftToArchive);
            repackRequestItem->set_archived_files(repackRequest.archivedFiles);
            repackRequestItem->set_archived_bytes(repackRequest.archivedBytes);
            repackRequestItem->set_failed_to_retrieve_files(repackRequest.failedFilesToRetrieve);
            repackRequestItem->set_failed_to_retrieve_bytes(repackRequest.failedBytesToRetrieve);
            repackRequestItem->set_failed_to_archive_files(repackRequest.failedFilesToArchive);
            repackRequestItem->set_failed_to_archive_bytes(repackRequest.failedBytesToArchive);
            repackRequestItem->set_total_failed_files(repackRequest.failedFilesToRetrieve + repackRequest.failedFilesToArchive);
            repackRequestItem->set_status(toString(repackRequest.status));
            uint64_t repackTime = time(nullptr) - repackRequest.creationLog.time;
            repackRequestItem->set_repack_finished_time(repackRequest.repackFinishedTime);
            if(repackRequest.repackFinishedTime != 0){
                //repackFinishedTime != 0: repack is finished
                repackTime = repackRequest.repackFinishedTime - repackRequest.creationLog.time;
            }
            repackRequestItem->set_repack_time(repackTime);
            repackRequestItem->mutable_creation_log()->set_username(repackRequest.creationLog.username);
            repackRequestItem->mutable_creation_log()->set_host(repackRequest.creationLog.host);
            repackRequestItem->mutable_creation_log()->set_time(repackRequest.creationLog.time);
            //Last expanded fSeq is in reality the next FSeq to Expand. So last one is next - 1
            repackRequestItem->set_last_expanded_fseq(repackRequest.lastExpandedFseq != 0 ? repackRequest.lastExpandedFseq - 1 : 0);
            repackRequestItem->mutable_destination_infos()->Clear();
            for(auto destinationInfo: repackRequest.destinationInfos){
                auto * destinationInfoToInsert = repackRequestItem->mutable_destination_infos()->Add();
                destinationInfoToInsert->set_vid(destinationInfo.vid);
                destinationInfoToInsert->set_files(destinationInfo.files);
                destinationInfoToInsert->set_bytes(destinationInfo.bytes);
            }

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
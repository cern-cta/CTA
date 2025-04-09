// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "common/checksum/ChecksumBlobSerDeser.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"
#include "../RequestMessage.hpp"
#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class RecycleTapeFileLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        RecycleTapeFileLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        void NextWrite() override;
    private:
        cta::catalogue::FileRecycleLogItor m_fileRecycleLogItor;
};

RecycleTapeFileLsWriteReactor::RecycleTapeFileLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName) {
    using namespace cta::admin;

    bool has_any = false;
    
    cta::catalogue::RecycleTapeFileSearchCriteria searchCriteria;
    request::RequestMessage requestMsg(*request);
    searchCriteria.vid = requestMsg.getOptional(OptionString::VID, &has_any);
    
    std::optional<std::string> diskFileIdStr;
    try {
       diskFileIdStr = requestMsg.getAndValidateDiskFileIdOptional(&has_any);
    } catch (const cta::exception::UserError &ex) {
        Finish(Status(::grpc::StatusCode::INVALID_ARGUMENT, ex.getMessageValue()));
        return;
    }

    searchCriteria.diskFileIds = requestMsg.getOptional(OptionStrList::FILE_ID, &has_any);
    
    if (diskFileIdStr){
        // single option on the command line we need to do the conversion ourselves.
        if(!searchCriteria.diskFileIds) searchCriteria.diskFileIds = std::vector<std::string>();
        searchCriteria.diskFileIds->push_back(diskFileIdStr.value());
    }
    searchCriteria.diskInstance = requestMsg.getOptional(OptionString::INSTANCE, &has_any);
    searchCriteria.archiveFileId = requestMsg.getOptional(OptionUInt64::ARCHIVE_FILE_ID, &has_any);

    searchCriteria.recycleLogTimeMin = requestMsg.getOptional(OptionUInt64::LOG_UNIXTIME_MIN, &has_any);
    searchCriteria.recycleLogTimeMax = requestMsg.getOptional(OptionUInt64::LOG_UNIXTIME_MAX, &has_any);
    searchCriteria.vo = requestMsg.getOptional(OptionString::VO, &has_any);

    // Copy number on its own does not give a valid set of search criteria (no &has_any)
    searchCriteria.copynb = requestMsg.getOptional(OptionUInt64::COPY_NUMBER);

    if(!has_any){
        Finish(Status(::grpc::INVALID_ARGUMENT, "Must specify at least one of the following search options: vid, fxid, fxidfile, archiveFileId, instance, vo, ltmin, ltmax"));
        return;
    }
    
    m_fileRecycleLogItor = catalogue.FileRecycleLog()->getFileRecycleLogItor(searchCriteria);

    NextWrite();
}

void RecycleTapeFileLsWriteReactor::NextWrite() {
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::RECYLETAPEFILE_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(m_fileRecycleLogItor.hasMore()) {

            common::dataStructures::FileRecycleLog fileRecycleLog = m_fileRecycleLogItor.next();

            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::RecycleTapeFileLsItem *recycleLogToReturn = data->mutable_rtfls_item();

            fillRecycleTapeFileItem(fileRecycleLog, recycleLogToReturn, m_instanceName);
        
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
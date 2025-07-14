// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "catalogue/CatalogueItor.hpp"
#include "common/checksum/ChecksumBlobSerDeser.hpp"
#include "../RequestMessage.hpp"
#include "CtaAdminServerWriteReactor.hpp"

namespace cta::frontend::grpc {

class TapeFileLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        TapeFileLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        void NextWrite() override;
    private:
        catalogue::ArchiveFileItor m_tapeFileItor;
};

TapeFileLsWriteReactor::TapeFileLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
  : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName) {
    using namespace cta::admin;

    request::RequestMessage requestMsg(*request);
    bool has_any = false; // set to true if at least one optional option is set

    // Get the search criteria from the optional options
    cta::catalogue::TapeFileSearchCriteria searchCriteria;

    searchCriteria.vid = requestMsg.getOptional(OptionString::VID, &has_any);

    auto getAndValidateDiskFileIdOptional = [requestMsg](bool* has_any) -> std::optional<std::string> {
        using namespace cta::admin;
        auto diskFileIdHex  = requestMsg.getOptional(OptionString::FXID, has_any);
        auto diskFileIdStr  = requestMsg.getOptional(OptionString::DISK_FILE_ID, has_any);

        if(diskFileIdHex && diskFileIdStr) {
            throw exception::UserError("File ID can't be received in both string (" + diskFileIdStr.value() + ") and hexadecimal (" + diskFileIdHex.value() + ") formats");
        }

        if(diskFileIdHex) {
            // If provided, convert FXID (hexadecimal) to DISK_FILE_ID (decimal)
            if (!utils::isValidHex(diskFileIdHex.value())) {
                throw cta::exception::UserError(diskFileIdHex.value() + " is not a valid hexadecimal file ID value");
            }
            return cta::utils::hexadecimalToDecimal(diskFileIdHex.value());
        }

        return diskFileIdStr;
    };

    // Disk file IDs can be a list or a single ID
    auto diskFileIdStr = getAndValidateDiskFileIdOptional(&has_any);
    searchCriteria.diskFileIds = requestMsg.getOptional(OptionStrList::FILE_ID, &has_any);
    if(diskFileIdStr) {
        if(!searchCriteria.diskFileIds) searchCriteria.diskFileIds = std::vector<std::string>();
        searchCriteria.diskFileIds->push_back(diskFileIdStr.value());
    }
    searchCriteria.diskInstance = requestMsg.getOptional(OptionString::INSTANCE, &has_any);

    searchCriteria.archiveFileId = requestMsg.getOptional(OptionUInt64::ARCHIVE_FILE_ID, &has_any);

    if(!has_any) {
        std::cout << "Must specify at least one of the following search options: vid, fxid, fxidfile or archiveFileId, will throw" << std::endl;
        throw cta::exception::UserError("Must specify at least one of the following search options: vid, fxid, fxidfile or archiveFileId");
    }

    m_tapeFileItor = catalogue.ArchiveFile()->getArchiveFilesItor(searchCriteria);

    NextWrite();
}

void TapeFileLsWriteReactor::NextWrite() {
    std::cout << "In TapeFileLsWriteReactor::NextWrite(), just entered!" << std::endl;
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response(); // https://stackoverflow.com/questions/75693340/how-to-set-oneof-field-in-c-grpc-server-and-read-from-client
        std::cout << "header is not sent, sending the header" << std::endl;
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::TAPEFILE_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        std::cout << "about to call StartWrite on the server side" << std::endl;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        std::cout << "called StartWrite on the server" << std::endl;
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(m_tapeFileItor.hasMore()) {
            const cta::common::dataStructures::ArchiveFile archiveFile = m_tapeFileItor.next();

            for(auto jt = archiveFile.tapeFiles.cbegin(); jt != archiveFile.tapeFiles.cend(); jt++) {
                m_response.Clear();                    
                cta::xrd::Data* data = new cta::xrd::Data();
                cta::admin::TapeFileLsItem *tf_item = data->mutable_tfls_item();

                auto af = tf_item->mutable_af();
                af->set_archive_id(archiveFile.archiveFileID);
                af->set_storage_class(archiveFile.storageClass);
                af->set_creation_time(archiveFile.creationTime);
                af->set_size(archiveFile.fileSize);

                // Checksum
                common::ChecksumBlob csb;
                checksum::ChecksumBlobToProtobuf(archiveFile.checksumBlob, csb);
                for(auto csb_it = csb.cs().begin(); csb_it != csb.cs().end(); ++csb_it) {
                    auto cs_ptr = af->add_checksum();
                    cs_ptr->set_type(csb_it->type());
                    cs_ptr->set_value(checksum::ChecksumBlob::ByteArrayToHex(csb_it->value()));
                }

                // Disk file
                auto df = data->mutable_tfls_item()->mutable_df();
                df->set_disk_id(archiveFile.diskFileId);
                df->set_disk_instance(archiveFile.diskInstance);
                df->mutable_owner_id()->set_uid(archiveFile.diskFileInfo.owner_uid);
                df->mutable_owner_id()->set_gid(archiveFile.diskFileInfo.gid);

                // Tape file
                auto tf = data->mutable_tfls_item()->mutable_tf();
                tf->set_vid(jt->vid);
                tf->set_copy_nb(jt->copyNb);
                tf->set_block_id(jt->blockId);
                tf->set_f_seq(jt->fSeq);

                m_response.set_allocated_data(data);
                StartWrite(&m_response);
            } // end for
            return;
        } // end while
        std::cout << "Finishing the call on the server side" << std::endl;
        // Finish the call
        Finish(::grpc::Status::OK);
    }
}
} // namespace cta::frontend::grpc
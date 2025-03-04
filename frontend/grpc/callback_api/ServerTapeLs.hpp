#include "CtaAdminServer.hpp"
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

class TapeLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        void OnWriteDone(bool ok) override {
            if (!ok) {
                Finish(Status(grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone"));
            }
            NextWrite();
        }
        // void OnDone() override
        void NextWrite();
    private:
        std::list<common::dataStructures::Tape> m_tapeList;
        bool m_isHeaderSent; // or could be a static variable in the function NextWrite()
};

// constructor does not make the first call to write, currently.
// In the example's Lister case, the first call to write is made in the constructor
TapeLsWriteReactor::TapeLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) : m_isHeaderSent(false) {
    // all this will go into a common method in a base class called
    // getTapesList or something
    using namespace cta::admin;

    cta::catalogue::TapeSearchCriteria searchCriteria;

    bool has_any = false; // set to true if at least one optional option is set

    // Get the search criteria from the optional options

    searchCriteria.full                = requestMsg.getOptional(OptionBoolean::FULL,                       &has_any);
    searchCriteria.fromCastor          = requestMsg.getOptional(OptionBoolean::FROM_CASTOR,                &has_any);
    searchCriteria.capacityInBytes     = requestMsg.getOptional(OptionUInt64::CAPACITY,                    &has_any);
    searchCriteria.logicalLibrary      = requestMsg.getOptional(OptionString::LOGICAL_LIBRARY,             &has_any);
    searchCriteria.tapePool            = requestMsg.getOptional(OptionString::TAPE_POOL,                   &has_any);
    searchCriteria.vo                  = requestMsg.getOptional(OptionString::VO,                          &has_any);
    searchCriteria.vid                 = requestMsg.getOptional(OptionString::VID,                         &has_any);
    searchCriteria.mediaType           = requestMsg.getOptional(OptionString::MEDIA_TYPE,                  &has_any);
    searchCriteria.vendor              = requestMsg.getOptional(OptionString::VENDOR,                      &has_any);
    searchCriteria.purchaseOrder       = requestMsg.getOptional(OptionString::MEDIA_PURCHASE_ORDER_NUMBER, &has_any);
    searchCriteria.physicalLibraryName = requestMsg.getOptional(OptionString::PHYSICAL_LIBRARY,            &has_any);
    searchCriteria.diskFileIds         = requestMsg.getOptional(OptionStrList::FILE_ID,                    &has_any);
    auto stateOpt                      = requestMsg.getOptional(OptionString::STATE,                       &has_any);
    if(stateOpt){
    searchCriteria.state = common::dataStructures::Tape::stringToState(stateOpt.value(), true);
    }
    if(!(requestMsg.has_flag(OptionBoolean::ALL) || has_any)) {
    throw cta::exception::UserError("Must specify at least one search option, or --all");
    } else if(requestMsg.has_flag(OptionBoolean::ALL) && has_any) {
    throw cta::exception::UserError("Cannot specify --all together with other search options");
    }

    m_tapeList = m_catalogue.Tape()->getTapes(searchCriteria);
    NextWrite();
}

void TapeLsWriteReactor::NextWrite() {
    cta::xrd::Response response;
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        response.mutable_header()->set_type(cta::xrd::Response::RSP_SUCCESS);
        response.mutable_header()->set_show_header(cta::admin::HeaderType::TAPE_LS);
        m_isHeaderSent = true;
        StartWrite(&response); // this will trigger the OnWriteDone method
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        for(; !m_tapeList.empty(); m_tapeList.pop_front()) {
            Data record;
            auto &tape = m_tapeList.front();
            auto tape_item = record.mutable_tals_item();

            tape_item->set_vid(tape.vid);
            tape_item->set_media_type(tape.mediaType);
            tape_item->set_vendor(tape.vendor);
            tape_item->set_logical_library(tape.logicalLibraryName);
            tape_item->set_tapepool(tape.tapePoolName);
            tape_item->set_vo(tape.vo);
            tape_item->set_encryption_key_name((bool)tape.encryptionKeyName ? tape.encryptionKeyName.value() : "-");
            tape_item->set_capacity(tape.capacityInBytes);
            tape_item->set_occupancy(tape.dataOnTapeInBytes);
            tape_item->set_last_fseq(tape.lastFSeq);
            tape_item->set_full(tape.full);
            tape_item->set_dirty(tape.dirty);
            tape_item->set_from_castor(tape.isFromCastor);
            tape_item->set_label_format(cta::admin::LabelFormatToProtobuf(tape.labelFormat));
            tape_item->set_read_mount_count(tape.readMountCount);
            tape_item->set_write_mount_count(tape.writeMountCount);
            tape_item->set_nb_master_files(tape.nbMasterFiles);
            tape_item->set_master_data_in_bytes(tape.masterDataInBytes);
            
            if(tape.labelLog) {
                ::cta::common::TapeLog * labelLog = tape_item->mutable_label_log();
                labelLog->set_drive(tape.labelLog.value().drive);
                labelLog->set_time(tape.labelLog.value().time);
            }
            if(tape.lastWriteLog){
                ::cta::common::TapeLog * lastWriteLog = tape_item->mutable_last_written_log();
                lastWriteLog->set_drive(tape.lastWriteLog.value().drive);
                lastWriteLog->set_time(tape.lastWriteLog.value().time);
            }
            if(tape.lastReadLog){
                ::cta::common::TapeLog * lastReadLog = tape_item->mutable_last_read_log();
                lastReadLog->set_drive(tape.lastReadLog.value().drive);
                lastReadLog->set_time(tape.lastReadLog.value().time);
            }
            ::cta::common::EntryLog * creationLog = tape_item->mutable_creation_log();
            creationLog->set_username(tape.creationLog.username);
            creationLog->set_host(tape.creationLog.host);
            creationLog->set_time(tape.creationLog.time);
            ::cta::common::EntryLog * lastModificationLog = tape_item->mutable_last_modification_log();
            lastModificationLog->set_username(tape.lastModificationLog.username);
            lastModificationLog->set_host(tape.lastModificationLog.host);
            lastModificationLog->set_time(tape.lastModificationLog.time);
            tape_item->set_comment(tape.comment);

            tape_item->set_state(tape.getStateStr());
            tape_item->set_state_reason(tape.stateReason ? tape.stateReason.value() : "");
            tape_item->set_purchase_order(tape.purchaseOrder ? tape.purchaseOrder.value() : "");
            tape_item->set_physical_library(tape.physicalLibraryName ? tape.physicalLibraryName.value() : "");
            tape_item->set_state_update_time(tape.stateUpdateTime);
            tape_item->set_state_modified_by(tape.stateModifiedBy);
            if (tape.verificationStatus) {
                tape_item->set_verification_status(tape.verificationStatus.value());
            }
            StartWrite(&record);
            return; // because we will be called in a loop by OnWriteDone()
        } // end for
        // did not write anything
        if (m_tapeList.empty()) {
            // Finish the call
            Finish(::grpc::Status::OK);
        }
    }
}
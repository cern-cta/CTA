#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include "common/dataStructures/LabelFormatSerDeser.hpp"

inline void fillTapeItem(const cta::common::dataStructures::Tape &tape,
    cta::admin::TapeLsItem *tape_item,
    const std::string m_instanceName) {

    tape_item->set_instance_name(m_instanceName);
    tape_item->set_vid(tape.vid);
    tape_item->set_media_type(tape.mediaType);
    tape_item->set_vendor(tape.vendor);
    tape_item->set_logical_library(tape.logicalLibraryName);
    tape_item->set_tapepool(tape.tapePoolName);
    tape_item->set_vo(tape.vo);
    tape_item->set_encryption_key_name(tape.encryptionKeyName.value_or(""));
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
}
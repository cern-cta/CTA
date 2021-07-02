/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @copyright      Copyright(C) 2021 DESY
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "FrontendGRpcSvc.h"
#include "version.h"

Status CtaRpcImpl::Version(::grpc::ServerContext *context, const ::google::protobuf::Empty *request,
                           ::cta::admin::Version *response) {
    response->set_cta_version(CTA_VERSION);
    response->set_xrootd_ssi_protobuf_interface_version("gPRC-frontend v0.0.1");
    return Status::OK;
}

Status CtaRpcImpl::GetStorageClasses(::grpc::ServerContext* context, const ::google::protobuf::Empty* request,
                                     ::grpc::ServerWriter<::cta::admin::StorageClassLsItem>* writer) {

    ::cta::admin::StorageClassLsItem storaceClass_item;
    for (const auto &sc: m_catalogue->getStorageClasses()) {

        storaceClass_item.Clear();

        storaceClass_item.set_name(sc.name);
        storaceClass_item.set_nb_copies(sc.nbCopies);
        storaceClass_item.set_vo(sc.vo.name);
        storaceClass_item.mutable_creation_log()->set_username(sc.creationLog.username);
        storaceClass_item.mutable_creation_log()->set_host(sc.creationLog.host);
        storaceClass_item.mutable_creation_log()->set_time(sc.creationLog.time);
        storaceClass_item.mutable_last_modification_log()->set_username(sc.lastModificationLog.username);
        storaceClass_item.mutable_last_modification_log()->set_host(sc.lastModificationLog.host);
        storaceClass_item.mutable_last_modification_log()->set_time(sc.lastModificationLog.time);
        storaceClass_item.set_comment(sc.comment);
        writer->Write(storaceClass_item, grpc::WriteOptions());
    }
    return Status::OK;
}

Status CtaRpcImpl::GetTapes(::grpc::ServerContext *context, const ::google::protobuf::Empty *request,
                            ::grpc::ServerWriter<::cta::admin::TapeLsItem> *writer) {

    ::cta::admin::TapeLsItem tape_item;

    for (const auto &tape: m_catalogue->getTapes()) {

        tape_item.Clear();

        tape_item.set_vid(tape.vid);
        tape_item.set_media_type(tape.mediaType);
        tape_item.set_vendor(tape.vendor);
        tape_item.set_logical_library(tape.logicalLibraryName);
        tape_item.set_tapepool(tape.tapePoolName);
        tape_item.set_vo(tape.vo);
        tape_item.set_encryption_key_name((bool) tape.encryptionKeyName ? tape.encryptionKeyName.value() : "-");
        tape_item.set_capacity(tape.capacityInBytes);
        tape_item.set_occupancy(tape.dataOnTapeInBytes);
        tape_item.set_last_fseq(tape.lastFSeq);
        tape_item.set_full(tape.full);
        tape_item.set_from_castor(tape.isFromCastor);
        tape_item.set_read_mount_count(tape.readMountCount);
        tape_item.set_write_mount_count(tape.writeMountCount);
        tape_item.set_nb_master_files(tape.nbMasterFiles);
        tape_item.set_master_data_in_bytes(tape.masterDataInBytes);

        if (tape.labelLog) {
            ::cta::common::TapeLog *labelLog = tape_item.mutable_label_log();
            labelLog->set_drive(tape.labelLog.value().drive);
            labelLog->set_time(tape.labelLog.value().time);
        }
        if (tape.lastWriteLog) {
            ::cta::common::TapeLog *lastWriteLog = tape_item.mutable_last_written_log();
            lastWriteLog->set_drive(tape.lastWriteLog.value().drive);
            lastWriteLog->set_time(tape.lastWriteLog.value().time);
        }
        if (tape.lastReadLog) {
            ::cta::common::TapeLog *lastReadLog = tape_item.mutable_last_read_log();
            lastReadLog->set_drive(tape.lastReadLog.value().drive);
            lastReadLog->set_time(tape.lastReadLog.value().time);
        }
        ::cta::common::EntryLog *creationLog = tape_item.mutable_creation_log();
        creationLog->set_username(tape.creationLog.username);
        creationLog->set_host(tape.creationLog.host);
        creationLog->set_time(tape.creationLog.time);
        ::cta::common::EntryLog *lastModificationLog = tape_item.mutable_last_modification_log();
        lastModificationLog->set_username(tape.lastModificationLog.username);
        lastModificationLog->set_host(tape.lastModificationLog.host);
        lastModificationLog->set_time(tape.lastModificationLog.time);
        tape_item.set_comment(tape.comment);

        tape_item.set_state(tape.getStateStr());
        tape_item.set_state_reason(tape.stateReason ? tape.stateReason.value() : "");
        tape_item.set_state_update_time(tape.stateUpdateTime);
        tape_item.set_state_modified_by(tape.stateModifiedBy);

        writer->Write(tape_item, grpc::WriteOptions());
    }
    return Status::OK;
}

void CtaRpcImpl::run(const std::string server_address) {

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(this);
    std::unique_ptr <Server> server(builder.BuildAndStart());

    m_log->log(cta::log::INFO, "Listening on socket address: " + server_address);
    server->Wait();
}

CtaRpcImpl::CtaRpcImpl(cta::log::LogContext *lc, std::unique_ptr<cta::catalogue::Catalogue> &catalogue, std::unique_ptr <cta::Scheduler> &scheduler):
    m_catalogue(std::move(catalogue)), m_scheduler(std::move(scheduler)) {
    m_log = lc;
}

using namespace cta;
using namespace cta::common;

int main(const int argc, char *const *const argv) {

    std::unique_ptr <cta::log::Logger> logger = std::unique_ptr<cta::log::Logger>(new log::StdoutLogger("cta-dev", "cta-grpc-frontend", true));
    log::LogContext lc(*logger);

    // use castor config to avoid dependency on xroot-ssi
    Configuration config("/etc/cta/cta.conf");

    std::string server_address("0.0.0.0:17017");

    // Initialise the Catalogue
    std::string catalogueConfigFile = "/etc/cta/cta-catalogue.conf";
    const rdbms::Login catalogueLogin = rdbms::Login::parseFile(catalogueConfigFile);

    const uint64_t nbArchiveFileListingConns = 2;
    auto catalogueFactory = catalogue::CatalogueFactoryFactory::create(*logger, catalogueLogin,
                                                                       10,
                                                                       nbArchiveFileListingConns);
    auto catalogue = catalogueFactory->create();
    try {
        catalogue->ping();
        lc.log(log::INFO, "Connected to catalog " + catalogue->getSchemaVersion().getSchemaVersion<std::string>());
    } catch (cta::exception::Exception &ex) {
        lc.log(cta::log::CRIT, ex.getMessageValue());
        exit(1);
    }


    // Initialise the Scheduler
    auto backed = config.getConfEntString("ObjectStore", "BackendPath");
    lc.log(log::INFO, "Using scheduler backend: " + backed);

    auto sInit = cta::make_unique<SchedulerDBInit_t>("Frontend", backed, *logger);
    auto scheddb = sInit->getSchedDB(*catalogue, *logger);
    scheddb->setBottomHalfQueueSize(25000);
    auto scheduler = cta::make_unique<cta::Scheduler>(*catalogue, *scheddb, 5, 2*1000*1000);

    CtaRpcImpl svc(&lc, catalogue, scheduler);
    svc.run(server_address);
}

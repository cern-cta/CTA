// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "common/dataStructures/DriveStatusSerDeser.hpp"
#include "common/dataStructures/MountTypeSerDeser.hpp"
#include "../RequestMessage.hpp"

namespace cta::frontend::grpc {

/*!
 * Converts list of drive configuration entries of all drives into searchable structure
 */
std::unordered_map<std::string, std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig>>
convertToMap(const std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig>& driveConfigs) {
  std::unordered_map<std::string, std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig>> driveConfigMap;

  for (const auto& config : driveConfigs) {
    driveConfigMap[config.tapeDriveName].emplace_back(config);
  }

  return driveConfigMap;
}

class DriveLsWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> /* CtaAdminServerWriteReactor */ {
    public:
        DriveLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, cta::log::LogContext lc, const cta::xrd::Request* request);
        void OnWriteDone(bool ok) override {
            std::cout << "In DriveLsWriteReactor, we are inside OnWriteDone" << std::endl;
            if (!ok) {
                std::cout << "Unexpected failure in OnWriteDone" << std::endl;
                Finish(Status(::grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone"));
            }
            std::cout << "Calling NextWrite inside server's OnWriteDone" << std::endl;
            NextWrite();
        }
        void OnDone() override;
        void NextWrite();
    private:
        std::list<common::dataStructures::TapeDrive> m_tapeDrives;
        std::unordered_map<std::string, std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig>> m_tapeDriveNameConfigMap;
        bool m_listAllDrives;
        std::optional<std::string> m_schedulerBackendName;
        bool m_isHeaderSent;
        cta::xrd::StreamResponse m_response;
        std::list<common::dataStructures::TapeDrive>::const_iterator next_dr;
        cta::log::LogContext m_lc;
};

void DriveLsWriteReactor::OnDone() {
    std::cout << "In DriveLsWriteReactor::OnDone(), about to delete this object" << std::endl;
    delete this;
}

DriveLsWriteReactor::DriveLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, cta::log::LogContext lc, const cta::xrd::Request* request)
: m_tapeDrives(catalogue.DriveState()->getTapeDrives()),
  m_tapeDriveNameConfigMap(convertToMap(catalogue.DriveConfig()->getTapeDriveConfigs())),
  m_listAllDrives(false),
  m_isHeaderSent(false),
  m_lc(lc) {

    using namespace cta::admin;

    std::cout << "In DriveLsWriteReactor constructor, just entered!" << std::endl;
    request::RequestMessage requestMsg(*request);

    m_listAllDrives = requestMsg.has_flag(cta::admin::OptionBoolean::ALL);
    m_schedulerBackendName = scheduler.getSchedulerBackendName();

    auto driveRegexOpt = requestMsg.getOptional(cta::admin::OptionString::DRIVE);

    // Dump all drives unless we specified a drive
    if (driveRegexOpt) {
        std::string driveRegexStr = '^' + driveRegexOpt.value() + '$';
        utils::Regex driveRegex(driveRegexStr.c_str());

        // Remove non-matching drives from the list
        for (auto dr_it = m_tapeDrives.begin(); dr_it != m_tapeDrives.end();) {
            if (driveRegex.has_match(dr_it->driveName)) {
                ++dr_it;
            } else {
                auto erase_it = dr_it;
                ++dr_it;
                m_tapeDrives.erase(erase_it);
            }
        }

        if (m_tapeDrives.empty()) {
            // throw exception::UserError(std::string("Drive ") + driveRegexOpt.value() + " not found.");
            Finish(Status(::grpc::StatusCode::INVALID_ARGUMENT, std::string("Drive ") + driveRegexOpt.value() + " not found."));
        }
    }

    next_dr = m_tapeDrives.cbegin();
    NextWrite();
}

void DriveLsWriteReactor::NextWrite() {
    std::cout << "In DriveLsWriteReactor::NextWrite(), just entered!" << std::endl;
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        std::cout << "header is not sent, sending the header" << std::endl;
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::DRIVE_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        std::cout << "about to call StartWrite on the server side" << std::endl;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        std::cout << "called StartWrite on the server" << std::endl;
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        std::cout << "header was sent, now entering the loop to send the data, should send " << m_tapeDrives.size() << " records!" << std::endl;
        while(next_dr != m_tapeDrives.cend()) {
            const auto& dr = *next_dr;
            const auto& driveConfigs = m_tapeDriveNameConfigMap[dr.driveName];
            ++next_dr;

            // Extract the SchedulerBackendName configuration if it exists
            std::string driveSchedulerBackendName = "unknown";
            // TODO: Add a log line here with an ERROR, in case commented-out condition is met
            auto it =
            std::find_if(driveConfigs.begin(),
                        driveConfigs.end(),
                        [&driveSchedulerBackendName](const cta::catalogue::DriveConfigCatalogue::DriveConfig& config) {
                            if (config.keyName == "SchedulerBackendName") {
                            driveSchedulerBackendName = config.value;
                            return true;
                            }
                            return false;
                        });

            if (it == driveConfigs.end()) {
                m_lc.log(cta::log::ERR,
                                   "DriveLsStream::fillBuffer could not find SchedulerBackendName configuration for drive" +
                                     dr.driveName);
            }
            if (!m_listAllDrives && m_schedulerBackendName.value() != driveSchedulerBackendName) {
                continue;
            }
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::DriveLsItem *dr_item = data->mutable_drls_item();
            
            dr_item->set_cta_version(dr.ctaVersion ? dr.ctaVersion.value() : "");
            dr_item->set_logical_library(dr.logicalLibrary);
            dr_item->set_drive_name(dr.driveName);
            dr_item->set_scheduler_backend_name(driveSchedulerBackendName);
            dr_item->set_host(dr.host);
            dr_item->set_logical_library_disabled(dr.logicalLibraryDisabled ? dr.logicalLibraryDisabled.value() : false);
            dr_item->set_desired_drive_state(dr.desiredUp ? cta::admin::DriveLsItem::UP : cta::admin::DriveLsItem::DOWN);
            dr_item->set_mount_type(cta::admin::MountTypeToProtobuf(dr.mountType));
            dr_item->set_drive_status(cta::admin::DriveStatusToProtobuf(dr.driveStatus));
            dr_item->set_vid(dr.currentVid ? dr.currentVid.value() : "");
            dr_item->set_tapepool(dr.currentTapePool ? dr.currentTapePool.value() : "");
            dr_item->set_vo(dr.currentVo ? dr.currentVo.value() : "");
            dr_item->set_files_transferred_in_session(dr.filesTransferedInSession ? dr.filesTransferedInSession.value() : 0);
            dr_item->set_bytes_transferred_in_session(dr.bytesTransferedInSession ? dr.bytesTransferedInSession.value() : 0);
            dr_item->set_session_id(dr.sessionId ? dr.sessionId.value() : 0);
            const auto lastUpdateTime = dr.lastModificationLog ? dr.lastModificationLog.value().time : time(nullptr);
            dr_item->set_time_since_last_update(time(nullptr) - lastUpdateTime);
            dr_item->set_current_priority(dr.currentPriority ? dr.currentPriority.value() : 0);
            dr_item->set_current_activity(dr.currentActivity ? dr.currentActivity.value() : "");
            dr_item->set_dev_file_name(dr.devFileName ? dr.devFileName.value() : "");
            dr_item->set_raw_library_slot(dr.rawLibrarySlot ? dr.rawLibrarySlot.value() : "");
            dr_item->set_comment(dr.userComment ? dr.userComment.value() : "");
            dr_item->set_reason(dr.reasonUpDown ? dr.reasonUpDown.value() : "");
            dr_item->set_physical_library(dr.physicalLibraryName ? dr.physicalLibraryName.value() : "");
            dr_item->set_physical_library_disabled(dr.physicalLibraryDisabled ? dr.physicalLibraryDisabled.value() : false);
            if (dr.mountType == cta::common::dataStructures::MountType::Retrieve) {
            dr_item->set_disk_system_name(dr.diskSystemName ? dr.diskSystemName.value() : "");
            dr_item->set_reserved_bytes(dr.reservedBytes ? dr.reservedBytes.value() : 0);
            }
            dr_item->set_session_elapsed_time(dr.sessionElapsedTime ? dr.sessionElapsedTime.value() : 0);

            auto driveConfig = dr_item->mutable_drive_config();

            for (const auto& config : driveConfigs) {
            auto driveConfigItemProto = driveConfig->Add();
            driveConfigItemProto->set_key(config.keyName);
            driveConfigItemProto->set_category(config.category);
            driveConfigItemProto->set_value(config.value);
            driveConfigItemProto->set_source(config.source);
            }

            // set the time spent in the current state
            uint64_t drive_time = time(nullptr);

            switch (dr.driveStatus) {
            using cta::common::dataStructures::DriveStatus;
                // clang-format off
            case DriveStatus::Probing:           drive_time -= dr.probeStartTime.value();    break;
            case DriveStatus::Up:                drive_time -= dr.downOrUpStartTime.value(); break;
            case DriveStatus::Down:              drive_time -= dr.downOrUpStartTime.value(); break;
            case DriveStatus::Starting:          drive_time -= dr.startStartTime.value();    break;
            case DriveStatus::Mounting:          drive_time -= dr.mountStartTime.value();    break;
            case DriveStatus::Transferring:      drive_time -= dr.transferStartTime.value(); break;
            case DriveStatus::CleaningUp:        drive_time -= dr.cleanupStartTime.value();  break;
            case DriveStatus::Unloading:         drive_time -= dr.unloadStartTime.value();   break;
            case DriveStatus::Unmounting:        drive_time -= dr.unmountStartTime.value();  break;
            case DriveStatus::DrainingToDisk:    drive_time -= dr.drainingStartTime.value(); break;
            case DriveStatus::Shutdown:          drive_time -= dr.shutdownTime.value();      break;
            case DriveStatus::Unknown:                                                       break;
                // clang-format on
            }
            dr_item->set_drive_status_since(drive_time);

            std::cout << "Calling StartWrite on the server, with some data this time" << std::endl;
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
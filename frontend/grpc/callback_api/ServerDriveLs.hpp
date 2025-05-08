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
#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

#pragma once

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

class DriveLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        DriveLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, cta::log::LogContext lc, const cta::xrd::Request* request);
        void NextWrite() override;
    private:
        std::list<common::dataStructures::TapeDrive> m_tapeDrives;
        std::unordered_map<std::string, std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig>> m_tapeDriveNameConfigMap;
        bool m_listAllDrives;
        std::list<common::dataStructures::TapeDrive>::const_iterator next_dr;
        cta::log::LogContext m_lc;
};

DriveLsWriteReactor::DriveLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, cta::log::LogContext lc, const cta::xrd::Request* request)
: CtaAdminServerWriteReactor(catalogue, scheduler, instanceName),
  m_tapeDrives(catalogue.DriveState()->getTapeDrives()),
  m_tapeDriveNameConfigMap(convertToMap(catalogue.DriveConfig()->getTapeDriveConfigs())),
  m_listAllDrives(false),
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
            
            fillDriveItem(dr, dr_item, m_instanceName, driveSchedulerBackendName, driveConfigs);

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
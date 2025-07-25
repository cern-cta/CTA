#pragma once

#include "CtaAdminResponseStream.hpp"
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>
#include "common/dataStructures/TapeDrive.hpp"
#include "catalogue/DriveConfigCatalogue.hpp"
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "common/dataStructures/DriveStatusSerDeser.hpp"
#include "common/dataStructures/MountTypeSerDeser.hpp"
#include "common/utils/Regex.hpp"
#include "common/log/LogContext.hpp"
#include "admin/AdminCmd.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"
#include <unordered_map>
#include <list>
#include <optional>

namespace cta::cmdline {

std::unordered_map<std::string, std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig>>
convertToMap(const std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig>& driveConfigs) {
    std::unordered_map<std::string, std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig>> driveConfigMap;
    for (const auto& config : driveConfigs) {
        driveConfigMap[config.tapeDriveName].emplace_back(config);
    }
    return driveConfigMap;
}

class DriveLsResponseStream : public CtaAdminResponseStream {
public:
    DriveLsResponseStream(cta::catalogue::Catalogue& catalogue, 
                         cta::Scheduler& scheduler, 
                         const std::string& instanceName, 
                         cta::log::LogContext& lc);
    
    template<typename RequestType>
    void parseRequest(const RequestType& request);
    
    bool isDone() override;
    cta::xrd::Data next() override;

private:
    cta::catalogue::Catalogue& m_catalogue;
    cta::Scheduler& m_scheduler;
    const std::string m_instanceName;
    cta::log::LogContext m_lc;
    
    std::list<common::dataStructures::TapeDrive> m_tapeDrives;
    std::unordered_map<std::string, std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig>> m_tapeDriveNameConfigMap;
    bool m_listAllDrives;
    std::optional<std::string> m_schedulerBackendName;
    
    void loadDriveData();
    void filterDrivesByRegex(const std::string& driveRegex);
    bool shouldIncludeDrive(const common::dataStructures::TapeDrive& drive);
    std::string extractSchedulerBackendName(const common::dataStructures::TapeDrive& drive);
};

DriveLsResponseStream::DriveLsResponseStream(cta::catalogue::Catalogue& catalogue, 
                                           cta::Scheduler& scheduler, 
                                           const std::string& instanceName, 
                                           cta::log::LogContext& lc)
    : m_catalogue(catalogue), 
      m_scheduler(scheduler), 
      m_instanceName(instanceName), 
      m_lc(lc),
      m_listAllDrives(false) {
    
    loadDriveData();
}

void DriveLsResponseStream::loadDriveData() {
    m_tapeDrives = m_catalogue.DriveState()->getTapeDrives();
    m_tapeDriveNameConfigMap = convertToMap(m_catalogue.DriveConfig()->getTapeDriveConfigs());
    m_schedulerBackendName = m_scheduler.getSchedulerBackendName();
}

template<typename RequestType>
void DriveLsResponseStream::parseRequest(const RequestType& request) {
    using namespace cta::admin;
    
    m_listAllDrives = request.has_flag(OptionBoolean::ALL);
    auto driveRegexOpt = request.getOptional(OptionString::DRIVE);
    
    if (driveRegexOpt) {
        filterDrivesByRegex(driveRegexOpt.value());
    }
}

void DriveLsResponseStream::filterDrivesByRegex(const std::string& driveRegex) {
    std::string driveRegexStr = '^' + driveRegex + '$';
    utils::Regex driveRegexObj(driveRegexStr.c_str());
    
    for (auto dr_it = m_tapeDrives.begin(); dr_it != m_tapeDrives.end();) {
        if (driveRegexObj.has_match(dr_it->driveName)) {
            ++dr_it;
        } else {
            auto erase_it = dr_it;
            ++dr_it;
            m_tapeDrives.erase(erase_it);
        }
    }
    
    if (m_tapeDrives.empty()) {
        throw std::invalid_argument("Drive " + driveRegex + " not found.");
    }
}

bool DriveLsResponseStream::isDone() {
    return m_tapeDrives.empty();
}

cta::xrd::Data DriveLsResponseStream::next() {
    if (isDone()) {
        throw std::runtime_error("Stream is exhausted");
    }
    
    while (!m_tapeDrives.empty()) {
        const auto drive = m_tapeDrives.front();
        m_tapeDrives.pop_front();
        
        if (!shouldIncludeDrive(drive)) {
            continue;
        }
        
        cta::xrd::Data data;
        auto driveItem = data.mutable_drls_item();
        
        std::string schedulerBackendName = extractSchedulerBackendName(drive);
        const auto& driveConfigs = m_tapeDriveNameConfigMap[drive.driveName];
        
        fillDriveItem(drive, driveItem, m_instanceName, schedulerBackendName, driveConfigs);
        
        return data;
    }
    
    throw std::runtime_error("Stream is exhausted");
}

bool DriveLsResponseStream::shouldIncludeDrive(const common::dataStructures::TapeDrive& drive) {
    if (m_listAllDrives) return true;
    
    std::string driveSchedulerBackendName = extractSchedulerBackendName(drive);
    return m_schedulerBackendName.value_or("unknown") == driveSchedulerBackendName;
}

std::string DriveLsResponseStream::extractSchedulerBackendName(const common::dataStructures::TapeDrive& drive) {
    const auto& driveConfigs = m_tapeDriveNameConfigMap[drive.driveName];
    
    auto it = std::find_if(driveConfigs.begin(), driveConfigs.end(),
        [](const cta::catalogue::DriveConfigCatalogue::DriveConfig& config) {
            return config.keyName == "SchedulerBackendName";
        });
    
    if (it == driveConfigs.end()) {
        m_lc.log(cta::log::ERR,
            "DriveLsResponseStream could not find SchedulerBackendName configuration for drive " + drive.driveName);
        return "unknown";
    }
    
    return it->value;
}

} // namespace cta::cmdline
#pragma once

#include "CtaAdminResponseStream.hpp"
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>
#include "catalogue/TapeSearchCriteria.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/log/LogContext.hpp"
#include "admin/AdminCmd.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"
#include <list>

namespace cta::cmdline {

class TapeLsResponseStream : public CtaAdminResponseStream {
public:
    TapeLsResponseStream(cta::catalogue::Catalogue& catalogue, 
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
    
    std::list<common::dataStructures::Tape> m_tapes;
    cta::catalogue::TapeSearchCriteria m_searchCriteria;
    
    void validateSearchCriteria(bool hasAllFlag, bool hasAnySearchOption) const;
};

TapeLsResponseStream::TapeLsResponseStream(cta::catalogue::Catalogue& catalogue, 
                                          cta::Scheduler& scheduler, 
                                          const std::string& instanceName, 
                                          cta::log::LogContext& lc)
    : m_catalogue(catalogue), 
      m_scheduler(scheduler), 
      m_instanceName(instanceName), 
      m_lc(lc) {
}

template<typename RequestType>
void TapeLsResponseStream::parseRequest(const RequestType& request) {
    using namespace cta::admin;
    
    bool has_any = false;  // set to true if at least one optional option is set
    
    // Get the search criteria from the optional options
    m_searchCriteria.full = request.getOptional(OptionBoolean::FULL, &has_any);
    m_searchCriteria.fromCastor = request.getOptional(OptionBoolean::FROM_CASTOR, &has_any);
    m_searchCriteria.capacityInBytes = request.getOptional(OptionUInt64::CAPACITY, &has_any);
    m_searchCriteria.logicalLibrary = request.getOptional(OptionString::LOGICAL_LIBRARY, &has_any);
    m_searchCriteria.tapePool = request.getOptional(OptionString::TAPE_POOL, &has_any);
    m_searchCriteria.vo = request.getOptional(OptionString::VO, &has_any);
    m_searchCriteria.vid = request.getOptional(OptionString::VID, &has_any);
    m_searchCriteria.mediaType = request.getOptional(OptionString::MEDIA_TYPE, &has_any);
    m_searchCriteria.vendor = request.getOptional(OptionString::VENDOR, &has_any);
    m_searchCriteria.purchaseOrder = request.getOptional(OptionString::MEDIA_PURCHASE_ORDER_NUMBER, &has_any);
    m_searchCriteria.physicalLibraryName = request.getOptional(OptionString::PHYSICAL_LIBRARY, &has_any);
    m_searchCriteria.diskFileIds = request.getOptional(OptionStrList::FILE_ID, &has_any);
    
    // Handle missing file copies (XRoot-specific option)
    if constexpr (std::is_same_v<RequestType, frontend::AdminCmdStream>) {
        m_searchCriteria.checkMissingFileCopies = request.getOptional(OptionBoolean::MISSING_FILE_COPIES, &has_any);
        if (m_searchCriteria.checkMissingFileCopies.value_or(false)) {
            m_searchCriteria.missingFileCopiesMinAgeSecs = request.getMissingFileCopiesMinAgeSecs();
        }
    }
    
    // Handle state option
    auto stateOpt = request.getOptional(OptionString::STATE, &has_any);
    if (stateOpt) {
        m_searchCriteria.state = common::dataStructures::Tape::stringToState(stateOpt.value(), true);
    }
    
    // Validate the search criteria
    bool hasAllFlag = request.has_flag(OptionBoolean::ALL);
    validateSearchCriteria(hasAllFlag, has_any);
    
    // Execute the search and get the tapes
    m_tapes = m_catalogue.Tape()->getTapes(m_searchCriteria);
}

bool TapeLsResponseStream::isDone() {
    return m_tapes.empty();
}

cta::xrd::Data TapeLsResponseStream::next() {
    if (isDone()) {
        throw std::runtime_error("Stream is exhausted");
    }
    
    const auto tape = m_tapes.front();
    m_tapes.pop_front();
    
    cta::xrd::Data data;
    auto tapeItem = data.mutable_tals_item();
    
    fillTapeItem(tape, tapeItem, m_instanceName);
    
    return data;
}

void TapeLsResponseStream::validateSearchCriteria(bool hasAllFlag, bool hasAnySearchOption) const {
    if (!(hasAllFlag || hasAnySearchOption)) {
        throw std::invalid_argument("Must specify at least one search option, or --all");
    } else if (hasAllFlag && hasAnySearchOption) {
        throw std::invalid_argument("Cannot specify --all together with other search options");
    }
}

} // namespace cta::cmdline
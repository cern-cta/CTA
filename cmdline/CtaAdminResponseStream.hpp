#pragma once

#include "cta_frontend.pb.h"
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

namespace cta::cmdline {

class CtaAdminResponseStream {
public:
  CtaAdminResponseStream(cta::catalogue::Catalogue& catalogue,
                         cta::Scheduler& scheduler,
                         const std::string& instanceName)
      : m_catalogue(catalogue),
        m_scheduler(scheduler),
        m_instanceName(instanceName) {}

  virtual ~CtaAdminResponseStream() = default;
  virtual bool isDone() = 0;
  virtual cta::xrd::Data next() = 0;
  virtual void init(const admin::AdminCmd& request) = 0;  //!< initializes the list of items to be streamed
  //!< init could throw because of the validateCmd call which can throw

protected:
  cta::catalogue::Catalogue& m_catalogue;
  cta::Scheduler& m_scheduler;
  const std::string m_instanceName;
};

}  // namespace cta::cmdline
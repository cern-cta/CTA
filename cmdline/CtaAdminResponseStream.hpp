#pragma once

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class CtaAdminResponseStream {
public:
    virtual ~CtaAdminResponseStream() = default;
    virtual bool isDone() = 0;
    virtual cta::xrd::Data next() = 0;
    virtual void init(const admin::AdminCmd& request) = 0; // initializes the list of items to be streamed
    // init could throw because of the validateCmd call which can throw
};

} // namespace cta::cmdline
#pragma once

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class CtaAdminResponseStream {
public:
    virtual ~CtaAdminResponseStream() = default;
    virtual bool isDone() = 0;
    virtual cta::xrd::Data next() = 0;
};

} // namespace cta::cmdline
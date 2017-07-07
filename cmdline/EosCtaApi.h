// Bind the XRootD SSI transport layer to a set of Google Protocol Buffer definitions

#ifndef __EOS_CTA_API_H
#define __EOS_CTA_API_H

#include "XrdSsiPbServiceClientSide.h"   // XRootD SSI/Protocol Buffer Service bindings (client side)
#include "eos/messages/eos_messages.pb.h"                     // Auto-generated message types from .proto file

#if 0
// Bind the type of the XrdSsiService to the types defined in the .proto file

typedef XrdSsiPbServiceClientSide<xrdssi::test::Request,     // Request message type
                                  xrdssi::test::Result,      // Response message type
                                  xrdssi::test::Metadata,    // Metadata message type
                                  xrdssi::test::Alert>       // Alert message type
        XrdSsiPbServiceType;
#endif

#endif

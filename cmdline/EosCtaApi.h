// Bind the XRootD SSI transport layer to a set of Google Protocol Buffer definitions

#ifndef __EOS_CTA_API_H
#define __EOS_CTA_API_H

#include "XrdSsiPbServiceClientSide.h"       // XRootD SSI/Protocol Buffer Service bindings (client side)
#include "eos/messages/eos_messages.pb.h"    // Auto-generated message types from .proto file

// Bind the type of the XrdSsiService to the types defined in the .proto file

typedef XrdSsiPbServiceClientSide<eos::wfe::Notification,    // XrdSSi Request message type
                                  eos::wfe::Response,        // XrdSsi Metadata message type
                                  eos::wfe::Alert>           // Alert message type
        XrdSsiPbServiceType;

#endif

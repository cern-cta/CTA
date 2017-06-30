// Bind the XRootD SSI transport layer to a set of Google Protocol Buffer definitions

#ifndef __TEST_CLIENT_H
#define __TEST_CLIENT_H

#include <iostream>
#include <unistd.h> // sleep

#include "XrdSsiPbServiceClientSide.h"   // XRootD SSI Service API
#include "test.pb.h"                     // Auto-generated message types from .proto file

// Bind the type of the XrdSsiClient to the types defined in the .proto file

typedef XrdSsiPbServiceClientSide<xrdssi::test::Request,     // Request message type
                                  xrdssi::test::Result,      // Response message type
                                  xrdssi::test::Metadata,    // Metadata message type
                                  xrdssi::test::Alert>       // Alert message type
        XrdSsiPbServiceType;

#endif

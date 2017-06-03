#include <iostream>

#include "XrdSsiException.h"
#include "TestSsiRequestProc.h"
#include "TestSsiProtobuf.h"

// This is for specialized private methods called by RequestProc::Execute to handle actions, alerts
// and metadata

template <>
void RequestProc<xrdssi::test::Request, xrdssi::test::Result>::ExecuteMetadata()
{
   const std::string metadata("Have some metadata!");
   SetMetadata(metadata.c_str(), metadata.size());
}


#ifndef __TEST_SSI_SERVICE_H
#define __TEST_SSI_SERVICE_H

#include <XrdSsi/XrdSsiService.hh>



/*
 * Service Object, obtained using GetService() method of the TestSsiServiceProvider factory
 */

class TestSsiService : public XrdSsiService
{
public:

// The pure abstract method ProcessRequest() is called when the client calls its ProcessRequest() method to hand off
// its request and resource objects. The client’s request and resource objects are transmitted to the server and passed
// into the service’s ProcessRequest() method.

virtual void ProcessRequest(XrdSsiRequest &reqRef, XrdSsiResource &resRef) override;

// Additional virtual methods:
//
// Attach(): optimize handling of detached requests
// Prepare(): perform preauthorization and resource optimization

         TestSsiService() {}
virtual ~TestSsiService() {}

};

#endif

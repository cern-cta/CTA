#ifndef __TEST_SSI_SERVICE_PROVIDER_H
#define __TEST_SSI_SERVICE_PROVIDER_H

#include <XrdSsi/XrdSsiProvider.hh>



/*
 * The service provider is used by xrootd to actually process client requests. The three methods that
 * you must implement in your derived XrdSsiProvider object are:
 *
 *     Init()          -- initialize the object for its intended use.
 *     GetService()    -- Called once (only) after initialisation to obtain an instance of an
 *                        XrdSsiService object.
 *     QueryResource() -- used to obtain the availability of a resource. Can be called whenever the
 *                        client asks for the resource status.
 */

class TestSsiServiceProvider : public XrdSsiProvider
{
public:

   // Init() is always called before any other method

   bool Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn, const std::string parms, int argc, char **argv) override;

   // The GetService() method must supply a service object

   XrdSsiService *GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold=256) override;

   // The QueryResource() method determines resource availability

   XrdSsiProvider::rStat QueryResource(const char *rName, const char *contact=0) override;

   // Constructor and destructor

            TestSsiServiceProvider() {}
   virtual ~TestSsiServiceProvider() {}
};

#endif

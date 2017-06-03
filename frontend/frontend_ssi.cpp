#include <iostream>

#include "XrdSsi/XrdSsiResponder.hh"
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"



/*
 * The XrdSsiResponder class knows how to safely interact with the request object. It allows handling asynchronous
 * requests such as cancellation, broken TCP connections, etc.
 *
 * The XrdSsiResponder class contains all the methods needed to interact with the request object: get the request,
 * release storage, send alerts, and post a response.
 *
 * The Request object will be bound to the XrdSsiResponder object via a call to XrdSsiResponder::BindRequest().
 * Once the relationship is established, you no longer need to keep a reference to the request object. The SSI
 * framework keeps track of the request object for you.
 *
 * RequestProc is a kind of agent object that the service object creates for each request that it receives.
 */

class RequestProc : public XrdSsiResponder
{
public:

void Execute()
{
   using namespace std;

   const string metadata("Have some metadata!");
   const string response("Have a response!");

   cerr << "Execute()" << endl;

   int reqLen;
   char *reqData = GetRequest(reqLen);

   // Parse the request

   ReleaseRequestBuffer(); // Optional

   // Perform the requested action

   // Optional: send alerts

   // Optional: send metadata ahead of the response

   SetMetadata(metadata.c_str(), metadata.size());

   // Send the response

   SetResponse(response.c_str(), response.size());
}

virtual void Finished(XrdSsiRequest &rqstR, const XrdSsiRespInfo &rInfo, bool cancel=false) override
{
   using namespace std;

   cerr << "Finished()" << endl;

   // Reclaim any allocated resources
}

         RequestProc() {}
virtual ~RequestProc() {}

};



/*
 * Service Object, obtained using GetService() method of the MyServiceProvider factory
 */

class MyService : public XrdSsiService
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

         MyService() {}
virtual ~MyService() {}

};




void MyService::ProcessRequest(XrdSsiRequest &reqRef, XrdSsiResource &resRef)
{
   using namespace std;

   cerr << "Called ProcessRequest()" << endl;

   RequestProc theProcessor;

   // Bind the processor to the request. This works because the
   // it inherited the BindRequest method from XrdSsiResponder.

   theProcessor.BindRequest(reqRef);

   // Execute the request, upon return the processor is deleted

   theProcessor.Execute();

   // Unbind the request from the responder (required)

   theProcessor.UnBindRequest();

   cerr << "ProcessRequest.UnBind()" << endl;
}



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

class MyServiceProvider : public XrdSsiProvider
{
public:

// Init() is always called before any other method

bool Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn, const std::string parms, int argc, char **argv) override;

// The GetService() method must supply a service object

XrdSsiService *GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold=256) override;

// The QueryResource() method determines resource availability

XrdSsiProvider::rStat QueryResource(const char *rName, const char *contact=0) override;

// Constructor and destructor

         MyServiceProvider() : initOK(false) {}
virtual ~MyServiceProvider() {}

private:

bool initOK;
};



bool MyServiceProvider::Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn, const std::string parms, int argc, char **argv)
{
   using namespace std;

   cerr << "Called Init(" << cfgFn << "," << parms << ")" << endl;

   // do some initialisation

   initOK = true;

   return initOK;
}



XrdSsiService* MyServiceProvider::GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold)
{
   using namespace std;

   cerr << "Called GetService(" << contact << "," << oHold << ")" << endl;

   XrdSsiService *ptr = new MyService;

   return ptr;
}



XrdSsiProvider::rStat MyServiceProvider::QueryResource(const char *rName, const char *contact)
{
   using namespace std;

   /*
    * The second argument, contact, is always (invalid?) for a server. It is only used by a client initiated
    * query for a resource at a particular endpoint.
    */

   cerr << "Called QueryResource(" << rName << "): ";

   // Return one of the following values:
   //
   // XrdSsiProvider::notPresent    The resource does not exist.
   // XrdSsiProvider::isPresent     The resource exists.
   // XrdSsiProvider::isPending     The resource exists but is not immediately available. (Useful only in clustered
   //                               environments where the resource may be immediately available on some other node.)

   if(strcmp(rName, "/mysql") == 0)
   {
      cerr << "isPresent" << endl;

      return XrdSsiProvider::isPresent;
   }
   else
   {
      cerr << "notPresent" << endl;

      return XrdSsiProvider::notPresent;
   }
}



/*
 * The service provider object is pointed to by the global pointer XrdSsiProviderServer which you
 * must define and set at library load time (i.e. it is a file level global static symbol).
 *
 * When your library is loaded, the XrdSsiProviderServer symbol is located in the library.
 * Initialization fails if the appropriate symbol cannot be found or it is a NULL pointer.
 */

XrdSsiProvider *XrdSsiProviderServer = new MyServiceProvider;

// XrdSsiProviderClient is instantiated and managed by the SSI library

//extern XrdSsiProvider *XrdSsiProviderClient;




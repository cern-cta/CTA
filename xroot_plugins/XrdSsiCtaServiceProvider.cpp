#include <iostream>

#include "XrdSsiPbAlert.h"
#include "XrdSsiCtaService.h"
#include "XrdSsiCtaServiceProvider.h"
#include "eos/messages/eos_messages.pb.h"



// The service provider object is pointed to by the global pointer XrdSsiProviderServer which you
// must define and set at library load time (i.e. it is a file level global static symbol).
//
// When your library is loaded, the XrdSsiProviderServer symbol is located in the library.
// Initialization fails if the appropriate symbol cannot be found or it is a NULL pointer.

XrdSsiProvider *XrdSsiProviderServer = new XrdSsiCtaServiceProvider;



bool XrdSsiCtaServiceProvider::Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn, const std::string parms, int argc, char **argv)
{
   using namespace std;

   cerr << "Called Init(" << cfgFn << "," << parms << ")" << endl;

   // do some initialisation

   return true;
}



XrdSsiService* XrdSsiCtaServiceProvider::GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold)
{
   using namespace std;

   cerr << "Called GetService(" << contact << "," << oHold << ")" << endl;

   XrdSsiService *ptr = new XrdSsiCtaService<eos::wfe::Notification, eos::wfe::Response, eos::wfe::Alert>;

   return ptr;
}



XrdSsiProvider::rStat XrdSsiCtaServiceProvider::QueryResource(const char *rName, const char *contact)
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

   if(strcmp(rName, "/ctafrontend") == 0)
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


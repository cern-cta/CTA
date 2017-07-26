#ifdef XRDSSI_DEBUG
#include <iostream>
#endif

#include <XrdSsi/XrdSsiProvider.hh>

#include "XrdSsiPbAlert.hpp"
#include "XrdSsiPbService.hpp"
#include "eos/messages/eos_messages.pb.h"



/*!
 * The Service Provider class.
 *
 * Instantiate a Service to process client requests.
 */

class XrdSsiCtaServiceProvider : public XrdSsiProvider
{
public:
            XrdSsiCtaServiceProvider() {
#ifdef XRDSSI_DEBUG
               std::cout << "[DEBUG] XrdSsiCtaServiceProvider() constructor" << std::endl;
#endif
            }
   virtual ~XrdSsiCtaServiceProvider() {
#ifdef XRDSSI_DEBUG
               std::cout << "[DEBUG] ~XrdSsiCtaServiceProvider() destructor" << std::endl;
#endif
   }

   //! Initialize the object for its intended use. This is always called before any other method.

   bool Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn, const std::string parms,
             int argc, char **argv) override;

   //! Called exactly once after initialisation to obtain an instance of an XrdSsiService object

   XrdSsiService *GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold=256) override;

   //! Determine resource availability. Can be called any time the client asks for the resource status.

   XrdSsiProvider::rStat QueryResource(const char *rName, const char *contact=0) override;
};



/*!
 * Global pointer to the Service Provider object.
 *
 * This must be defined at library load time (i.e. it is a file-level global static symbol). When the
 * shared library is loaded, XRootD initialization fails if the appropriate symbol cannot be found (or
 * it is a null pointer).
 */

XrdSsiProvider *XrdSsiProviderServer = new XrdSsiCtaServiceProvider;



/*!
 * Initialise the Service Provider
 */

bool XrdSsiCtaServiceProvider::Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn, const std::string parms, int argc, char **argv)
{
#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] Called Init(" << cfgFn << "," << parms << ")" << std::endl;
#endif

   // do some initialisation

   return true;
}



/*!
 * Instantiate a Service object
 */

XrdSsiService* XrdSsiCtaServiceProvider::GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold)
{
#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] Called GetService(" << contact << "," << oHold << ")" << std::endl;
#endif

   XrdSsiService *ptr = new XrdSsiPb::Service<eos::wfe::Notification, eos::wfe::Response, eos::wfe::Alert>;

   return ptr;
}



/*!
 * Query whether a resource exists on a server.
 *
 * @param[in]    rName      The resource name
 * @param[in]    contact    Used by client-initiated queries for a resource at a particular endpoint.
 *                          It is set to NULL for server-initiated queries.
 *
 * @retval    XrdSsiProvider::notPresent    The resource does not exist
 * @retval    XrdSsiProvider::isPresent     The resource exists
 * @retval    XrdSsiProvider::isPending     The resource exists but is not immediately available. (Useful
 *                                          only in clustered environments where the resource may be
 *                                          immediately available on some other node.)
 */

XrdSsiProvider::rStat XrdSsiCtaServiceProvider::QueryResource(const char *rName, const char *contact)
{
   // We only have one resource

   XrdSsiProvider::rStat resourcePresence = (strcmp(rName, "/ctafrontend") == 0) ?
                                            XrdSsiProvider::isPresent : XrdSsiProvider::notPresent;

#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] XrdSsiCtaServiceProvider::QueryResource(" << rName << "): "
             << ((resourcePresence == XrdSsiProvider::isPresent) ? "isPresent" : "notPresent")
             << std::endl;
#endif

   return resourcePresence;
}


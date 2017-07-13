#ifdef XRDSSI_DEBUG
#include <iostream>
#endif

#include "XrdSsiPbAlert.h"
#include "XrdSsiCtaService.h"
#include "XrdSsiCtaServiceProvider.h"
#include "eos/messages/eos_messages.pb.h"



/*!
 * The service provider object is pointed to by the global pointer XrdSsiProviderServer.
 *
 * The shared object must define and set this at library load time (i.e. it is a file-level global
 * static symbol). When the shared object is loaded, initialization fails if the appropriate symbol
 * cannot be found or it is a null pointer.
 */

XrdSsiProvider *XrdSsiProviderServer = new XrdSsiCtaServiceProvider;



bool XrdSsiCtaServiceProvider::Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn, const std::string parms, int argc, char **argv)
{
#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] Called Init(" << cfgFn << "," << parms << ")" << std::endl;
#endif

   // do some initialisation

   return true;
}



XrdSsiService* XrdSsiCtaServiceProvider::GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold)
{
#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] Called GetService(" << contact << "," << oHold << ")" << std::endl;
#endif

   XrdSsiService *ptr = new XrdSsiCtaService<eos::wfe::Notification, eos::wfe::Response, eos::wfe::Alert>;

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


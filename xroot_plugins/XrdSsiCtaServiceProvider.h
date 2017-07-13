#ifndef __XRD_SSI_CTA_SERVICE_PROVIDER_H
#define __XRD_SSI_CTA_SERVICE_PROVIDER_H

#include <XrdSsi/XrdSsiProvider.hh>

/*!
 * The Service Provider is used by XRootD to process client requests
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

   bool Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn, const std::string parms, int argc, char **argv) override;

   //! Called exactly once after initialisation to obtain an instance of an XrdSsiService object

   XrdSsiService *GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold=256) override;

   //! Determine resource availability. Can be called any time the client asks for the resource status.

   XrdSsiProvider::rStat QueryResource(const char *rName, const char *contact=0) override;
};

#endif

#ifndef __XRD_SSI_CTA_SERVICE_H
#define __XRD_SSI_CTA_SERVICE_H

#include <unistd.h> // for debugging

#include <XrdSsi/XrdSsiService.hh>
#include "XrdSsiPbRequestProc.h"



/*
 * Service Object, obtained using GetService() method of the XrdSsiCtaServiceProvider factory
 */

template <typename RequestType, typename MetadataType, typename AlertType>
class XrdSsiCtaService : public XrdSsiService
{
public:
            XrdSsiCtaService() {
#ifdef XRDSSI_DEBUG
               std::cout << "[DEBUG] XrdSsiCtaService() constructor" << std::endl;
#endif
            }
   virtual ~XrdSsiCtaService() {
#ifdef XRDSSI_DEBUG
               std::cout << "[DEBUG] ~XrdSsiCtaService() destructor" << std::endl;
#endif
            }

   // The pure abstract method ProcessRequest() is called when the client calls its ProcessRequest() method to hand off
   // its request and resource objects. The client's request and resource objects are transmitted to the server and passed
   // into the service's ProcessRequest() method.

   virtual void ProcessRequest(XrdSsiRequest &reqRef, XrdSsiResource &resRef) override;

   // Additional virtual methods:
   //
   // Attach(): optimize handling of detached requests
   // Prepare(): perform preauthorization and resource optimization
};



template <typename RequestType, typename MetadataType, typename AlertType>
void XrdSsiCtaService<RequestType, MetadataType, AlertType>::ProcessRequest(XrdSsiRequest &reqRef, XrdSsiResource &resRef)
{
#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] XrdSsiCtaService::ProcessRequest()" << std::endl;
#endif

   RequestProc<RequestType, MetadataType, AlertType> processor;

   // Bind the processor to the request. Inherits the BindRequest method from XrdSsiResponder.

   processor.BindRequest(reqRef);

   // Execute the request, upon return the processor is deleted

   processor.Execute();

   // Tell the framework we are done with the request object

   // WE NEED TO ENSURE THAT FINISHED() HAS BEEN CALLED BEFORE UNBIND
   sleep(1);
#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] XrdSsiCtaService::ProcessRequest(): Calling UnBindRequest()" << std::endl;
#endif

   // Unbind the request from the responder

   processor.UnBindRequest();
}

#endif

/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          XRootD SSI server-side Service object management
 * @copyright      Copyright 2017 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __XRD_SSI_PB_SERVICE_H
#define __XRD_SSI_PB_SERVICE_H

#include <XrdSsi/XrdSsiService.hh>
#include "XrdSsiPbRequestProc.hpp"

namespace XrdSsiPb {

/*!
 * Service Object.
 *
 * Obtained using GetService() method of the XrdSsiPbServiceProvider factory
 */

template <typename RequestType, typename MetadataType, typename AlertType>
class Service : public XrdSsiService
{
public:
            Service() {
#ifdef XRDSSI_DEBUG
               std::cout << "[DEBUG] Service() constructor" << std::endl;
#endif
            }
   virtual ~Service() {
#ifdef XRDSSI_DEBUG
               std::cout << "[DEBUG] ~Service() destructor" << std::endl;
#endif
            }

   /*!
    * Stop the Service.
    *
    * Requires some method to pass Finished(true) to in-flight Requests. This has been raised with Andy.
    * Currently not implemented as it would require tracking all in-flight Requests by the application
    * when this is really the job of the framework.
    */

   virtual bool Stop() override
   {
#ifdef XRDSSI_DEBUG
      std::cout << "[DEBUG] Service::Stop()" << std::endl;
#endif
      return false;
   }

   virtual void ProcessRequest(XrdSsiRequest &reqRef, XrdSsiResource &resRef) override;

   /*
    * The following additional virtual methods are available but have not been defined:
    *
    * Attach()     optimize handling of detached requests
    * Prepare()    perform preauthorization and resource optimization
    */
};



/*!
 * Bind a Request to a Request Processor and execute the Processor.
 *
 * The client calls its ProcessRequest() method to hand off its Request and Resource objects. The client's
 * Request and Resource objects are transmitted to the server and passed into the service's ProcessRequest()
 * method.
 */

template <typename RequestType, typename MetadataType, typename AlertType>
void Service<RequestType, MetadataType, AlertType>::ProcessRequest(XrdSsiRequest &reqRef, XrdSsiResource &resRef)
{
   XrdSsiPb::RequestProc<RequestType, MetadataType, AlertType> processor;

   // Bind the processor to the request. Inherits the BindRequest method from XrdSsiResponder.

#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] XrdSsiPbService::ProcessRequest(): Binding Processor to Request" << std::endl;
#endif

   processor.BindRequest(reqRef);

   // Execute the request, upon return the processor is deleted

   processor.Execute();

   // Tell the framework we have finished with the request object: unbind the request from the responder

#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] XrdSsiPbService::ProcessRequest(): Unbinding Processor from Request" << std::endl;
#endif

   processor.UnBindRequest();
}

} // namespace XrdSsiPb

#endif

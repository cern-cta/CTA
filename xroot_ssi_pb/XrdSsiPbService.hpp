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

#pragma once

#include <XrdSsi/XrdSsiService.hh>
#include <XrdSsi/XrdSsiEntity.hh>
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
               std::cerr << "[DEBUG] Service() constructor" << std::endl;
#endif
            }
   virtual ~Service() {
#ifdef XRDSSI_DEBUG
               std::cerr << "[DEBUG] ~Service() destructor" << std::endl;
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
      std::cerr << "[DEBUG] Service::Stop()" << std::endl;
#endif
      return false;
   }

   virtual void ProcessRequest(XrdSsiRequest &reqRef, XrdSsiResource &resRef) override;

   /*!
    * Perform Request pre-authorisation and/or resource optimisation.
    */
   virtual bool Prepare(XrdSsiErrInfo &eInfo, const XrdSsiResource &resource) override
   {
#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG] Service::Prepare():" << std::endl;
      std::cerr << "[DEBUG]    Resource name: " << resource.rName << std::endl
                << "[DEBUG]    Resource user: " << resource.rUser << std::endl
                << "[DEBUG]    Resource info: " << resource.rInfo << std::endl
                << "[DEBUG]    Hosts to avoid: " << resource.hAvoid << std::endl
                << "[DEBUG]    Affinity: ";

      switch(resource.affinity)
      {
         case XrdSsiResource::None:     std::cerr << "None" << std::endl; break;
         case XrdSsiResource::Default:  std::cerr << "Default" << std::endl; break;
         case XrdSsiResource::Weak:     std::cerr << "Weak" << std::endl; break;
         case XrdSsiResource::Strong:   std::cerr << "Strong" << std::endl; break;
         case XrdSsiResource::Strict:   std::cerr << "Strict" << std::endl; break;
      }

      std::cerr << "[DEBUG]    Resource options: "
                << (resource.rOpts & XrdSsiResource::Reusable ? "Resuable " : "")
                << (resource.rOpts & XrdSsiResource::Discard  ? "Discard"   : "")
                << std::endl;
#endif
      if(resource.client == nullptr || resource.client->name == nullptr)
      {
         eInfo.Set("Service::Prepare(): XRootD client name is not set. "
            "Possible misconfiguration of the KRB5 or SSS keyfile.", EACCES);
         return false;
      }

#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG]    Resource client name: " << resource.client->name << std::endl;
#endif

      return true;
   }

   /*!
    * Receive notification that a Request has been attached.
    *
    * This is required only if the service needs to make decisions on how to run a request based on whether
    * it is attached or detached. See Sect. 3.3.1 "Detached Requests" in the XRootD SSI documentation.
    */
   virtual bool Attach(XrdSsiErrInfo &eInfo, const std::string &handle,
                       XrdSsiRequest &reqRef, XrdSsiResource *resp) override
   {
#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG] Service::Attach()" << std::endl;
#endif
      return true;
   }
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
   XrdSsiPb::RequestProc<RequestType, MetadataType, AlertType> processor(resRef);

   // Bind the processor to the request. Inherits the BindRequest method from XrdSsiResponder.

#ifdef XRDSSI_DEBUG
   std::cerr << "[DEBUG] XrdSsiPbService::ProcessRequest(): Binding Processor to Request" << std::endl;
#endif
   processor.BindRequest(reqRef);

   // Execute the request, upon return the processor is deleted

   processor.Execute();

   // Tell the framework we have finished with the request object: unbind the request from the responder

#ifdef XRDSSI_DEBUG
   std::cerr << "[DEBUG] XrdSsiPbService::ProcessRequest(): Unbinding Processor from Request" << std::endl;
#endif

   processor.UnBindRequest();
}

} // namespace XrdSsiPb


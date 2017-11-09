/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          XRootD SSI client-side Service object management
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

#ifdef XRDSSI_DEBUG
#include <iostream>
#endif

#include <XrdSsi/XrdSsiProvider.hh>
#include <XrdSsi/XrdSsiService.hh>
#include "XrdSsiPbException.hpp"
#include "XrdSsiPbRequest.hpp"



/*!
 * XrdSsiProviderClient is instantiated and managed by the SSI library
 */
extern XrdSsiProvider *XrdSsiProviderClient;



namespace XrdSsiPb {

// Constants

const unsigned int DefaultResponseBufferSize = 1024;       //!< Default size for the response buffer in bytes
const unsigned int DefaultRequestTimeout     = 15;         //!< Default XRootD request timeout in secs
// Better to get the default from XRD_REQUESTTIMEOUT if it is not set explicitly?
// Decide on order of precedence for setting request timeout
// What about setting stream timeout?



/*!
 * Convenience object to manage the XRootD SSI service on the client side
 */
template <typename RequestType, typename MetadataType, typename AlertType>
class ServiceClientSide
{
public:
   ServiceClientSide(const std::string &endpoint, const std::string &resource,
                     unsigned int response_bufsize = DefaultResponseBufferSize,
                     unsigned int request_tmo      = DefaultRequestTimeout);

   virtual ~ServiceClientSide();

   std::future<void> Send(const RequestType &request, MetadataType &response);

private:
   XrdSsiResource m_resource;            //!< Requests are bound to this resource. As the resource is
                                         //!< reusable, the lifetime of the resource is the same as the
                                         //!< lifetime of the Service object.

   XrdSsiService *m_server_ptr;          //!< Pointer to XRootD Server object

   unsigned int   m_response_bufsize;    //!< Buffer size for responses from the XRootD SSI server
   unsigned int   m_request_tmo;         //!< Timeout for a response from the server
};



/*!
 * Client-side Service Constructor
 */
template <typename RequestType, typename MetadataType, typename AlertType>
ServiceClientSide<RequestType, MetadataType, AlertType>::
ServiceClientSide(const std::string &endpoint, const std::string &resource,
                  unsigned int response_bufsize, unsigned int request_tmo) :
   m_resource(resource),
   m_response_bufsize(response_bufsize),
   m_request_tmo(request_tmo)
{
#ifdef XRDSSI_DEBUG
   std::cerr << "[DEBUG] ServiceClientSide() constructor" << std::endl;
#endif
   XrdSsiErrInfo eInfo;

   // Set Resource options

   m_resource.rOpts = XrdSsiResource::Reusable;     //< Resource context may be cached and is reusable

   // Other possibly-useful options:
   //
   // (For specifying the tapeserver callback)
   //
   // XrdSsiResource::rInfo
   //     This option allows you to send additional out-of-band information to the server that will be executing
   //     the request. The information should be specified in CGI format (i.e. key=value[&key=value[...]]). This
   //     information is supplied to the server-side service in its corresponding request resource object. Note
   //     that restrictions apply for reusable resources.
   //
   // XrdSsiResource::rUser
   //     This is an arbitrary string that is meant to further identify the request. The SSI framework normally
   //     uses this information to tag log messages. It is also supplied to the server-side service in its
   //     corresponding request resource object.

   // Get the Service pointer

   if(!(m_server_ptr = XrdSsiProviderClient->GetService(eInfo, endpoint)))
   {
      throw XrdSsiException(eInfo);
   }
}



/*!
 * Client-side Service Destructor
 */
template <typename RequestType, typename MetadataType, typename AlertType>
ServiceClientSide<RequestType, MetadataType, AlertType>::~ServiceClientSide()
{
#ifdef XRDSSI_DEBUG
   std::cerr << "[DEBUG] ServiceClientSide() destructor" << std::endl;

   if(!m_server_ptr->Stop())
   {
      // As there is no way to get a list of in-flight Requests from the Service, the current Stop()
      // implementation simply returns false. i.e. there is no way to know if we are shutting down
      // cleanly or not.

      std::cerr << "[WARNING] ServiceClientSide object was destroyed before shutting down the Service." << std::endl;
   }
#endif
}



/*!
 * Send a Request to the Service
 *
 * @param[in]     request
 * @param[out]    response
 *
 * @returns       future for Data/Stream requests. This return value can be ignored for Metadata-only Responses.
 */
template <typename RequestType, typename MetadataType, typename AlertType>
std::future<void> ServiceClientSide<RequestType, MetadataType, AlertType>::Send(const RequestType &request, MetadataType &response)
{
   // Instantiate the Request object
   auto request_ptr = new Request<RequestType, MetadataType, AlertType>(request, m_response_bufsize, m_request_tmo);
   auto metadata_future = request_ptr->GetMetadataFuture();
   auto data_future = request_ptr->GetDataFuture();

   // Transfer ownership of the Request to the Service object.
   m_server_ptr->ProcessRequest(*request_ptr, m_resource);

   // After ProcessRequest() returns, it is safe for request_ptr to go out-of-scope, as the framework
   // will handle deletion of the Request object. It is also safe to delete the Resource; in our case
   // we do not need to as we created a reusable Resource.

   // Wait synchronously for the framework to return its Response (or an exception)
   response = metadata_future.get();

   // Return the future for Data/Stream requests
   return data_future;
}

} // namespace XrdSsiPb


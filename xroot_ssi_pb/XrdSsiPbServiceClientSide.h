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

#ifndef __XRD_SSI_PB_SERVICE_CLIENT_SIDE_H
#define __XRD_SSI_PB_SERVICE_CLIENT_SIDE_H

#include <unistd.h> // sleep()

#include <XrdSsi/XrdSsiProvider.hh>
#include <XrdSsi/XrdSsiService.hh>
#include "XrdSsiPbException.h"
#include "XrdSsiPbRequest.h"



//! XrdSsiProviderClient is instantiated and managed by the SSI library

extern XrdSsiProvider *XrdSsiProviderClient;



namespace XrdSsiPb {

// Constants

const unsigned int DefaultResponseBufferSize = 2097152;    //!< Default size for the response buffer in bytes = 2 Mb
const unsigned int DefaultServerTimeout      = 15;         //!< Maximum XRootD reply timeout in secs
const unsigned int DefaultShutdownTimeout    = 0;          //!< Maximum time to wait for the Service to shut down in secs



/*!
 * Convenience object to manage the XRootD SSI service on the client side
 */

template <typename RequestType, typename MetadataType, typename AlertType>
class ServiceClientSide
{
public:
   // Service object constructor for the client side

   ServiceClientSide() = delete;

   ServiceClientSide(const std::string &hostname, unsigned int port, const std::string &resource,
                     unsigned int response_bufsize = DefaultResponseBufferSize,
                     unsigned int server_tmo       = DefaultServerTimeout);

   // Service object destructor for the client side

   virtual ~ServiceClientSide();

   // Request shutdown of the Service

   bool Shutdown(int shutdown_tmo = DefaultShutdownTimeout);

   // Send a Request to the Service

   MetadataType Send(const RequestType &request);

private:
   bool           m_is_running;          //!< Is the service running?
   XrdSsiResource m_resource;            //!< Requests are bound to this resource. As the resource is
                                         //!< reusable, the lifetime of the resource is the same as the
                                         //!< lifetime of the Service object.

   XrdSsiService *m_server_ptr;          //!< Pointer to XRootD Server object

   unsigned int   m_response_bufsize;    //!< Buffer size for responses from the XRootD SSI server
   unsigned int   m_server_tmo;          //!< Timeout for a response from the server

   std::string    m_request_str;         //!< Buffer for sending a request
};



//! Constructor

template <typename RequestType, typename MetadataType, typename AlertType>
ServiceClientSide<RequestType, MetadataType, AlertType>::
ServiceClientSide(const std::string &hostname, unsigned int port, const std::string &resource,
                          unsigned int response_bufsize,
                          unsigned int server_tmo) :
   m_is_running(false),
   m_resource(resource),
   m_response_bufsize(response_bufsize),
   m_server_tmo(server_tmo)
{
#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] ServiceClientSide() constructor" << std::endl;
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

   if(!(m_server_ptr = XrdSsiProviderClient->GetService(eInfo, hostname + ":" + std::to_string(port))))
   {
      throw XrdSsiException(eInfo);
   }

   m_is_running = true;
}



//! Destructor

template <typename RequestType, typename MetadataType, typename AlertType>
ServiceClientSide<RequestType, MetadataType, AlertType>::~ServiceClientSide()
{
#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] ServiceClientSide() destructor" << std::endl;
#endif

#if 0
   // Default Stop() implementation is return false;
   // i.e. XRootD SSI makes no attempt to shut down!

   // If the Service has not been shut down, make one last valiant effort to do so

   if(m_is_running && !m_server_ptr->Stop())
   {
      std::cerr << "ServiceClientSide object was destroyed before all Requests have been processed. "
                << "This will cause a memory leak. Call Shutdown() before deleting the object." << std::endl;
   }
#endif
}



/*!
 * Shut down the XRootD SSI Service.
 *
 * A Service object can only be deleted after all requests handed to the object have completed. It is
 * possible to take back control of Requests by calling each Request's Finished() method: this cancels
 * the Request and the object can then be deleted.
 *
 * However, the current interface does not provide a way to recover a list of outstanding XrdSsi
 * Requests. I have raised this with Andy.
 *
 * @param[in]    shutdown_tmo    No. of seconds to wait before giving up
 *
 * @retval       true            The Service has been shut down
 * @retval       false           The Service has not been shut down
 */

template <typename RequestType, typename MetadataType, typename AlertType>
bool ServiceClientSide<RequestType, MetadataType, AlertType>::
Shutdown(int shutdown_tmo)
{
   // Trivial case: server has already been shut down

   if(!m_is_running) return true;

#ifdef XRDSSI_DEBUG
   std::cerr << "[DEBUG] ServiceClientSide::Shutdown():" << std::endl;
   std::cerr << "Shutting down XRootD SSI service...";
#endif

   // The XrdSsiService object cannot be explicitly deleted. The Stop() method deletes the object if
   // it is safe to do so.

   while(!m_server_ptr->Stop() && shutdown_tmo--)
   {
      sleep(1);
#ifdef XRDSSI_DEBUG
      std::cerr << ".";
#endif
   }

   if(shutdown_tmo > 0)
   {
      m_is_running = false;

#ifdef XRDSSI_DEBUG
      std::cerr << "done." << std::endl;
#endif
   }
   else
   {
#ifdef XRDSSI_DEBUG
      std::cerr << "failed." << std::endl;
#endif
   }

   return !m_is_running;
}



/*!
 * Send a Request to the Service
 */

template <typename RequestType, typename MetadataType, typename AlertType>
MetadataType ServiceClientSide<RequestType, MetadataType, AlertType>::Send(const RequestType &request)
{
   // Check service is up

   if(!m_is_running)
   {
      throw XrdSsiException("Service has been stopped");
   }

   // Serialize the Request

   if(!request.SerializeToString(&m_request_str))
   {
      throw PbException("request.SerializeToString() failed");
   }

   // Instantiate the Request object

   auto request_ptr = new Request<RequestType, MetadataType, AlertType>(m_request_str, m_response_bufsize, m_server_tmo);

   auto future_response = request_ptr->GetFuture();

   // Transfer ownership of the Request to the Service object. The framework will handle deletion of the
   // Request object. Although it is safe to delete the Resource after ProcessRequest() returns, we are
   // creating reusable Resources, so no need to delete it.

   m_server_ptr->ProcessRequest(*request_ptr, m_resource);

   // Wait synchronously for the framework to return its response

   auto response = future_response.get();

   return response;
}

} // namespace XrdSsiPb

#endif

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



// Constants

const unsigned int DefaultResponseBufferSize = 2097152;    // Default size for the response buffer in bytes = 2 Mb
const unsigned int DefaultServerTimeout      = 15;         // Maximum XRootD reply timeout in secs
const unsigned int DefaultShutdownTimeout    = 30;         // Maximum time to wait for the Service to shut down in secs



// XrdSsiProviderClient is instantiated and managed by the SSI library

extern XrdSsiProvider *XrdSsiProviderClient;



// Convenience object to manage the XRootD SSI service on the client side

template <typename RequestType, typename MetadataType, typename AlertType>
class XrdSsiPbServiceClientSide
{
public:
   // Service object constructor for the client side

   XrdSsiPbServiceClientSide() = delete;

   XrdSsiPbServiceClientSide(const std::string &hostname, unsigned int port, const std::string &resource,
                           unsigned int response_bufsize = DefaultResponseBufferSize,
                           unsigned int server_tmo       = DefaultServerTimeout,
                           unsigned int shutdown_tmo     = DefaultShutdownTimeout) :
      m_resource(resource),
      m_response_bufsize(response_bufsize),
      m_server_tmo(server_tmo),
      m_shutdown_tmo(shutdown_tmo)
   {
      XrdSsiErrInfo eInfo;

      // Resource context may be cached and is reusable

      //m_resource.rOpts = XrdSsiResource::Reusable;

      // Get the Service pointer

      if(!(m_server_ptr = XrdSsiProviderClient->GetService(eInfo, hostname + ":" + std::to_string(port))))
      {
         throw XrdSsiPbException(eInfo);
      }
   }

   // Service object destructor for the client side

   virtual ~XrdSsiPbServiceClientSide();

   // Send a Request to the Service

   void send(const RequestType &request);

private:
   XrdSsiResource m_resource;            // Requests are bound to this resource. As the resource is
                                         // reusable, the lifetime of the resource is the same as the
                                         // lifetime of the Service object.

   XrdSsiService *m_server_ptr;          // Pointer to XRootD Server object

   unsigned int   m_response_bufsize;    // Buffer size for responses from the XRootD SSI server
   unsigned int   m_server_tmo;          // Timeout for a response from the server
   unsigned int   m_shutdown_tmo;        // Timeout to shut down the server in the destructor

   std::string    m_request_str;         // Buffer for sending a request
};



// Destructor

template <typename RequestType, typename MetadataType, typename AlertType>
XrdSsiPbServiceClientSide<RequestType, MetadataType, AlertType>::~XrdSsiPbServiceClientSide()
{
#ifdef XRDSSI_DEBUG
   std::cerr << "Stopping XRootD SSI service...";
#endif

   // The XrdSsiService object cannot be explicitly deleted. The Stop() method deletes the object if
   // it is safe to do so.

   while(!m_server_ptr->Stop() && m_shutdown_tmo--)
   {
      sleep(1);
#ifdef XRDSSI_DEBUG
      std::cerr << ".";
#endif
   }

   if(m_shutdown_tmo > 0)
   {
#ifdef XRDSSI_DEBUG
      std::cerr << "done." << std::endl;
#endif
   }
   else
   {
      // Timeout reached and there are still outstanding requests
      //
      // A service object can only be deleted after all requests handed to the object have completed.
      // It is possible to take back control of Requests by calling each Request's Finished() method:
      // this cancels the Request and the object can then be deleted.
      //
      // However, the current interface does not provide a way to recover a list of outstanding
      // Requests. I have raised this with Andy.
      //
      // Until this is solved, deleting the Service may leak memory, so don't delete Service objects
      // unless the client is shutting down!

#ifdef XRDSSI_DEBUG
      std::cerr << "failed." << std::endl;
#endif
   }
}



// Send a Request to the Service

template <typename RequestType, typename MetadataType, typename AlertType>
void XrdSsiPbServiceClientSide<RequestType, MetadataType, AlertType>::send(const RequestType &request)
{
   // Serialize the request object

   if(!request.SerializeToString(&m_request_str))
   {
      throw XrdSsiPbException("request.SerializeToString() failed");
   }

   // Requests are always executed in the context of a service. They need to correspond to what the service allows.

   XrdSsiRequest *request_ptr =
      new XrdSsiPbRequest<RequestType, MetadataType, AlertType>(m_request_str, m_response_bufsize, m_server_tmo);

   // Transfer ownership of the request to the service object
   // TestSsiRequest handles deletion of the request buffer, so we can allow the pointer to go out-of-scope

   m_server_ptr->ProcessRequest(*request_ptr, m_resource);

   // Note: for a non-reusable resource, it is safe to delete it after ProcessRequest() returns. We
   // have created reusable resources, so no need to delete it.
   //
   // Possible useful options:
   //
   // For specifying the tapeserver callback:
   // XrdSsiResource::rInfo
   // This option allows you to send additional out-of-band information to the
   // server that will be executing the request. The information should be specified
   // in CGI format (i.e. key=value[&key=value[...]]). This information is supplied
   // to the server-side service in its corresponding request resource object. Note
   // that restrictions apply for reusable resources.
   //
   // XrdSsiResource::rUser
   // This is an arbitrary string that is meant to further identify the request. The
   // SSI framework normally uses this information to tag log messages. It is also
   // supplied to the server-side service in its corresponding request resource
   // object.
}

#endif

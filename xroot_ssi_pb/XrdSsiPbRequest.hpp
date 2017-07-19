/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          XRootD SSI Request class
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

#ifndef __XRD_SSI_PB_REQUEST_H
#define __XRD_SSI_PB_REQUEST_H

#include <future>

#include <XrdSsi/XrdSsiRequest.hh>

namespace XrdSsiPb {

/*!
 * Request Callback class
 *
 * The client should specialize on this class for the Alert Response type. This permits an arbitrary
 * number of Alert messages to be sent before each Response. These can be used for any purpose defined
 * by the client and server, for example writing messages to the client log.
 */

template<typename CallbackArg>
class RequestCallback
{
public:
   void operator()(const CallbackArg &arg);
};



/*!
 * Request class
 */

template <typename RequestType, typename MetadataType, typename AlertType>
class Request : public XrdSsiRequest
{
public:
   Request(const std::string &buffer_str, unsigned int response_bufsize, uint16_t timeout) :
      m_request_bufptr(buffer_str.c_str()),
      m_request_bufsize(buffer_str.size()),
      m_response_bufsize(response_bufsize)
   {
#ifdef XRDSSI_DEBUG
      std::cout << "[DEBUG] Request constructor: "
                << "Response buffer size = " << m_response_bufsize
                << ", Response timeout = " << timeout << std::endl;
#endif

      // Set response timeout

      SetTimeOut(timeout);
   }

   virtual ~Request() 
   {
#ifdef XRDSSI_DEBUG
      std::cout << "[DEBUG] ~Request destructor" << std::endl;
#endif
   }

   /*!
    * The implementation of GetRequest() must create request data, save it in some manner, and provide
    * it to the framework.
    *
    * Optionally also define the RelRequestBuffer() method to clean up when the framework no longer
    * needs access to the data. The thread used to initiate a request may be the same one used in the
    * GetRequest() call.
    */

   virtual char *GetRequest(int &reqlen) override
   {
      reqlen = m_request_bufsize;
      return const_cast<char*>(m_request_bufptr);
   }

   virtual bool ProcessResponse(const XrdSsiErrInfo &eInfo, const XrdSsiRespInfo &rInfo) override;

   virtual XrdSsiRequest::PRD_Xeq ProcessResponseData(const XrdSsiErrInfo &eInfo, char *buff, int blen, bool last) override;

   virtual void Alert(XrdSsiRespInfoMsg &alert_msg) override;

   /*!
    * Return the future associated with this object's promise
    */

   auto GetFuture() { return m_promise.get_future(); }

private:
   const char *m_request_bufptr;                //!< Pointer to the Request buffer
   int         m_request_bufsize;               //!< Size of the Request buffer
   char       *m_response_bufptr;               //!< Pointer to the Response buffer
   int         m_response_bufsize;              //!< Size of the Response buffer

   std::promise<MetadataType> m_promise;        //!< Promise a reply of Metadata type
   RequestCallback<AlertType> AlertCallback;    //!< Callback for Alerts
};



/*!
 * Process Responses from the server
 *
 * Requests are sent to the server asynchronously via the service object. ProcessResponse() informs
 * the Request object on the client side if it completed or failed.
 */

template<typename RequestType, typename MetadataType, typename AlertType>
bool Request<RequestType, MetadataType, AlertType>::ProcessResponse(const XrdSsiErrInfo &eInfo, const XrdSsiRespInfo &rInfo)
{
#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] ProcessResponse(): response type = " << rInfo.State() << std::endl;
#endif

   try {
      switch(rInfo.rType) {

      // Handle errors in the XRootD framework (e.g. no response from server)

      case XrdSsiRespInfo::isError:     throw XrdSsiException(eInfo);

      // To implement detached requests, add another callback type which saves the handle

      case XrdSsiRespInfo::isHandle:    throw XrdSsiException("Detached requests are not implemented.");

      // To implement file requests, add another callback type

      case XrdSsiRespInfo::isFile:      throw XrdSsiException("File requests are not implemented.");

      // To implement stream requests, add another callback type

      case XrdSsiRespInfo::isStream:    throw XrdSsiException("Stream requests are not implemented.");

      // Metadata-only responses and data responses

      case XrdSsiRespInfo::isNone:
      case XrdSsiRespInfo::isData:
         // Check for metadata. The Protocol Buffers Metadata format is used for simple Responses. It
         // can also be used as the header for large asynchronous data transfers or streaming data.

         int metadata_len;
         const char *metadata_buffer = GetMetadata(metadata_len);

         if(metadata_len > 0)
         {
            // Deserialize the metadata

            MetadataType metadata;

            if(metadata.ParseFromArray(metadata_buffer, metadata_len))
            {
               m_promise.set_value(metadata);
            }
            else
            {
               throw PbException("metadata.ParseFromArray() failed");
            }
         }

         if(rInfo.rType == XrdSsiRespInfo::isNone)
         {
            // If this is a metadata-only response, we are done

            Finished();
            delete this;
            break;
         }
         else // XrdSsiRespInfo::isData
         {
            // Allocate response buffer

            m_response_bufptr = new char[m_response_bufsize];

            // Read the first chunk of data into the buffer, and pass it to ProcessResponseData()

            GetResponseData(m_response_bufptr, m_response_bufsize);
         }
      }
   } catch(std::exception &ex) {
      // Use the exception to fulfil the promise

      m_promise.set_exception(std::current_exception());

      // Return control of the object to the calling thread and delete rInfo

      Finished();

      // It is now safe to delete the Request object (implies that the pointer on the calling side will
      // never refer to it again and the destructor of the base class doesn't access any class members)

      delete this;
   } catch(...) {
      // set_exception() above can also throw an exception...

      Finished();
      delete this;
   }

   return true;
}



/*!
 * Process Response Data.
 *
 * Responses will be implemented as a binary blob or binary stream, which is received one chunk at a time.
 * The chunk size is defined when the Request object is instantiated (see m_response_bufsize above).
 *
 * The format of Responses is not defined by a Protocol Buffer, as this would require us to read the entire
 * Response before parsing it, which would defeat the point of reading the Response in chunks. How the
 * Response is parsed is up to the client, but two possibilities are:
 *
 * 1. The format is defined in the metadata which is a kind of header
 * 2. The format is record-based, and each record is a protocol buffer
 *
 * ProcessResponseData() is called either by GetResponseData(), or asynchronously at any time for data
 * streams.
 */

template<typename RequestType, typename MetadataType, typename AlertType>
XrdSsiRequest::PRD_Xeq Request<RequestType, MetadataType, AlertType>
             ::ProcessResponseData(const XrdSsiErrInfo &eInfo, char *response_bufptr, int response_buflen, bool is_last)
{
   // The buffer length can be 0 if the response is metadata only

   if(response_buflen != 0)
   {
      // Handle one block of response data

      // TO DO: Provide an interface to the client to read a chunk of data
   }

   if(!is_last)
   {
      // If there is more datam get the next chunk

      GetResponseData(response_bufptr, m_response_bufsize);
   }
   else
   {
      // No more data, so clean up

      delete[] response_bufptr;

      // If Request objects are uniform, we could re-use them instead of deleting them, to avoid the
      // overhead of repeated object creation. This would require a more complex Request factory. For
      // now we just delete.

      Finished();
      delete this;
   }

   // Indicate what type of post-processing is required (normal in this case)

   return XrdSsiRequest::PRD_Normal;

   // If the client is resource-limited and can't handle the queue at this time,
   // return XrdSsiRequest::PRD_Hold;
}



/*!
 * Deserialize Alert messages and call the Alert callback
 */

template<typename RequestType, typename MetadataType, typename AlertType>
void Request<RequestType, MetadataType, AlertType>::Alert(XrdSsiRespInfoMsg &alert_msg)
{
   try
   {
      // Get the Alert

      int alert_len;
      char *alert_buffer = alert_msg.GetMsg(alert_len);

      // Deserialize the Alert

      AlertType alert;

      if(alert.ParseFromArray(alert_buffer, alert_len))
      {
         AlertCallback(alert);
      }
      else
      {
         throw PbException("alert.ParseFromArray() failed");
      }
   }
   catch(std::exception &ex)
   {
      m_promise.set_exception(std::current_exception());
   }
   // catch(...) {} ?
   // set_exception() can also throw()...

   // Recycle the message to free memory

   alert_msg.RecycleMsg();
}

} // namespace XrdSsiPb

#endif

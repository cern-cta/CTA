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

#pragma once

#include <future>
#include <XrdSsi/XrdSsiRequest.hh>
#include <XrdSsi/XrdSsiStream.hh>

#ifdef XRDSSI_DEBUG
#include <XrdSsiPbDebug.hpp>
#endif

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
   Request(const RequestType &request, unsigned int response_bufsize, uint16_t request_timeout);

   virtual ~Request() {
#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG] ~Request destructor" << std::endl;
#endif
   }

   /*!
    * The implementation of GetRequest() must create request data, save it in some manner, and provide
    * it to the framework.
    */
   virtual char *GetRequest(int &reqlen) override
   {
      reqlen = m_request_str.size();
      return const_cast<char*>(m_request_str.c_str());
   }

   // Optionally also define the RelRequestBuffer() method to clean up when the framework no longer
   // needs access to the data. The thread used to initiate a request may be the same one used in the
   // GetRequest() call.

   virtual bool ProcessResponse(const XrdSsiErrInfo &eInfo, const XrdSsiRespInfo &rInfo) override;

   virtual XrdSsiRequest::PRD_Xeq ProcessResponseData(const XrdSsiErrInfo &eInfo, char *response_bufptr, int response_buflen, bool is_last) override;

   virtual void Alert(XrdSsiRespInfoMsg &alert_msg) override;

   /*!
    * Return the future associated with this object's Metadata promise
    */
   std::future<MetadataType> GetMetadataFuture() { return m_metadata_promise.get_future(); }

   /*!
    * Return the future associated with this object's Data/Stream promise
    */
   std::future<void> GetDataFuture() { return m_data_promise.get_future(); }

private:
   void ProcessResponseMetadata();

   std::string                 m_request_str;         //!< Request buffer
   std::unique_ptr<char[]>     m_response_buffer;     //!< Pointer to storage for Data responses
   char                       *m_response_bufptr;     //!< Pointer to the Response buffer
   int                         m_response_bufsize;    //!< Size of the Response buffer

   std::promise<MetadataType>  m_metadata_promise;    //!< Promise a reply of Metadata type
   std::promise<void>          m_data_promise;        //!< Promise a data or stream response

   RequestCallback<AlertType>  AlertCallback;         //!< Callback for Alert messages

   /*!
    * Callback for Data Responses
    *
    * This method needs to be specialised for any client which can accept Data or Stream payloads.
    *
    * @param[in,out]    post_process       Indicate what type of post-processing should be done by the XrdSsi framework:
    *                      XrdSsiRequest::PRD_Normal (default): normal post-processing
    *                      XrdSsiRequest::PRD_Hold: client is resource-limited and can't handle the queue at this time
    * @param[in]        response_bufptr    Pointer to newly-arrived data buffer
    * @param[in]        response_buflen    Length of response_bufptr
    */
   void DataCallback(XrdSsiRequest::PRD_Xeq &post_process, char *response_bufptr, int response_buflen) {
      throw XrdSsiException("Data/Stream Responses are not implemented.");
   }
};



/*!
 * Request constructor
 */
template<typename RequestType, typename MetadataType, typename AlertType>
Request<RequestType, MetadataType, AlertType>::
Request(const RequestType &request, unsigned int response_bufsize, uint16_t request_timeout) :
   m_response_bufsize(response_bufsize)
{
#ifdef XRDSSI_DEBUG
   std::cerr << "[DEBUG] Request constructor: "
             << "Response buffer size = " << m_response_bufsize
             << " bytes, request timeout = " << request_timeout << std::endl;
#endif
   // Set XRootD request timeout
   SetTimeOut(request_timeout);

   // Serialize the Request
   if(!request.SerializeToString(&m_request_str))
   {
      throw PbException("request.SerializeToString() failed");
   }
}



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
   std::cerr << "[DEBUG] ProcessResponse(): response type = " << rInfo.State() << std::endl;
#endif

   try {
      switch(rInfo.rType) {

      // Data and Metadata responses

      case XrdSsiRespInfo::isData:
         // Process Metadata
         ProcessResponseMetadata();

         // Process Data
         if(rInfo.blen > 0)
         {
            // For Data responses, we need to allocate the buffer to receive the data
            m_response_buffer = std::unique_ptr<char[]>(new char[m_response_bufsize]);
            m_response_bufptr = m_response_buffer.get();

            // Process Data Response: copy one chunk of data into the buffer, then call ProcessResponseData()
            GetResponseData(m_response_bufptr, m_response_bufsize);
         } else { // Response is Metadata-only

            // Return control of the object to the calling thread and delete rInfo
            Finished();

            // It is now safe to delete the Request object (implies that the pointer on the calling side will
            // never refer to it again and the destructor of the base class doesn't access any class members)

            delete this;
         }
         break;

      // Stream response

      case XrdSsiRespInfo::isStream:
         // Process Metadata
         ProcessResponseMetadata();

         // For Stream responses, we need to allocate the buffer to receive the data
         m_response_buffer = std::unique_ptr<char[]>(new char[m_response_bufsize]);
         m_response_bufptr = m_response_buffer.get();

         // Process Stream Response: copy one chunk of data into the buffer, then call ProcessResponseData()
         GetResponseData(m_response_bufptr, m_response_bufsize);

         break;

      // Handle errors in the XRootD framework (e.g. no response from server)

      case XrdSsiRespInfo::isError:     throw XrdSsiException(eInfo);

      // To implement detached requests, add another callback type which saves the handle

      case XrdSsiRespInfo::isHandle:    throw XrdSsiException("Detached requests are not implemented.");

      // To implement file requests, add another callback type

      case XrdSsiRespInfo::isFile:      throw XrdSsiException("File requests are not implemented.");

      // Handle invalid responses

      case XrdSsiRespInfo::isNone:
      default:                          throw XrdSsiException("Invalid Response.");
      }
   } catch(std::exception &ex) {
      // Use the exception to fulfil the promise

      m_metadata_promise.set_exception(std::current_exception());

      Finished();
      delete this;
   }

   return true;
}



/*!
 * Process Response Metadata
 *
 * A Response can (optionally) contain Metadata. This can be used for simple responses (e.g. status
 * code, short message) or as the header for large asynchronous data transfers or streaming data.
 */
template<typename RequestType, typename MetadataType, typename AlertType>
void Request<RequestType, MetadataType, AlertType>::ProcessResponseMetadata()
{
   int metadata_len;
   const char *metadata_buffer = GetMetadata(metadata_len);
#ifdef XRDSSI_DEBUG
   std::cerr << "[DEBUG] ProcessResponseMetadata(): received " << metadata_len << " bytes" << std::endl;
#endif
#if 0
   // Show metadata contents as a string of bytes
   std::cerr << "[DEBUG] ProcessResponseMetadata(): ";
   for(int i = 0; i < metadata_len; ++i)
   {
      std::cerr << "<" << static_cast<int>(*(metadata_buffer+i)) << ">";
   }
   std::cerr << std::endl;
#endif

   // Deserialize the metadata

   MetadataType metadata;

   if(metadata.ParseFromArray(metadata_buffer, metadata_len))
   {
      m_metadata_promise.set_value(metadata);
   }
   else
   {
      throw PbException("metadata.ParseFromArray() failed");
   }
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
#ifdef XRDSSI_DEBUG
   std::cerr << "[DEBUG] ProcessResponseData(): received " << response_buflen << " bytes" << std::endl;
#endif

   XrdSsiRequest::PRD_Xeq post_process = XrdSsiRequest::PRD_Normal;

   // The buffer length is set to -1 if an error occurred setting up the response
   if(response_buflen == -1)
   {
      throw XrdSsiException(eInfo);
   }

   // The buffer length can be 0 if the response is metadata only
   if(response_buflen != 0)
   {
      DataCallback(post_process, response_bufptr, response_buflen);
   }

   if(is_last) // No more data to come
   {
#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG] ProcessResponseData: done" << std::endl;
#endif
      // Clean up

      // Set the data promise
      m_data_promise.set_value();

      // If Request objects are uniform, we could re-use them instead of deleting them, to avoid the
      // overhead of repeated object creation. This would require a more complex Request factory. For
      // now we just delete.

      Finished();
      delete this;
   }
   else
   {
#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG] ProcessResponseData: read next chunk" << std::endl;
#endif
      // If there is more data, get the next chunk
      GetResponseData(m_response_bufptr, m_response_bufsize);
   }

   return post_process;
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
      m_metadata_promise.set_exception(std::current_exception());
   }

   // Recycle the message to free memory

   alert_msg.RecycleMsg();
}

} // namespace XrdSsiPb


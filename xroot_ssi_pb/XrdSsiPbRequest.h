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
 * XRootD SSI + Protocol Buffers Callback class
 *
 * The client should specialize on this class for each XRootD reply type (Metadata, Alert)
 */

template<typename CallbackArg>
class RequestCallback
{
public:
   void operator()(const CallbackArg &arg);
};



/*!
 * Convert Exceptions to Alerts
 *
 * The client should specialize on this class to specify how to log exceptions
 */

template<typename AlertType>
class ExceptionToAlert
{
public:
   void operator()(const std::exception &e, AlertType &alert);
};



/*!
 * XRootD SSI + Protocol Buffers Request class
 */

template <typename RequestType, typename MetadataType, typename AlertType>
class Request : public XrdSsiRequest
{
public:
   Request(const std::string &buffer_str, unsigned int response_bufsize, uint16_t timeout) :
      m_request_bufptr(buffer_str.c_str()),
      m_request_len(buffer_str.size()),
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
      reqlen = m_request_len;
      return const_cast<char*>(m_request_bufptr);
   }

   /*!
    * Requests are sent to the server asynchronously via the service object. ProcessResponse() is used
    * to inform the request object if the request completed or failed.
    */

   virtual bool ProcessResponse(const XrdSsiErrInfo &eInfo, const XrdSsiRespInfo &rInfo) override;

   /*!
    * ProcessResponseData() is an optional callback used in conjunction with the request's GetResponseData()
    * method, or when the response is a data stream and you wish to asynchronously receive data via the
    * stream. Most applications will need to implement this as scalable applications generally require that
    * any large amount of data be asynchronously received.
    */

   virtual XrdSsiRequest::PRD_Xeq ProcessResponseData(const XrdSsiErrInfo &eInfo, char *buff, int blen, bool last) override;

   /*!
    * Deserialize Alert messages and call the Alert callback
    */

   virtual void Alert(XrdSsiRespInfoMsg &alert_msg) override;

   /*!
    * Return the future associated with the promise
    */

   auto GetFuture() { return m_promise.get_future(); }

private:
   const char *m_request_bufptr;          //!< Pointer to the Request buffer
   int         m_request_len;             //!< Size of the Request buffer
   int         m_response_bufsize;        //!< Size of the Response buffer

   //! Promise a reply of Metadata type

   std::promise<MetadataType> m_promise;

   //! Convert exceptions to Alerts. Must be defined on the client side.

   ExceptionToAlert<AlertType> ExceptionHandler;

   // Callbacks for each of the XRootD reply types

   RequestCallback<AlertType> AlertCallback;

   // Responses and stream Responses will be implemented as a binary blob/stream. The format of the
   // response will not be a protocol buffer. If the response was a protocol buffer we would need to
   // read the entire response before parsing it, which would defeat the point of streaming. The
   // possibilities are:
   // 1. The format is defined in the metadata which is a kind of header
   // 2. The format is record-based, and each record is a protocol buffer
   //
   // Commented out for now pending review, as EOS archive only needs metadata responses
   //
   //RequestCallback<ResponseType> ResponseCallback;
};



template<typename RequestType, typename MetadataType, typename AlertType>
bool Request<RequestType, MetadataType, AlertType>::ProcessResponse(const XrdSsiErrInfo &eInfo, const XrdSsiRespInfo &rInfo)
{
#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] ProcessResponse(): response type = " << rInfo.State() << std::endl;
#endif

   try {
      switch(rInfo.rType) {

      // Handle errors in the XRootD framework (e.g. no response from server)

      case XrdSsiRespInfo::isError:     throw XrdSsiException(eInfo.Get());

      // To implement detached requests, add another callback type which saves the handle

      case XrdSsiRespInfo::isHandle:    throw XrdSsiException("Detached requests are not implemented.");

      // To implement file requests, add another callback type

      case XrdSsiRespInfo::isFile:      throw XrdSsiException("File requests are not implemented.");

      // To implement stream requests, add another callback type

      case XrdSsiRespInfo::isStream:    throw XrdSsiException("Stream requests are not implemented.");

      // Metadata-only responses and data responses

      case XrdSsiRespInfo::isNone:
      case XrdSsiRespInfo::isData:
         // Check for metadata: Arbitrary metadata can be sent ahead of the response data, for example to
         // describe the response so that it can be handled in the most optimal way.

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

            char *response_bufptr = new char[m_response_bufsize];

            // Read the first chunk of data into the buffer, and pass it to ProcessResponseData()

            GetResponseData(response_bufptr, m_response_bufsize);
         }
      }
   }
   catch(std::exception &e)
   {
      // Use the exception to fulfil the promise

      m_promise.set_exception(std::current_exception());

      // Return control of the object to the calling thread and delete rInfo

      Finished();

      // It is now safe to delete the Request object (which implies that the pointer on the calling side
      // will never refer to it again and the destructor of the base class doesn't access any class members).

      delete this;
   }

   return true;
}



template<typename RequestType, typename MetadataType, typename AlertType>
XrdSsiRequest::PRD_Xeq Request<RequestType, MetadataType, AlertType>
             ::ProcessResponseData(const XrdSsiErrInfo &eInfo, char *response_bufptr, int response_buflen, bool is_last)
{
   // The buffer length can be 0 if the response is metadata only

   if(response_buflen != 0)
   {
      // Handle one block of response data
   }

   // If there is more data then get it, otherwise clean up

   if(!is_last)
   {
      // Get the next chunk of data:
      // Does this call this method recursively? Is there danger of a stack overflow?

      GetResponseData(response_bufptr, m_response_bufsize);
   }
   else
   {
      delete[] response_bufptr;

      // Note that if request objects are uniform, you may want to re-use them instead
      // of deleting them, to avoid the overhead of repeated object creation.

      Finished();
      delete this;
   }

   return XrdSsiRequest::PRD_Normal; // Indicate what type of post-processing is required (normal in this case)
                                     // If we can't handle the queue at this time, return XrdSsiRequest::PRD_Hold;
}



template<typename RequestType, typename MetadataType, typename AlertType>
void Request<RequestType, MetadataType, AlertType>::Alert(XrdSsiRespInfoMsg &alert_msg)
{
   // Get the Alert

   int alert_len;
   char *alert_buffer = alert_msg.GetMsg(alert_len);

   // Deserialize the Alert

   AlertType alert;

   if(!alert.ParseFromArray(alert_buffer, alert_len))
   {
      PbException e("alert.ParseFromArray() failed");
      ExceptionHandler(e, alert);
   }

   AlertCallback(alert);

   // Recycle the message to free memory

   alert_msg.RecycleMsg();
}

} // namespace XrdSsiPb

#endif

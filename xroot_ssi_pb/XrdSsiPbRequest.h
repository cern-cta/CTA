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

#include <XrdSsi/XrdSsiRequest.hh>



// XRootD SSI + Protocol Buffers callbacks: The client should specialize on this class for each XRootD reply type
// (Response, Metadata, Alert, Error)

template<typename CallbackArg>
class XrdSsiPbRequestCallback
{
public:
   void operator()(const CallbackArg &arg);
};



// XRootD SSI + Protocol Buffers Request class

template <typename RequestType, typename MetadataType, typename AlertType>
class XrdSsiPbRequest : public XrdSsiRequest
{
public:
           XrdSsiPbRequest(const std::string &buffer_str, unsigned int response_bufsize, uint16_t timeout) :
              m_request_bufptr(buffer_str.c_str()),
              m_request_len(buffer_str.size()),
              m_response_bufsize(response_bufsize)
           {
#ifdef XRDSSI_DEBUG
              std::cout << "Creating XrdSsiPbRequest object:" << std::endl;
              std::cout << "  Response buffer size = " << m_response_bufsize << std::endl;
              std::cout << "  Response timeout = " << timeout << std::endl;
#endif

              // Set response timeout

              SetTimeOut(timeout);
           }
   virtual ~XrdSsiPbRequest() 
           {
#ifdef XRDSSI_DEBUG
              std::cout << "Deleting XrdSsiPbRequest object" << std::endl;
#endif
           }

   // It is up to the implementation to create request data, save it in some manner, and provide it to
   // the framework when GetRequest() is called. Optionally define the RelRequestBuffer() method to
   // clean up when the framework no longer needs access to the data.
   //
   // The thread used to initiate a request may be the same one used in the GetRequest() call.

   virtual char *GetRequest(int &reqlen) override { reqlen = m_request_len; return const_cast<char*>(m_request_bufptr); }

   // Requests are sent to the server asynchronously via the service object. The ProcessResponse() callback
   // is used to inform the request object if the request completed or failed.

   virtual bool ProcessResponse(const XrdSsiErrInfo &eInfo, const XrdSsiRespInfo &rInfo) override;

   // ProcessResponseData() is an optional callback used in conjunction with the request's GetResponseData() method,
   // or when the response is a data stream and you wish to asynchronously receive data via the stream. Most
   // applications will need to implement this as scalable applications generally require that any large amount of
   // data be asynchronously received.

   virtual XrdSsiRequest::PRD_Xeq ProcessResponseData(const XrdSsiErrInfo &eInfo, char *buff, int blen, bool last) override;

   // Alert method is optional, by default Alert messages are ignored

   virtual void Alert(XrdSsiRespInfoMsg &aMsg) override;

private:
   // Pointer to the Request buffer

   const char *m_request_bufptr;
   int         m_request_len;

   // Response buffer size

   int         m_response_bufsize;

   // Callbacks for each of the XRootD reply types

   //XrdSsiPbRequestCallback<ResponseType> ResponseCallback;
   XrdSsiPbRequestCallback<MetadataType> MetadataCallback;
   XrdSsiPbRequestCallback<AlertType> AlertCallback;

   // Additional callback for handling errors from the XRootD framework

   XrdSsiPbRequestCallback<std::string> ErrorCallback;
};



// Process the response

template<typename RequestType, typename MetadataType, typename AlertType>
bool XrdSsiPbRequest<RequestType, MetadataType, AlertType>::ProcessResponse(const XrdSsiErrInfo &eInfo, const XrdSsiRespInfo &rInfo)
{
#ifdef XRDSSI_DEBUG
   std::cout << "ProcessResponse() callback called with response type = " << rInfo.State() << std::endl;
#endif

   switch(rInfo.rType)
   {
      // Handle errors in the XRootD framework (e.g. no response from server)

      case XrdSsiRespInfo::isError:
         ErrorCallback(eInfo.Get());
         Finished();    // Return control of the object to the calling thread and delete rInfo

         // Andy says it is now safe to delete the Request object, which implies that the pointer on the calling side
         // will never refer to it again and the destructor of the base class doesn't access any class members.

         delete this;
         break;

      case XrdSsiRespInfo::isHandle:
         // To implement detached requests, add another callback type which saves the handle
         ErrorCallback("Detached requests are not implemented.");
         Finished();
         delete this;
         break;

      case XrdSsiRespInfo::isFile:
         // To implement file requests, add another callback type
         ErrorCallback("File requests are not implemented.");
         Finished();
         delete this;
         break;

      case XrdSsiRespInfo::isStream:
         // To implement stream requests, add another callback type
         ErrorCallback("Stream requests are not implemented.");
         Finished();
         delete this;
         break;

      case XrdSsiRespInfo::isNone:
      case XrdSsiRespInfo::isData:
         // Check for metadata: Arbitrary metadata can be sent ahead of the response data, for example to
         // describe the response so that it can be handled in the most optimal way.

         int metadata_len;
         const char *metadata_buffer = GetMetadata(metadata_len);

         if(metadata_len > 0)
         {
std::string dump_buffer(metadata_buffer, metadata_len);
DumpBuffer(dump_buffer);
            // Deserialize the metadata

            MetadataType metadata;

            if(metadata.ParseFromArray(metadata_buffer, metadata_len))
            {
               MetadataCallback(metadata);
            }
            else
            {
               ErrorCallback("metadata.ParseFromArray() failed");
               Finished();
               delete this;
               break;
            }
         }

         // If this is a metadata-only response, there is nothing more to do

         if(rInfo.rType == XrdSsiRespInfo::isNone)
         {
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

   return true;
}



template<typename RequestType, typename MetadataType, typename AlertType>
XrdSsiRequest::PRD_Xeq XrdSsiPbRequest<RequestType, MetadataType, AlertType>
             ::ProcessResponseData(const XrdSsiErrInfo &eInfo, char *response_bufptr, int response_buflen, bool is_last)
{
   // The buffer length can be 0 if the response is metadata only

   if(response_buflen != 0)
   {
#if 0
      // How do we handle message boundaries for multi-block responses?

      // Deserialize the response

      ResponseType response;

      if(response.ParseFromArray(response_bufptr, response_buflen))
      {
         ResponseCallback(response);
      }
      else
      {
         ErrorCallback("response.ParseFromArray() failed");
      }
#endif
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
void XrdSsiPbRequest<RequestType, MetadataType, AlertType>::Alert(XrdSsiRespInfoMsg &alert_msg)
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
      ErrorCallback("alert.ParseFromArray() failed");
   }

   // Recycle the message to free memory

   alert_msg.RecycleMsg();
}

#endif

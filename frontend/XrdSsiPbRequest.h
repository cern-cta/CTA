#ifndef __XRD_SSI_PB_REQUEST_H
#define __XRD_SSI_PB_REQUEST_H

#include <XrdSsi/XrdSsiRequest.hh>



// XRootD SSI + Protocol Buffers callbacks.
// The client should define a specialized callback type for each XRootD reply type (Response, Metadata, Alert)

template<typename CallbackArg>
class XrdSsiPbRequestCallback
{
public:
   void operator()(const CallbackArg &arg);
};



// XRootD SSI + Protocol Buffers Request class

template <typename RequestType, typename ResponseType, typename MetadataType, typename AlertType>
class XrdSsiPbRequest : public XrdSsiRequest
{
public:
           XrdSsiPbRequest(const std::string &buffer_str, uint16_t timeout=0) : request_buffer(buffer_str.c_str()), request_len(buffer_str.size())
           {
              std::cerr << "Creating TestSsiRequest object, setting timeout=" << timeout << std::endl;
              SetTimeOut(timeout);
           }
   virtual ~XrdSsiPbRequest() 
           {
              std::cerr << "Deleting TestSsiRequest object" << std::endl;
           }

   // It is up to the implementation to create request data, save it in some manner, and provide it to
   // the framework when GetRequest() is called. Optionally define the RelRequestBuffer() method to
   // clean up when the framework no longer needs access to the data.
   //
   // The thread used to initiate a request may be the same one used in the GetRequest() call.

   // Query for Andy: shouldn't the return type for GetRequest be const?

   virtual char *GetRequest(int &reqlen) override { reqlen = request_len; return const_cast<char*>(request_buffer); }

   // Requests are sent to the server asynchronously via the service object. The ProcessResponse() callback
   // is used to inform the request object if the request completed or failed.

   virtual bool ProcessResponse(const XrdSsiErrInfo &eInfo, const XrdSsiRespInfo &rInfo) override;

   // ProcessReesponseData() is an optional callback used in conjunction with the request's GetResponseData() method,
   // or when the response is a data stream and you wish to asynchronously receive data via the stream. Most
   // applications will need to implement this as scalable applications generally require that any large amount of
   // data be asynchronously received.

   virtual XrdSsiRequest::PRD_Xeq ProcessResponseData(const XrdSsiErrInfo &eInfo, char *buff, int blen, bool last) override;

   // Alert method is optional, by default Alert messages are ignored

   virtual void Alert(XrdSsiRespInfoMsg &aMsg) override;

private:
   const char *request_buffer;
   int         request_len;

   // Callbacks for each of the XRootD response types

   XrdSsiPbRequestCallback<RequestType>  RequestCallback;
   XrdSsiPbRequestCallback<MetadataType> MetadataCallback;
   XrdSsiPbRequestCallback<AlertType>    AlertCallback;

   // Additional callback for handling errors from the XRootD framework

   XrdSsiPbRequestCallback<std::string>  ErrorCallback;
};



// Process the response

template<typename RequestType, typename ResponseType, typename MetadataType, typename AlertType>
bool XrdSsiPbRequest<RequestType, ResponseType, MetadataType, AlertType>::ProcessResponse(const XrdSsiErrInfo &eInfo, const XrdSsiRespInfo &rInfo)
{
   using namespace std;

   cerr << "ProcessResponse() callback called with response type = " << rInfo.State() << endl;

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
            // Temporary workaround for XrdSsi bug #537:
            ++metadata_buffer; --metadata_len;

            // Deserialize the metadata

            const std::string metadata_str(metadata_buffer, metadata_len);

            MetadataType metadata;

            if(!metadata.ParseFromString(metadata_str))
            {
               ErrorCallback("metadata.ParseFromString() failed");
               Finished();
               delete this;
               break;
            }

            MetadataCallback(metadata);

            // If this is a metadata-only response, there is nothing more to do

            if(rInfo.rType == XrdSsiRespInfo::isNone)
            {
               Finished();
               delete this;
               break;
            }
         }

         // Handle messages

         if(rInfo.rType == XrdSsiRespInfo::isData)
         {
            static const int myBSize = 1024;

            char *myBuff = reinterpret_cast<char*>(malloc(myBSize));

            GetResponseData(myBuff, myBSize);
         }
   }

   return true;
}



template<typename RequestType, typename ResponseType, typename MetadataType, typename AlertType>
XrdSsiRequest::PRD_Xeq XrdSsiPbRequest<RequestType, ResponseType, MetadataType, AlertType>::ProcessResponseData(const XrdSsiErrInfo &eInfo, char *myBuff, int myBLen, bool isLast)
{
   using namespace std;

   // If we can't handle the queue at this time, return XrdSsiRequest::PRD_Hold;

   // GetResponseData() above places the data in the allocated buffer, then calls this method with
   // the buffer type and length

   cerr << "Called ProcessResponseData with myBLen = " << myBLen << ", isLast = " << isLast << endl;

   // myBLen can be 0 if there is no relevant response or response is metadata only

   // Process the response data of length myBLen placed in myBuff
   // If there is more data then get it, else free the buffer and
   // indicate to the framework that we are done

   if(!isLast)
   {
      static const int myBSize = 1024;

      // Get the next chunk of data (and call this method recursively ... could this cause a stack overflow ?)

      GetResponseData(myBuff, myBSize);
   }
   else
   {
      myBuff[myBLen] = 0;

      cerr << "Contents of myBuff = " << myBuff << endl;

      free(myBuff);

      Finished(); // Andy says you can now do a "delete this"

      delete this; // Note that if request objects are uniform, you may want to re-use them instead
                   // of deleting them, to avoid the overhead of repeated object creation.
   }

   return XrdSsiRequest::PRD_Normal; // Indicate what type of post-processing is required (normal in this case)
}



template<typename RequestType, typename ResponseType, typename MetadataType, typename AlertType>
void XrdSsiPbRequest<RequestType, ResponseType, MetadataType, AlertType>::Alert(XrdSsiRespInfoMsg &aMsg)
{
   using namespace std;

   int aMsgLen;
   char *aMsgData = aMsg.GetMsg(aMsgLen);

   // Process the alert

   cout << "Received Alert message: " << aMsgData << endl;

   // Failure to recycle the message will cause a memory leak

   aMsg.RecycleMsg();
}

#endif

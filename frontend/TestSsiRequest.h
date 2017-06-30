#ifndef __TEST_SSI_REQUEST_H
#define __TEST_SSI_REQUEST_H

#include <XrdSsi/XrdSsiRequest.hh>

template <typename RequestType, typename ResponseType, typename MetadataType, typename AlertType>
class TestSsiRequest : public XrdSsiRequest
{
public:

           TestSsiRequest(const std::string &buffer_str, uint16_t timeout=0) : request_buffer(buffer_str.c_str()), request_len(buffer_str.size())
           {
              std::cerr << "Creating TestSsiRequest object, setting timeout=" << timeout << std::endl;
              SetTimeOut(timeout);
           }
   virtual ~TestSsiRequest() 
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
};



// Process the response

template<typename RequestType, typename ResponseType, typename MetadataType, typename AlertType>
bool TestSsiRequest<RequestType, ResponseType, MetadataType, AlertType>::ProcessResponse(const XrdSsiErrInfo &eInfo, const XrdSsiRespInfo &rInfo)
{
   using namespace std;

   cerr << "ProcessResponse() callback called with response type = " << rInfo.State() << endl;

   if (eInfo.hasError())
   {
      // Handle error using the passed eInfo object

      cerr << "Error = " << eInfo.Get() << endl;

      Finished();  // Returns control of the object to the calling thread
                   // Deletes rInfo
                   // Andy says you can now do a "delete this"

      delete this; // well, OK, so long as you are 100% sure that this object was allocated by new,
                   // that the pointer on the calling side will never refer to it again, that the
                   // destructor of the base class doesn't access any class members, ...
   }
   else
   {
      // Arbitrary metadata can be sent ahead of the response data, for example to describe the
      // response so that it can be handled in the most optimum way. To access the metadata, call
      // GetMetadata() before calling GetResponseData().

      int myMetadataLen;

      GetMetadata(myMetadataLen);

      if(rInfo.rType == XrdSsiRespInfo::isData && myMetadataLen > 0)
      {
         cerr << "Response has " << myMetadataLen << " bytes of metadata." << endl;

         // do something with metadata

         // A metadata-only response is indicated when XrdSsiRespInfo::rType is set to isNil (i.e. no response data is present).
#if 0
         // clean up

         Finished();

         delete this;
#endif
      }

      if(rInfo.rType == XrdSsiRespInfo::isHandle)
      {
         cerr << "Response is detached, handle = " << endl;

         // copy the handle somewhere

         // clean up

         Finished();

         delete this;
      }

      if(rInfo.rType == XrdSsiRespInfo::isData)
      {
         // A proper data response type

         static const int myBSize = 1024;

         char *myBuff = reinterpret_cast<char*>(malloc(myBSize));

         GetResponseData(myBuff, myBSize);
      }
   }

   return true; // you should always return true. (So why not make the return type void?)
}



template<typename RequestType, typename ResponseType, typename MetadataType, typename AlertType>
XrdSsiRequest::PRD_Xeq TestSsiRequest<RequestType, ResponseType, MetadataType, AlertType>::ProcessResponseData(const XrdSsiErrInfo &eInfo, char *myBuff, int myBLen, bool isLast)
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
void TestSsiRequest<RequestType, ResponseType, MetadataType, AlertType>::Alert(XrdSsiRespInfoMsg &aMsg)
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

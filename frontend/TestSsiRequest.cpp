#include <iostream>

#include "TestSsiRequest.h"

// Process the response

bool TestSsiRequest::ProcessResponse(const XrdSsiErrInfo &eInfo, const XrdSsiRespInfo &rInfo)
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

XrdSsiRequest::PRD_Xeq TestSsiRequest::ProcessResponseData(const XrdSsiErrInfo &eInfo, char *myBuff, int myBLen, bool isLast)
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

void TestSsiRequest::Alert(XrdSsiRespInfoMsg &aMsg)
{
   using namespace std;

   int aMsgLen;
   char *aMsgData = aMsg.GetMsg(aMsgLen);

   // Process the alert

   cout << "Received Alert message: " << aMsgData << endl;

   // Failure to recycle the message will cause a memory leak

   aMsg.RecycleMsg();
}


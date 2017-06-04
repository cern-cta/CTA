#ifndef __TEST_SSI_REQUEST_H
#define __TEST_SSI_REQUEST_H

#include <XrdSsi/XrdSsiRequest.hh>

class TestSsiRequest : public XrdSsiRequest
{
public:

           TestSsiRequest(const std::string &buffer_str, uint16_t tmo=0) : request_buffer(buffer_str.c_str()), request_len(buffer_str.length())
           {
              std::cerr << "Creating TestSsiRequest object, setting tmo=" << tmo << std::endl;
              this->SetTimeOut(tmo);
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

#endif

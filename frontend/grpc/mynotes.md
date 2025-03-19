Could probably extract the common functionality (message queue, next() method, etc) into an interface class
Example of this class:
```c++
class GrpcCtaAdminStreamServerHandler {
private:
  const unsigned int CHUNK_SIZE = 4 * 1024;
  
  enum class StreamState : unsigned int {
    NEW = 1,
    PROCESSING,
    WRITE,
    ERROR,
    FINISH
  };
  
  cta::log::Logger& m_log;
  cta::frontend::grpc::request::Tag m_tag;
  AsyncServer& m_asyncServer;
  cta::xrd::CtaRpcStream::AsyncService& m_ctaRpcStreamSvc;
  StreamState m_streamState;
  /* 
   * Context for the rpc, allowing to tweak aspects of it such as the use
   * of compression, authentication, as well as to send metadata back to the
   * client.
   */
  ::grpc::ServerContext m_ctx;
  
  // Request from the client
  cta::xrd::Request m_request;
  // Response send back to the client
  cta::xrd::StreamResponse m_response;
  // // The means to get back to the client.
  ::grpc::ServerAsyncWriter<cta::xrd::StreamResponse> m_writer;
  // other private members vary depending on the command, for example, for the tape ls we have tapeSearchCriteria and list of tapes
}
```

# Instructions on running the callback_api implementation

## Architecture
In the folder `frontend/grpc/callback_api` we have all the relevant files
- CtaAdminClientReadReactor.hpp -> this contains the client-side logic for submitting the requests and also for printing the response that was received
- CtaAdminCmdStreaming.hpp -> this contains the code that parses the command.
- CtaAdminCmdStreaming.cpp -> this contains the main function that is run when we do cta-admin-grpc-stream
- CtaAdminServer.hpp -> this contains the server-side logic for 
- ServerTapeLs.hpp -> contains the implementation of the ServerTapeLs command. We will need a similar file for all the other streaming command implementations.

CMake Architecture for compiling:
- CMakeLists.txt in the callback_api folder, will build the executable cta-admin-grpc-stream

## How to run
We expect to have the server up and running, the main function/entrypoint is ctafrontend-grpc.sh
Then we will install the cta-admin-grpc-stream rpm and run the tape ls command. BTW, make sure you load the db table contents.

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
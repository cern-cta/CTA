#include "zmq/castorZmqWrapper.hpp"
namespace zmq
{
  int poll (zmq_pollitem_t *items_, int nitems_, long timeout_)
  {
    int rc = zmq_poll (items_, nitems_, timeout_);
    if (rc < 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
    return rc;
  }
  
  void proxy (void *frontend, void *backend, void *capture)
  {
    int rc = zmq_proxy (frontend, backend, capture);
    if (rc != 0){
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
    }
  }
  
  void version (int &major_, int &minor_, int &patch_)
  {
    zmq_version (&major_, &minor_, &patch_);
  }
  
  
  Message::Message ()
  {
    int rc = zmq_msg_init (&m_message);
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
   Message::Message (size_t size_)
  {
    int rc = zmq_msg_init_size (&m_message, size_);
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  Message::Message (void *data_, size_t size_, free_fn *ffn_,void *hint_ )
  {
    int rc = zmq_msg_init_data (&m_message, data_, size_, ffn_, hint_);
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  Message::~Message ()
  {
    int rc = zmq_msg_close (&m_message);
    ZMQ_ASSERT (rc == 0);
  }
  
  void Message::rebuild ()
  {
    int rc = zmq_msg_close (&m_message);
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
    rc = zmq_msg_init (&m_message);
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  void Message::rebuild (size_t size_)
  {
    int rc = zmq_msg_close (&m_message);
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
    rc = zmq_msg_init_size (&m_message, size_);
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  void Message::rebuild (void *data_, size_t size_, free_fn *ffn_, void *hint_)
  { 
    int rc = zmq_msg_close (&m_message);
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
    rc = zmq_msg_init_data (&m_message, data_, size_, ffn_, hint_);
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  void Message::move (Message& otherMsg)
  {
    int rc = zmq_msg_move (&m_message, &(otherMsg.m_message));
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  void Message::copy (Message& otherMsg)
  {
    int rc = zmq_msg_copy (&m_message, &(otherMsg.m_message));
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  bool Message::more ()
  {
    int rc = zmq_msg_more (&m_message);
    return rc != 0;
  }
  
  void* Message::data ()
  {
    return zmq_msg_data (&m_message);
  }
  
  const void* Message::data () const
  {
    return zmq_msg_data (const_cast<zmq_msg_t*>(&m_message));
  }
  
  size_t Message::size () const
  {
    return zmq_msg_size (const_cast<zmq_msg_t*>(&m_message));
  }
  
  Context::Context ()
  {
    m_context = zmq_ctx_new ();
    if (m_context == NULL)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  
  Context::Context (int io_threads, int max_sockets)
  {
    m_context = zmq_ctx_new ();
    if (m_context == NULL)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
    
    int rc = zmq_ctx_set (m_context, ZMQ_IO_THREADS, io_threads);
    ZMQ_ASSERT (rc == 0);
    
    rc = zmq_ctx_set (m_context, ZMQ_MAX_SOCKETS, max_sockets);
    ZMQ_ASSERT (rc == 0);
  }
  
  Context::~Context ()
  {
    //close();
  }
  
  void Context::close()
  {
    if (m_context == NULL)
      return;
    int rc = zmq_ctx_destroy (m_context);
    ZMQ_ASSERT (rc == 0);
    m_context = NULL;
  }
  
  
  Socket::Socket (Context &context, int type_)
  {
    m_ownerContext = context.m_context;
    m_socket = zmq_socket (context.m_context, type_);
    if (m_socket == NULL)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  Socket::~Socket ()
  {
    close();
  }
  
  Socket::operator void* ()
  {
    return m_socket;
  }
  
  void Socket::close()
  {
    if(m_socket == NULL)
      // already closed
      return ;
    int rc = zmq_close (m_socket);
    ZMQ_ASSERT (rc == 0);
    m_socket = 0 ;
  }
    
  void Socket::bind (const char *addr)
  {
    int rc = zmq_bind (m_socket, addr);
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  void Socket::unbind (const char *addr)
  {
    int rc = zmq_unbind (m_socket, addr);
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  void Socket::connect (const char *addr)
  {
    int rc = zmq_connect (m_socket, addr);
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  void Socket::disconnect (const char *addr)
  {
    int rc = zmq_disconnect (m_socket, addr);
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  bool Socket::connected()
  {
    return(m_socket != NULL);
  }
  
  size_t Socket::send (const void *buf_, size_t len_, int flags_)
  {
    int nbytes = zmq_send (m_socket, buf_, len_, flags_);
    if (nbytes >= 0)
      return (size_t) nbytes;
    if (zmq_errno () == EAGAIN)
      return 0;
    throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  bool Socket::send (Message &msg_, int flags_)
  {
    int nbytes = zmq_msg_send (&(msg_.m_message), m_socket, flags_);
    if (nbytes >= 0)
      return true;
    if (zmq_errno () == EAGAIN)
      return false;
    throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  size_t Socket::recv (void *buf_, size_t len_, int flags_)
  {
    int nbytes = zmq_recv (m_socket, buf_, len_, flags_);
    if (nbytes >= 0)
      return (size_t) nbytes;
    if (zmq_errno () == EAGAIN)
      return 0;
    throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  bool Socket::recv (Message& msg_, int flags_)
  {
    int nbytes = zmq_msg_recv (&(msg_.m_message), m_socket, flags_);
    if (nbytes >= 0)
      return true;
    if (zmq_errno () == EAGAIN)
      return false;
    throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
  }
  
  Monitor::Monitor() : socketPtr(NULL) {
  
  }
  Monitor::~Monitor() {
  }
  
  void Monitor::monitor(Socket &socket, const char *addr, int events)
  {
    int rc = zmq_socket_monitor(socket.m_socket, addr, events);
    if (rc != 0)
      throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
    
    socketPtr = socket.m_socket;
    void *s = zmq_socket (socket.m_ownerContext, ZMQ_PAIR);
    assert (s);
    
    rc = zmq_connect (s, addr);
    assert (rc == 0);
    
    on_monitor_started();
    
    while (true) {
      zmq_msg_t eventMsg;
      zmq_msg_init (&eventMsg);
      rc = zmq_recvmsg (s, &eventMsg, 0);
      if (rc == -1 && zmq_errno() == ETERM)
        break;
      assert (rc != -1);
      zmq_event_t* event = static_cast<zmq_event_t*>(zmq_msg_data (&eventMsg));
      
      zmq_msg_t addrMsg;
      zmq_msg_init (&addrMsg);
      rc = zmq_recvmsg (s, &addrMsg, 0);
      if (rc == -1 && zmq_errno() == ETERM)
        break;
      assert (rc != -1);
      const char* str = static_cast<const char*>(zmq_msg_data (&addrMsg));
      std::string address(str, str + zmq_msg_size(&addrMsg));
      zmq_msg_close (&addrMsg);
      
      switch (event->event) {
        case ZMQ_EVENT_CONNECTED:
          on_event_connected(*event, address.c_str());
          break;
        case ZMQ_EVENT_CONNECT_DELAYED:
          on_event_connect_delayed(*event, address.c_str());
          break;
        case ZMQ_EVENT_CONNECT_RETRIED:
          on_event_connect_retried(*event, address.c_str());
          break;
        case ZMQ_EVENT_LISTENING:
          on_event_listening(*event, address.c_str());
          break;
        case ZMQ_EVENT_BIND_FAILED:
          on_event_bind_failed(*event, address.c_str());
          break;
        case ZMQ_EVENT_ACCEPTED:
          on_event_accepted(*event, address.c_str());
          break;
        case ZMQ_EVENT_ACCEPT_FAILED:
          on_event_accept_failed(*event, address.c_str());
          break;
        case ZMQ_EVENT_CLOSED:
          on_event_closed(*event, address.c_str());
          break;
        case ZMQ_EVENT_CLOSE_FAILED:
          on_event_close_failed(*event, address.c_str());
          break;
        case ZMQ_EVENT_DISCONNECTED:
          on_event_disconnected(*event, address.c_str());
          break;
        default:
          on_event_unknown(*event, address.c_str());
          break;
      }
      zmq_msg_close (&eventMsg);
    }
    zmq_close (s);
    socketPtr = NULL;
  }

  void Monitor::on_monitor_started() {}
  void Monitor::on_event_connected(const zmq_event_t &, const char*) {}
  void Monitor::on_event_connect_delayed(const zmq_event_t &, const char*) {}
  void Monitor::on_event_connect_retried(const zmq_event_t &, const char*) {}
  void Monitor::on_event_listening(const zmq_event_t &, const char*) {}
  void Monitor::on_event_bind_failed(const zmq_event_t &, const char*) {}
  void Monitor::on_event_accepted(const zmq_event_t &, const char*) {}
  void Monitor::on_event_accept_failed(const zmq_event_t &, const char*) {}
  void Monitor::on_event_closed(const zmq_event_t &, const char*) {}
  void Monitor::on_event_close_failed(const zmq_event_t &, const char*) {}
  void Monitor::on_event_disconnected(const zmq_event_t &, const char*) {}
  void Monitor::on_event_unknown(const zmq_event_t &, const char*) {}
}
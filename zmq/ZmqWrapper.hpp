/******************************************************************************
 *                      castorZmqWrapper.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/
#pragma once
#include "castor/exception/Exception.hpp"
#include <zmq.h>
#include <string>

// In order to prevent unused variable warnings when building in non-debug
// mode use this macro to make assertions.
#ifndef NDEBUG
# define ZMQ_ASSERT(expression) assert(expression)
#else
# define ZMQ_ASSERT(expression) (void)(expression)
#endif

/**
 * Straightforward wrapper of ZMQ C APi into C++ classes.  
 * Each function is pretty simple and calls the matching zmq function  
 * and handles the errors, throwing exceptions if needed. 
 * If you need to know what does a particular function, have a look at ZMQ's doc :
 */
namespace zmq
{

  typedef zmq_free_fn FreeFunctor;
  
  int poll (zmq_pollitem_t *items, int nitems, long timeout = -1);
  
  void proxy (void *frontend, void *backend, void *capture);
  
  void version (int &major, int &minor, int &patch);
  
  class Message
  {
    friend class Socket;
    
  public:
    
    Message ();
    
    explicit Message (size_t size);
    
    Message (void *data, size_t size, FreeFunctor *ffn,void *hint = NULL);
    
    ~Message ();
    
    void rebuild ();
    
    void rebuild (size_t size_);
    
    void rebuild (void *data, size_t size, FreeFunctor *ffn_, void *hint = NULL);
    
    void move (Message& msg_);
    
    void copy (Message& msg_);
    
    bool more ();
    
    void *data ();
    
    const void* data () const;
    
    size_t size () const;
    
  private:
    
    // The underlying message
    zmq_msg_t m_message;
    
    // Disable implicit message copying, so that users won't use shared
    // messages (less efficient) without being aware of the fact.
    Message (const Message&);
    void operator = (const Message&);
  };
  
  class Context
  {
    friend class Socket;
    
  public:
    Context ();
    
    explicit Context (int io_threads, int max_sockets = ZMQ_MAX_SOCKETS_DFLT);
    
    ~Context ();
    
    void close();
    
  private:
    
    void *m_context;
    
    Context (const Context&);
    void operator = (const Context&);
  };
  
  class Socket
  {
    friend class Monitor;
  public:
    
    Socket (Context &context, int type);
    
    ~Socket ();
    
    operator void* ();
    
    void close();
    
    template<class T> void setsockopt (int option,const T& optval)
    {
      const int size = sizeof(optval);
      int rc = zmq_setsockopt (m_socket, option, &optval,&size);
      if (rc != 0)
        throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
    }
    
    template<class T> void getsockopt (int option, T& optval) const{
      size_t size = sizeof(optval);
      int rc = zmq_getsockopt ((m_socket), option, &optval, &size);
      if (rc != 0)
        throw castor::exception::Exception (zmq_strerror(zmq_errno ()));
    }
    
    void bind (const char *addr);
    
    void unbind (const char *addr);
    
    void connect (const char *addr);
    
    void disconnect (const char *addr);
    
    bool connected();
    
    size_t send (const void *buf, size_t len, int flags = 0);
    
    bool send (Message &msg_, int flags = 0);
    
    size_t recv (void *buf, size_t len, int flags = 0);
    
    bool recv (Message& msg_, int flags = 0);
    bool moreParts() const;
  private:
    void *m_socket;
    void *m_ownerContext;
    
    Socket (const Socket&) ;
    void operator = (const Socket&) ;
  };

  class Monitor
  {
  public:
    Monitor();
    virtual ~Monitor();
    
    void monitor(Socket &socket, const char *addr, int events = ZMQ_EVENT_ALL);
    
    virtual void on_monitor_started();
    virtual void on_event_connected(const zmq_event_t &, const char*);
    virtual void on_event_connect_delayed(const zmq_event_t &, const char*) ;
    virtual void on_event_connect_retried(const zmq_event_t &, const char*);
    virtual void on_event_listening(const zmq_event_t &, const char*);
    virtual void on_event_bind_failed(const zmq_event_t &, const char*);
    virtual void on_event_accepted(const zmq_event_t &, const char*);
    virtual void on_event_accept_failed(const zmq_event_t &, const char*) ;
    virtual void on_event_closed(const zmq_event_t &, const char*) ;
    virtual void on_event_close_failed(const zmq_event_t &, const char*) ;
    virtual void on_event_disconnected(const zmq_event_t &, const char*) ;
    virtual void on_event_unknown(const zmq_event_t &, const char*);
  private:
    void* m_socketMonitored;
  };
}



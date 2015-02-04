#pragma once

#include "ObjectStores.hpp"

namespace cta { namespace objectstore {

template <class C>
class ContextHandleImplementation: public ContextHandle {};

template <>
class ContextHandleImplementation <ObjectStoreVFS>: public ContextHandle {
public:
  ContextHandleImplementation(): m_set(false), m_fd(-1) {}
  virtual void set(int fd) { m_set = true;  m_fd = fd; __sync_synchronize(); }
  virtual int get(int) { return m_fd; }
  virtual void reset() { m_set = false; m_fd = -1; __sync_synchronize(); }
  virtual bool isSet() { return m_set; }
  virtual void release() { __sync_synchronize(); if (m_set && -1 != m_fd) ::close(m_fd); reset(); } 
private:
  bool m_set;
  volatile int m_fd;
};


template <>
class ContextHandleImplementation <ObjectStoreRados>: public ContextHandle {
public:
  ContextHandleImplementation(): m_set(false) {}
  virtual void set(int) { m_set = true; }
  virtual int get(int) { return 0; }
  virtual void reset() { m_set = false; }
  virtual bool isSet() { return m_set; }
  virtual void release() { reset(); }
private:
  bool m_set;
};

}}
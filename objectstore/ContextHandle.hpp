#pragma once

#include "ObjectStores.hpp"

namespace cta { namespace objectstore {

template <class C>
class ContextHandleImplementation: public ContextHandle {};

template <>
class ContextHandleImplementation <ObjectStoreVFS>: public ContextHandle {
public:
  ContextHandleImplementation(): m_set(false), m_fd(-1) {}
  void set(int fd) { m_set = true;  m_fd = fd; __sync_synchronize(); }
  int get(int) { return m_fd; }
  void reset() { m_set = false; m_fd = -1; __sync_synchronize(); }
  bool isSet() { return m_set; }
  void release() { __sync_synchronize(); if (m_set && -1 != m_fd) ::close(m_fd); reset(); } 
private:
  bool m_set;
  volatile int m_fd;
};

template <>
class ContextHandleImplementation <ObjectStoreRados>: public ContextHandle {
public:
  ContextHandleImplementation(): m_set(false) {}
  void set(int) { m_set = true; }
  void reset() { m_set = false; }
  bool isSet() { return m_set; }
  void release() { reset(); }
private:
  bool m_set;
};

}}
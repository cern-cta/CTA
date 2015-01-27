#pragma once

#include "ObjectOps.hpp"
#include "exception/Exception.hpp"

class FIFO: private ObjectOps<cta::objectstore::FIFO> {
public:
  FIFO(ObjectStore & os, const std::string & name, ContextHandle & context):
  ObjectOps<cta::objectstore::FIFO>(os, name),
  m_locked(false) {
    cta::objectstore::FIFO fs;
    updateFromObjectStore(fs, context);
  }
  
  void lock(ContextHandle & context) {
    if (m_locked) throw cta::exception::Exception(
      "In FIFO::lock: already locked");
    lockExclusiveAndRead(m_currentState, context);
    m_locked = true;
  }
  
  std::string peek() {
    std::string ret;
    if (!m_locked) throw cta::exception::Exception(
      "In FIFO::pop: FIFO should be locked");
    return m_currentState.name(m_currentState.readpointer());
    return ret;
  }
  
  void popAndUnlock(ContextHandle & context) {
    if (!m_locked) throw cta::exception::Exception(
      "In FIFO::pop: FIFO should be locked");
    m_currentState.set_readpointer(m_currentState.readpointer()+1);
    if (m_currentState.readpointer() > 100) {
      compactCurrentState();
    }
    write(m_currentState);
    unlock(context);
    m_locked = false;
  }
  
  void push(std::string name, ContextHandle & context) {
    cta::objectstore::FIFO fs;
    lockExclusiveAndRead(fs, context);
    fs.add_name(name);
    write(fs);
    unlock(context);
  }
  
  std::string dump(ContextHandle & context) {
    cta::objectstore::FIFO fs;
    updateFromObjectStore(fs, context);
    std::stringstream ret;
    ret<< "<<<< FIFO dump start" << std::endl
      << "Read pointer=" << fs.readpointer() << std::endl
      << "Array size=" << fs.name_size() << std::endl;
    for (int i=fs.readpointer(); i<fs.name_size(); i++) {
      ret << "name[phys=" << i << " ,log=" << i-fs.readpointer()
          << "]=" << fs.name(i) << std::endl;
    }
    ret<< ">>>> FIFO dump end." << std::endl;
    return ret.str();
  }
  
private:
  bool m_locked;
  cta::objectstore::FIFO m_currentState;
  
  void compactCurrentState() {
    uint64_t oldReadPointer = m_currentState.readpointer();
    uint64_t oldSize = m_currentState.name_size();
    // Copy the elements at position oldReadPointer + i to i (squash all)
    // before the read pointer
    for (int i = oldReadPointer; i<m_currentState.name_size(); i++) {
      *m_currentState.mutable_name(i-oldReadPointer) = m_currentState.name(i);
    }
    // Shorten the name array by oldReadPointer elements
    for (uint64_t i = 0; i < oldReadPointer; i++) {
      m_currentState.mutable_name()->RemoveLast();
    }
    // reset the read pointer
    m_currentState.set_readpointer(0);
    // Check the size is as expected
    if ((uint64_t)m_currentState.name_size() != oldSize - oldReadPointer) {
      std::stringstream err;
      err << "In FIFO::compactCurrentState: wrong size after compaction: "
        << "oldSize=" << oldSize << " oldReadPointer=" << oldReadPointer
        << " newSize=" << m_currentState.name_size();
      throw cta::exception::Exception(err.str());
    }
  }
};
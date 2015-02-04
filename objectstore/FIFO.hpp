#pragma once

#include "ObjectOps.hpp"
#include "Agent.hpp"
#include "exception/Exception.hpp"
#include <cxxabi.h>

namespace cta { namespace objectstore {

class FIFO: private ObjectOps<serializers::FIFO> {
public:
  FIFO(const std::string & name, Agent & agent):
  ObjectOps<serializers::FIFO>(agent.objectStore(), name) {
    serializers::FIFO fs;
    updateFromObjectStore(fs, agent.getFreeContext());
  }
  
private:
  void lock(ContextHandle & ctx) {
    lockExclusiveAndRead(m_currentState, ctx, __func__);
  }
  
public:
  class FIFOEmpty: public cta::exception::Exception {
  public:
    FIFOEmpty(const std::string & context): cta::exception::Exception(context) {}
  };
  
  friend class Transaction;  
  class Transaction {
  public:
    Transaction(FIFO & fifo, Agent & agent):
    m_fifo(fifo), m_ctx(agent.getFreeContext()), m_writeDone(false) {
      m_fifo.lock(m_ctx);
    }
    
    ~Transaction() {
      try {
        if(!m_writeDone)
          m_fifo.unlock(m_ctx);
      } catch (abi::__forced_unwind&) {
            throw;
      } catch (...) {}
    }
    
    std::string peek() {
      if (m_writeDone)
        throw cta::exception::Exception("In FIFO::Transaction::peek: write already occurred");
      if (m_fifo.m_currentState.readpointer() >= (uint64_t)m_fifo.m_currentState.name_size()) {
        throw FIFOEmpty("In FIFO::Transaction::peek: FIFO empty");
      }
      return m_fifo.m_currentState.name(m_fifo.m_currentState.readpointer());
    }
    
    void popAndUnlock() {
      if (m_writeDone)
        throw cta::exception::Exception("In FIFO::Transaction::popAndUnlock: write already occurred");
      if (m_fifo.m_currentState.readpointer() >= (uint64_t)m_fifo.m_currentState.name_size()) {
        throw FIFOEmpty("In FIFO::Transaction::popAndUnlock: FIFO empty");
      }
      m_fifo.m_currentState.set_readpointer(m_fifo.m_currentState.readpointer()+1);
      if (m_fifo.m_currentState.readpointer() > 100) {
        m_fifo.compactCurrentState();
      }
      m_fifo.write(m_fifo.m_currentState);
      m_fifo.unlock(m_ctx);
      m_writeDone = true;
    }
  private:
    FIFO & m_fifo;
    ContextHandle & m_ctx;
    bool m_writeDone;
  };
  
  Transaction startTransaction(Agent & agent) {
    return Transaction(*this, agent);
  }
  
  void push(std::string name, Agent & agent) {
    serializers::FIFO fs;
    ContextHandle & context = agent.getFreeContext();
    lockExclusiveAndRead(fs, context, __func__);
    fs.add_name(name);
    write(fs);
    unlock(context);
  }
  
  std::string dump(Agent & agent) {
    serializers::FIFO fs;
    updateFromObjectStore(fs, agent.getFreeContext());
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
  
  uint64_t size(Agent & agent) {
    serializers::FIFO fs;
    updateFromObjectStore(fs, agent.getFreeContext());
    return fs.name_size();
  }
  
private:
  serializers::FIFO m_currentState;
  
  void compactCurrentState() {
    uint64_t oldReadPointer = m_currentState.readpointer();
    uint64_t oldSize = m_currentState.name_size();
    // Copy the elements at position oldReadPointer + i to i (squash all)
    // before the read pointer
    for (int i = oldReadPointer; i<m_currentState.mutable_name()->size(); i++) {
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

}}
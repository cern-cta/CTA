/* 
 * File:   BlockingQueue.hpp
 * Author: dcome
 *
 * Created on March 5, 2014, 5:06 PM
 */

#pragma once

#include <queue>
#include <exception>

#include "Threading.hpp"

namespace castor {
namespace tape {
namespace threading {
    
/**
 * Exception class used to signal there are no more elements
*/
class noMore : public castor::tape::Exception
{
public:
  noMore():Exception("")
  {} 
};
  
/***
 * This simple class provides a thread-safe blocking queue
 *  
 */
template<class C>
class BlockingQueue {

public:

  typedef typename std::queue<C>::value_type value_type;
  typedef typename std::queue<C>::reference reference;
  typedef typename std::queue<C>::const_reference const_reference;
  
  BlockingQueue(){}
  ~BlockingQueue() {}

  void push(const C& e) {
    {
      MutexLocker ml(&m_mutex);
      m_queue.push(e);
    }
    m_sem.release();
  }
  
  ///Return the next value of the queue and remove it
  C pop() {
    m_sem.acquire();
    return popCriticalSection();
  }
 
  /**
   * Return the next value of the queue and remove it
   * if there are no elements, throws a no noMoreElements exception 
   */
  C tryPop() {
    if (!m_sem.tryAcquire()) throw noMore();
    return popCriticalSection();
  }
  
  ///return the number of elements currently in the queue
  size_t size() const { 
      MutexLocker ml(&m_mutex);
      return m_queue.size(); 
  }
  
private:  
  /// holds data of the queue
  std::queue<C> m_queue;
  
  ///Used for blocking a consumer thread as long as the queue is empty 
  Semaphore m_sem;
  
  ///used for locking-operation thus providing thread-safety
  mutable Mutex m_mutex;

  ///Thread and exception safe pop
  C popCriticalSection() {
    MutexLocker ml(&m_mutex);
    C ret = m_queue.front();
    m_queue.pop();
    
    return ret;
  }
  
};

} //end of threading
} //end of tape
} //end of castor

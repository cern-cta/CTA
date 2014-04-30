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
  typedef struct valueRemainingPair {C value; size_t remaining;} valueRemainingPair;
  
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
   * Atomically pop the element of the top of the pile AND return it with the 
   * number of remaining elements in the queue 
   * @return a struct holding the popped element (into ret.value) and the number of elements 
   * remaining (into ret.remaining)
   * 
   */
  valueRemainingPair popGetSize () {
    m_sem.acquire();
    valueRemainingPair ret;
    ret.value = popCriticalSection(&ret.remaining);
    return ret;
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

  ///Thread and exception safe pop. Optionally atomically extracts the size 
  // of the queue after pop
  C popCriticalSection(size_t * sz = NULL) {
    MutexLocker ml(&m_mutex);
    C ret = m_queue.front();
    m_queue.pop();
    if (sz)
      *sz = m_queue.size();
    return ret;
  }
  
};

} //end of threading
} //end of tape
} //end of castor

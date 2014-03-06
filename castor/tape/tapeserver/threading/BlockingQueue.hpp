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



/***
 * This simple class provides a thread-safe blocking queue
 *  
 */
namespace castor {
namespace tape {
namespace threading {
    
/*
making it a nested class  ensures that BlockingQueue<C>::noMoreElements
is different from BlockingQueue<D>::noMoreElements. That way, we have more 
informations about the exception because we know what kind of queue throwed it
But it makes it more tedious to catch.
 * @TODO : decide what final thing to do
*/
class noMoreElements : public std::exception {};
  
template<class C>
class BlockingQueue {

public:

  typedef typename std::queue<C>::value_type value_type;
  typedef typename std::queue<C>::reference reference;
  typedef typename std::queue<C>::const_reference const_reference;
  
  BlockingQueue() {}

  
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
 
  ///Return the next value of the queue and remove it
  ///if there are no elements, throws a no noMoreElements exception 
  C tryPop() {
    if (!m_sem.tryAcquire()) throw noMoreElements();
    return popCriticalSection();
  }
  size_t size() const { 
      MutexLocker ml(&m_mutex);
      return m_queue.size(); 
  }
  ~BlockingQueue() {}
private:
    
  /// holds data of the queue
  std::queue<C> m_queue;
  
  ///Used for blocking a consumer thread as long as the queue is empty 
  Semaphore m_sem;
  
  ///used for locking-operation 
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

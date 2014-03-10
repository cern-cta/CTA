/******************************************************************************
 *                      TapeQueue.hpp
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

#include "castor/tape/tapeserver/daemon/TapeThreading.hpp"
#include <queue>

template<class C>
class LinkedListQueue {
public:
  LinkedListQueue(): m_first(NULL), m_last(NULL), m_size(0) {}
  void push(C e) {
    elem * ins = new elem(e);
    if (m_last) {
      m_last->next = ins;
      m_last = ins;
    } else {
      m_first = m_last = ins;
    }
    m_size++;
  }
  C front() {
    return m_first->val;
  }
  void pop() {
    elem * old = m_first;
    m_first = m_first->next;
    if (!m_first) m_last = NULL;
    m_size--;
    delete old;
  }
  size_t size() { return m_size; }
private:
  class elem {
  public:
    elem(C v): val(v), next(NULL) {}
    C val;
    elem * next;
  };
  elem * m_first;
  elem * m_last;
  size_t m_size;
};

template<class C>
//class TapeQueue: public LinkedListQueue<C>{};
class TapeQueue: public std::queue<C>{};

template<class C>
class BlockingQueue {
public:
  BlockingQueue(): m_content(0) {}
  class noMore {};
  void push(C e) {
    {
      TapeMutexLocker ml(&m_mutex);
      m_queue.push(e);
      m_content++;
    }
    m_sem.release();
  }
  C pop() {
    m_sem.acquire();
    return popCriticalSection();
  }
  C tryPop() {
    if (!m_sem.tryAcquire()) throw noMore();
    return popCriticalSection();
  }
  size_t size() { return m_queue.size(); }
  ~BlockingQueue() {}
private:
  TapeQueue<C> m_queue;
  TapeSemaphore m_sem;
  TapeMutex m_mutex;
  int m_content;

  C popCriticalSection() {
    TapeMutexLocker ml(&m_mutex);
    C ret = m_queue.front();
    m_queue.pop();
    m_content--;
    return ret;
  }
};

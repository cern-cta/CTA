/******************************************************************************
 *                      TapeThreading.hpp
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
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>

/* WARNING WARNING TODO: this is a quick replacement of Qt's threading primives
 * in an attempt to replace Qt's primitives which might give problems.
 * There is no error handling!!!!!! */

/* Simplified version of Qt's Qthread object
 * We are limited here to:
 *  start
 *  run
 *  wait
 */

class TapeThread {
public:
  void start() {
    if (pthread_create(&m_thread, NULL, pthread_runner, this)) {
      throw MemException("Failed to start thread");
    }
  }
  void wait() {
    pthread_join(m_thread, NULL);
  }
  virtual void run () = 0;
  virtual ~TapeThread () {}
private:
  pthread_t m_thread;
  static void * pthread_runner (void * arg) {
    TapeThread * _this = (TapeThread *) arg;
    try {
      _this->run();
    } catch (std::exception & e) {
      printf ("Thread finished with uncaught exception: %s\n", e.what());
    }
    return NULL;
  }
};

class TapeMutex {
public:
  TapeMutex() {
    m_deleted = false;
    pthread_mutex_init(&m_mutex, NULL);
    //printf("Initialised a mutex, TapeMutex::this=0x%x\n", this);
  }
  ~TapeMutex() {
    pthread_mutex_destroy(&m_mutex);
    //printf("Destroyed a mutex, TapeMutex::this=0x%x\n", this);
    m_deleted = true;
  }
  void lock() { 
    if (m_deleted) *((volatile char*) NULL) = 0;
    errnoExeptLauncher(pthread_mutex_lock(&m_mutex), &m_mutex); 
  }
  void unlock() { 
    if (m_deleted) *((volatile char*) NULL) = 0;
    errnoExeptLauncher(pthread_mutex_unlock(&m_mutex), &m_mutex); 
  }
private:
  pthread_mutex_t m_mutex;
  bool m_deleted;
};

class TapeMutexLocker {
public:
  TapeMutexLocker(TapeMutex * m): m_mutex(m) { m_mutex->lock();}
  ~TapeMutexLocker() { m_mutex->unlock(); }
private:
  TapeMutex * m_mutex;
};

#ifndef __APPLE__
class TapeSemaphore {
public:
  TapeSemaphore(int initial = 0) {
    sem_init(&m_sem, 0, initial);
  }
  ~TapeSemaphore() {
    TapeMutexLocker ml(&m_mutexPosterProtection);
    sem_destroy(&m_sem);
  }
  void acquire() { sem_wait(&m_sem); }
  bool tryAcquire() { return !sem_trywait(&m_sem); }
  void release(int n=1) {
    for (int i=0; i<n; i++) {
      TapeMutexLocker ml(&m_mutexPosterProtection);
      sem_post(&m_sem);
    }
  }
private:
  sem_t m_sem;
  /* this mutex protects against the destruction killing the poster
   (race condition in glibc) */
  TapeMutex m_mutexPosterProtection;
};
#else
/* @$%@% APPLE does not like posix semaphores! */
class TapeSemaphore {
public:
  TapeSemaphore(): m_value(0) {
    pthread_cond_init(&m_cond, NULL);
    pthread_mutex_init(&m_mutex, NULL);
  }
  ~TapeSemaphore() {
    /* Barrier protecting the last user */
    pthread_mutex_lock(&m_mutex);
    pthread_mutex_unlock(&m_mutex);
    /* Cleanup */
    pthread_cond_destroy(&m_cond);
    pthread_mutex_destroy(&m_mutex);
  }
  void acquire()  { 
    pthread_mutex_lock(&m_mutex);
    while (m_value <= 0) {
      pthread_cond_wait(&m_cond, &m_mutex);
    }
    m_value--;
    pthread_mutex_unlock(&m_mutex);
  }
  bool tryAcquire() {
    bool ret;
    pthread_mutex_lock(&m_mutex);
    if (m_value > 0) {
      ret = true;
      m_value--;
    } else {
      ret = false;
    }
    pthread_mutex_unlock(&m_mutex);
    return ret;
  }
  void release() {
    pthread_mutex_lock(&m_mutex);
    m_value++;
    pthread_cond_signal(&m_cond);
    pthread_mutex_unlock(&m_mutex);
  }
private:
  pthread_cond_t m_cond;
  pthread_mutex_t m_mutex;
  int m_value;
};
#endif

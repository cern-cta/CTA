/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "Backend.hpp"
#include "rados/librados.hpp"
#include <future>


namespace cta { namespace objectstore {
/**
 * An implementation of the object store primitives, using Rados.
 */
class BackendRados: public Backend {
public:
  class AsyncUpdater;
  friend class AsyncUpdater;
  /**
   * The constructor, connecting to the storage pool 'pool' using the user id
   * 'userId'
   * @param userId
   * @param pool
   */
  BackendRados(const std::string & userId, const std::string & pool, const std::string &radosNameSpace = "");
  ~BackendRados() override;
  std::string user() {
    return m_user;
  }
  std::string pool() {
    return m_pool;
  }
  

  void create(std::string name, std::string content) override;
  
  void atomicOverwrite(std::string name, std::string content) override;

  std::string read(std::string name) override;
  
  void remove(std::string name) override;
  
  bool exists(std::string name) override;
  
  std::list<std::string> list() override;
  
  class ScopedLock: public Backend::ScopedLock {
    friend class BackendRados;
  public:
    void release() override;
    ~ScopedLock() override;
  private:
    ScopedLock(librados::IoCtx & ioCtx): m_lockSet(false), m_context(ioCtx) {}
    void set(const std::string & oid, const std::string clientId);
    bool m_lockSet;
    librados::IoCtx & m_context;
    std::string m_clientId;
    std::string m_oid;
  };
  
  static const size_t c_maxBackoff;
  static const size_t c_backoffFraction;
  static const uint64_t c_maxWait;
  
private:
  static std::string createUniqueClientId();
  
public:  
  ScopedLock * lockExclusive(std::string name) override;

  ScopedLock * lockShared(std::string name) override;
  
  /**
   * A class following up the check existence-lock-fetch-update-write-unlock. Constructor implicitly
   * starts the lock step.
   */
  class AsyncUpdater: public Backend::AsyncUpdater {
  public:
    AsyncUpdater(BackendRados & be, const std::string & name, std::function <std::string(const std::string &)> & update);
    void wait() override;
  private:
    /** A reference to the backend */
    BackendRados &m_backend;
    /** The object name */
    const std::string m_name;
    /** The operation on the object */
    std::function <std::string(const std::string &)> & m_update;
    /** Storage for stat operation (size) */
    uint64_t m_size;
    /** Storage for stat operation (date) */
    time_t date;
    /** The promise that will both do the job and allow synchronization with the caller. */
    std::promise<void> m_job;
    /** The future from m_jobs, which will be extracted before any thread gets a chance to play with it. */
    std::future<void> m_jobFuture;
    /** A future used to hold the structure of the lock operation. It will be either empty of complete at 
     destruction time */
    std::unique_ptr<std::future<void>> m_lockAsync;
    /** A string used to identify the locker */
    std::string m_lockClient;
    /** The rados bufferlist used to hold the object data (read+write) */
    ::librados::bufferlist m_radosBufferList;
    /** A future the hole the the structure of the update operation. It will be either empty of complete at 
     destruction time */
    std::unique_ptr<std::future<void>> m_updateAsync;
    /** The first callback operation (after checking existence) */
    static void statCallback(librados::completion_t completion, void *pThis);
    /** Async delete in case of zero sized object */
    static void deleteEmptyCallback(librados::completion_t completion, void *pThis);
    /** The second callback operation (after reading) */
    static void fetchCallback(librados::completion_t completion, void *pThis);
    /** The third callback operation (after writing) */
    static void commitCallback(librados::completion_t completion, void *pThis);
    /** The fourth callback operation (after unlocking) */
    static void unlockCallback(librados::completion_t completion, void *pThis);
  };
  
  Backend::AsyncUpdater* asyncUpdate(const std::string & name, std::function <std::string(const std::string &)> & update) override;

  /**
   * A class following up the check existence-lock-delete. 
   * Constructor implicitly starts the lock step.
   */
  class AsyncDeleter: public Backend::AsyncDeleter {
  public:
    AsyncDeleter(BackendRados & be, const std::string & name);
    void wait() override;
  private:
    /** A reference to the backend */
    BackendRados &m_backend;
    /** The object name */
    const std::string m_name;
    /** Storage for stat operation (size) */
    uint64_t m_size;
    /** Storage for stat operation (date) */
    time_t date;
    /** The promise that will both do the job and allow synchronization with the caller. */
    std::promise<void> m_job;
    /** The future from m_jobs, which will be extracted before any thread gets a chance to play with it. */
    std::future<void> m_jobFuture;
    /** A future used to hold the structure of the lock operation. It will be either empty of complete at 
     destruction time */
    std::unique_ptr<std::future<void>> m_lockAsync;
    /** A string used to identify the locker */
    std::string m_lockClient;
    /** A future the hole the the structure of the update operation. It will be either empty of complete at 
     destruction time */
    std::unique_ptr<std::future<void>> m_updateAsync;
    /** The first callback operation (after checking existence) */
    static void statCallback(librados::completion_t completion, void *pThis);
    /** The second callback operation (after deleting) */
    static void deleteCallback(librados::completion_t completion, void *pThis);
    /** Async delete in case of zero sized object */
    static void deleteEmptyCallback(librados::completion_t completion, void *pThis);
  };
  
  Backend::AsyncDeleter* asyncDelete(const std::string & name) override;
  
  /**
   * A class following up the check existence-lock-delete. TOBECHECK
   * Constructor implicitly starts the lock step.
   */
  class AsyncLockfreeFetcher: public Backend::AsyncLockfreeFetcher {
  public:
    AsyncLockfreeFetcher(BackendRados & be, const std::string & name);
    void wait() override;
    std::string get() override;
  private:
    /** A reference to the backend */
    BackendRados &m_backend;
    /** The object name */
    const std::string m_name;
    /** Storage for stat operation (size) */
    uint64_t m_size;
    /** Storage for stat operation (date) */
    time_t date;
    /** The rados bufferlist used to hold the object data (read+write) */
    ::librados::bufferlist m_radosBufferList;
    /** The promise that will both do the job and allow synchronization with the caller. */
    std::promise<std::string> m_job;
    /** The future from m_jobs, which will be extracted before any thread gets a chance to play with it. */
    std::future<std::string> m_jobFuture;
    /** The first callback operation (after checking existence) */
    static void statCallback(librados::completion_t completion, void *pThis);
    /** The second callback operation (after reading) */
    static void fetchCallback(librados::completion_t completion, void *pThis);
  };
  Backend::AsyncLockfreeFetcher* asyncLockfreeFetch(const std::string & name) override;
  
  class Parameters: public Backend::Parameters {
    friend class BackendRados;
  public:
    /**
     * The standard-issue params to string for logging
     * @return a string representation of the parameters for logging
     */
    std::string toStr() override;
    std::string toURL() override;
  private:
    std::string m_userId;
    std::string m_pool;
    std::string m_namespace;
  };
  
  Parameters * getParams() override;

  std::string typeName() override {
    return "cta::objectstore::BackendRados";
  }
  
private:
  std::string m_user;
  std::string m_pool;
  std::string m_namespace;
  librados::Rados m_cluster;
  librados::IoCtx m_radosCtx;
};

}} // end of cta::objectstore

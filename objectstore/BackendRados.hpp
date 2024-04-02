/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "Backend.hpp"
#include "rados/librados.hpp"
#include "common/threading/Mutex.hpp"
#include "common/threading/BlockingQueue.hpp"
#include "common/Timer.hpp"
#include "common/log/Logger.hpp"
#include "common/log/LogContext.hpp"
#include <future>

// RADOS_LOCKING can be NOTIFY or BACKOFF
#define BACKOFF (1)
#define NOTIFY (2)
#define RADOS_LOCKING_STRATEGY BACKOFF

namespace cta::objectstore {

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
  BackendRados(log::Logger & logger, const std::string & userId, const std::string & pool, const std::string &radosNameSpace = "");
  ~BackendRados() override;
  std::string user() {
    return m_user;
  }
  std::string pool() {
    return m_pool;
  }


  void create(const std::string& name, const std::string& content) override;

  void atomicOverwrite(const std::string& name, const std::string& content) override;

  std::string read(const std::string& name) override;

  void remove(const std::string& name) override;

  bool exists(const std::string& name) override;

  std::list<std::string> list() override;

  enum class LockType {
    Shared,
    Exclusive
  };

  class ScopedLock: public Backend::ScopedLock {
    friend class BackendRados;
  public:
    void release() override;
  private:
    inline void releaseBackoff();
    inline void releaseNotify();
  public:
    ~ScopedLock() override;
  private:
    explicit ScopedLock(librados::IoCtx& ioCtx) : m_lockSet(false), m_context(ioCtx), m_lockType(LockType::Shared) {}
    void set(const std::string & oid, const std::string& clientId, LockType lockType);
    bool m_lockSet;
    librados::IoCtx & m_context;
    std::string m_clientId;
    std::string m_oid;
    LockType m_lockType;
  };

  static const size_t c_maxBackoff;
  static const size_t c_backoffFraction;
  static const uint64_t c_maxWait;

  // This method was originally part of cta::exception::Errnum. As it is only used in the BackupRados class,
  // it has been moved here.
  template <typename F>
  static void throwOnReturnedErrnoOrThrownStdException(F f, std::string_view context = "") {
    try {
      exception::Errnum::throwOnReturnedErrno(f(), context);
    } catch(exception::Errnum&) {
      throw; // Let the exception of throwOnReturnedErrno pass through
    } catch(std::error_code& ec) {
      throw exception::Errnum(ec.value(), std::string(context) + " Got a std::error_code: " + ec.message());
    } catch(std::exception& ex) {
      throw exception::Exception(std::string(context) + " Got a std::exception: " + ex.what());
    }
  }

private:
  static std::string createUniqueClientId();
  /** This function will lock or die (actually throw, that is) */
  void lock(const std::string& name, uint64_t timeout_us, LockType lockType, const std::string & clientId);
  inline void lockBackoff(const std::string& name, uint64_t timeout_us, LockType lockType,
    const std::string & clientId, librados::IoCtx & radosCtx);
  inline void lockNotify(const std::string& name, uint64_t timeout_us, LockType lockType,
    const std::string & clientId, librados::IoCtx & radosCtx);

public:
  ScopedLock * lockExclusive(const std::string& name, uint64_t timeout_us=0) override;
  ScopedLock * lockShared(const std::string& name, uint64_t timeout_us=0) override;

private:
  /**
   * A class handling the watch part when waiting for a lock.
   */
  class LockWatcher {
  public:
    LockWatcher(librados::IoCtx & context, const std::string & name);
    virtual ~LockWatcher();
    typedef std::chrono::microseconds durationUs;
    void wait(const durationUs & timeout);
  private:
    /** An internal class containing the internals exposed to the callback of Rados.
     * The internals are kept separated so we can used asynchronous unwatch and forget
     * about the structure. The callback of aio_unwatch will take care of releasing the
     * object */
    struct Internal: public librados::WatchCtx2 {
      void handle_error(uint64_t cookie, int err) override;
      void handle_notify(uint64_t notify_id, uint64_t cookie, uint64_t notifier_id, librados::bufferlist& bl) override;
      static void deleter(librados::completion_t cb, void * i);
      // We could receive several notifications. The promise should be set only
      // on the first occurrence.
      threading::Mutex m_promiseMutex;
      bool m_promiseSet = false;
      std::promise<void> m_promise;
      std::future<void> m_future;
      std::string m_name;
    };
    std::unique_ptr<Internal> m_internal;
    librados::IoCtx & m_context;
    uint64_t m_watchHandle;
  };

private:
  /**
   * Base class for jobs handled by the thread-and-context pool.
   */
  class AsyncJob {
  public:
    virtual void execute()=0;
    virtual ~AsyncJob() = default;
  };

  /**
   * The queue for the thread-and-context pool.
   */
  cta::threading::BlockingQueue<AsyncJob *> m_jobQueue;

  /**
   * The class for the worker threads
   */
  class RadosWorkerThreadAndContext: private cta::threading::Thread {
  public:
    RadosWorkerThreadAndContext(BackendRados & parentBackend, int threadID, log::Logger & logger);
    virtual ~RadosWorkerThreadAndContext();
    void start() { cta::threading::Thread::start(); }
    void wait() { cta::threading::Thread::wait(); }
  private:
    BackendRados & m_parentBackend;
    const int m_threadID;
    log::LogContext m_lc;
    void run() override;
  };
  friend RadosWorkerThreadAndContext;

  /**
   * The container for the threads
   */
  std::vector<RadosWorkerThreadAndContext *> m_threads;

public:
  /**
   * A class following up the async creation. Constructor implicitly starts the creation.
   */
  class AsyncCreator: public Backend::AsyncCreator {
  public:
    AsyncCreator(BackendRados & be, const std::string & name, const std::string & value);
    void wait() override;
  private:
    /** A reference to the backend */
    BackendRados &m_backend;
    /** The object name */
    const std::string m_name;
    /** The content of object */
    std::string m_value;
    /** Storage for stat operation (date) in case of EEXIST (we retry on zero sized objects, others are a genuine error). */
    time_t m_time;
    /** Storage for stat operation (size) in case of EEXIST (we retry on zero sized objects, others are a genuine error). */
    uint64_t m_size;
    /** The promise that will both do the job and allow synchronization with the caller. */
    std::promise<void> m_job;
    /** The future from m_jobs, which will be extracted before any thread gets a chance to play with it. */
    std::future<void> m_jobFuture;
    /** The rados bufferlist used to hold the object data (read+write) */
    ::librados::bufferlist m_radosBufferList;
    /** Callback for the write operation */
    static void createExclusiveCallback(librados::completion_t completion, void *pThis);
    /** Callback for stat operation, handling potential retries after EEXIST */
    static void statCallback(librados::completion_t completion, void *pThis);
    /** Timer for retries (created only when needed */
    std::unique_ptr<cta::utils::Timer> m_retryTimer;
  };

  Backend::AsyncCreator* asyncCreate(const std::string& name, const std::string& value) override;

  /**
   * A class following up the lock-fetch-update-write-unlock. Constructor implicitly
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
    /** An async job that will process the update of the object. */
    class UpdateJob: public AsyncJob {
    public:
      void setParentUpdater (AsyncUpdater * updater) { m_parentUpdater = updater; }
      void execute() override;
    private:
      AsyncUpdater * m_parentUpdater = nullptr;
    };
    friend class UpdateJob;
    UpdateJob m_updateJob;
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
    /** The promise that will both do the job and allow synchronization with the caller. */
    std::promise<void> m_job;
    /** The future from m_jobs, which will be extracted before any thread gets a chance to play with it. */
    std::future<void> m_jobFuture;
    /** A future used to hold the structure of the lock operation. It will be either empty of complete at
     destruction time */
    std::unique_ptr<std::future<void>> m_lockAsync;
    /** A string used to identify the locker */
    std::string m_lockClient;
    /** The second callback operation (after deleting) */
    static void deleteCallback(librados::completion_t completion, void *pThis);
  };

  Backend::AsyncDeleter* asyncDelete(const std::string & name) override;

  /**
   * A class following up the async lockfree fetch.
   * Constructor implicitly starts the fetch step.
   */
  class AsyncLockfreeFetcher: public Backend::AsyncLockfreeFetcher {
  public:
    AsyncLockfreeFetcher(BackendRados & be, const std::string & name);
    std::string wait() override;
  private:
    /** A reference to the backend */
    BackendRados &m_backend;
    /** The object name */
    const std::string m_name;
    /** The aio posting task */
    class AioReadPoster: public AsyncJob {
    public:
      void setParentFatcher (AsyncLockfreeFetcher * fetcher) { m_parentFetcher = fetcher; }
      void execute() override;
    private:
      AsyncLockfreeFetcher * m_parentFetcher = nullptr;
    };
    friend class AioReadPoster;
    AioReadPoster m_aioReadPoster;
    /** The promise that will both do the job and allow synchronization with the caller. */
    std::promise<std::string> m_job;
    /** The future from m_jobs, which will be extracted before any thread gets a chance to play with it. */
    std::future<std::string> m_jobFuture;
    /** The rados bufferlist used to hold the object data (read+write) */
    ::librados::bufferlist m_radosBufferList;
    /** The callback for the fetch operation */
    static void fetchCallback(librados::completion_t completion, void *pThis);
  };

  Backend::AsyncLockfreeFetcher* asyncLockfreeFetch(const std::string& name) override;

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
  log::Logger& m_logger;
  std::string m_user;
  std::string m_pool;
  std::string m_namespace;
  librados::Rados m_cluster;
  std::vector<librados::IoCtx> m_radosCtxPool;
  cta::threading::Mutex m_radosCxtIndexMutex;
  size_t m_radosCtxIndex=0;
  librados::IoCtx & getRadosCtx();
};

} // namespace cta::objectstore

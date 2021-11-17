/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "Backend.hpp"
#include "rados/librados.hpp"
#include "common/threading/Mutex.hpp"
#include "common/threading/BlockingQueue.hpp"
#include "common/log/Logger.hpp"
#include "common/log/LogContext.hpp"
#include <future>

// RADOS_LOCKING can be NOTIFY or BACKOFF
#define BACKOFF (1)
#define NOTIFY (2)
#define RADOS_LOCKING_STRATEGY BACKOFF

// Define this to get long response times logging.
#define RADOS_SLOW_CALLS_LOGGING
#define RADOS_SLOW_CALLS_LOGGING_FILE "/var/tmp/cta-rados-slow-calls.log"
#define RADOS_SLOW_CALL_TIMEOUT 1

#ifdef RADOS_SLOW_CALLS_LOGGING
#include "common/Timer.hpp"
#include "common/threading/Mutex.hpp"
#include "common/threading/MutexLocker.hpp"
#include <fstream>
#include <iomanip>
#include <algorithm>
#include <syscall.h>
#endif //RADOS_SLOW_CALLS_LOGGING

#define RADOS_LOCK_PERFORMANCE_LOGGING_FILE "/var/tmp/cta-rados-locking.log"

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
    ScopedLock(librados::IoCtx & ioCtx): m_lockSet(false), m_context(ioCtx), m_lockType(LockType::Shared) {}
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
   * A class for logging the calls to rados taking too long.
   * If RADOS_SLOW_CALLS_LOGGING is not defined, this is just an empty shell.
   */
  class RadosTimeoutLogger {
  public:
    void logIfNeeded(const std::string & radosCall, const std::string & objectName) {
      #ifdef RADOS_SLOW_CALLS_LOGGING
      if (m_timer.secs() >= RADOS_SLOW_CALL_TIMEOUT) {
        cta::threading::MutexLocker ml(g_mutex);
        std::ofstream logFile(RADOS_SLOW_CALLS_LOGGING_FILE, std::ofstream::app);

        auto now = std::chrono::system_clock::now();
        auto duration = now.time_since_epoch();
        std::time_t end_time = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
        duration -= std::chrono::seconds(end_time);
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

        char date[80];
        int len = strftime(date, 80, "%b %e %T", localtime(&end_time));
        snprintf(date+len, 79-len, ".%06ld ", us);

        logFile << date << " PID=\"" << ::getpid() << "\" TID=\"" << syscall(SYS_gettid) << "\" op=\"" << radosCall
                << "\" obj=\"" << objectName << "\" duration=\"" << m_timer.secs() << "\"" << std::endl;
      }
      #endif //RADOS_SLOW_CALLS_LOGGING
    }
    void reset() {
      #ifdef RADOS_SLOW_CALLS_LOGGING
      m_timer.reset();
      #endif //RADOS_SLOW_CALLS_LOGGING
    }
  private:
    #ifdef RADOS_SLOW_CALLS_LOGGING
    cta::utils::Timer m_timer;
    static cta::threading::Mutex g_mutex;
    #endif //RADOS_SLOW_CALLS_LOGGING
  };

  /**
   * A class for logging lock timings and performance
   */
  class RadosLockTimingLogger {
  public:
    struct Measurements {
      size_t attempts = 0;
      size_t waitCount = 0;
      double totalTime = 0;
      double totalLatency = 0;
      double minLatency = 0;
      double maxLatency = 0;
      double totalWaitTime = 0;
      double minWaitTime = 0;
      double maxWaitTime = 0;
      double totalLatencyMultiplier = 0;
      double minLatencyMultiplier = 0;
      double maxLatencyMultiplier = 0;
      void addSuccess (double latency, double time);
      void addAttempt(double latency, double waitTime, double latencyMultiplier);
   };
    void addMeasurements(const Measurements & measurements);
    void logIfNeeded();
    ~RadosLockTimingLogger();
  private:
    struct CumulatedMesurements: public Measurements {
      size_t totalCalls = 0;
      size_t minAttempts = 0;
      size_t maxAttempts = 0;
      double minTotalTime = 0;
      double maxTotalTime = 0;
    };
    CumulatedMesurements m_measurements;
    threading::Mutex m_mutex;
    utils::Timer m_timer;
  };
  static RadosLockTimingLogger g_RadosLockTimingLogger;

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
      RadosTimeoutLogger m_radosTimeoutLogger;
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
    virtual ~AsyncJob() {}
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
    /** Instrumentation for rados calls timing */
    RadosTimeoutLogger m_radosTimeoutLogger;
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
    /** Instrumentation for rados calls timing */
    RadosTimeoutLogger m_radosTimeoutLogger;
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
    /** Instrumentation for rados calls timing */
    RadosTimeoutLogger m_radosTimeoutLogger;
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
    /** Instrumentation for rados calls timing */
    RadosTimeoutLogger m_radosTimeoutLogger;
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
  std::string m_user;
  std::string m_pool;
  std::string m_namespace;
  librados::Rados m_cluster;
  std::vector<librados::IoCtx> m_radosCtxPool;
  cta::threading::Mutex m_radosCxtIndexMutex;
  size_t m_radosCtxIndex=0;
  librados::IoCtx & getRadosCtx();
};

}} // end of cta::objectstore

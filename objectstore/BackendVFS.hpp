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

#include <functional>
#include <future>
#include <list>
#include <string>

#include "Backend.hpp"
#include "common/threading/Thread.hpp"

namespace cta::objectstore {

/**
 * An implementation of the object store primitives, using flock to lock,
 * so that several threads can compete for the same file (locks are per file
 * descriptor). The lock will be done on a special file (XXXX.lock), which will
 * not be moved (as opposed to the content itself, which needs to be moved to
 * be atomically created)
 */
class BackendVFS: public Backend {
 public:
  /**
   * Default constructor, generating automatically a directory in /tmp
   * Following the use of this constructor, the object store WILL be
   * destroyed by default on destruction. This can be overridden with
   * noDeleteOnExit()
   */
  explicit BackendVFS(int line = 0, const char *file = "");

  /**
   * Passive constructor, using an existing store. It will NOT destroy the
   * storage on exit, but this can be changed with deleteOnExit()
   * @param path
   */
  explicit BackendVFS(const std::string& path);

  /**
   * Instructs the object to not delete the storage on exit. This is useful
   * to create automatically a storage in /tmp, to then use it from several
   * programs (potentially in parallel).
   */
  void noDeleteOnExit();

  /**
   * Instructs the object to DO delete the storage on exit.
   */
  void deleteOnExit();

  ~BackendVFS() override;

  void create(const std::string& name, const std::string& content) override;

  void atomicOverwrite(const std::string& name, const std::string& content) override;

  std::string read(const std::string& name) override;

  void remove(const std::string& name) override;

  bool exists(const std::string& name) override;

  std::list<std::string> list() override;

  class ScopedLock: public Backend::ScopedLock {
    friend class BackendVFS;
  public:
    void release() override;
    ~ScopedLock() override { ScopedLock::release(); }
  private:
    ScopedLock(): m_fdSet(false), m_fd(0) {}
    void set(int fd, const std::string& path) { m_fd = fd; m_fdSet = true; m_path = path; }
    bool m_fdSet;
    std::string m_path;
    int m_fd;
  };

  ScopedLock * lockExclusive(const std::string& name, uint64_t timeout_us = 0) override;

  ScopedLock * lockShared(const std::string& name, uint64_t timeout_us = 0) override;

  /**
   * A class mimicking AIO using C++ async tasks
   */
  class AsyncCreator: public Backend::AsyncCreator {
   public:
    AsyncCreator(BackendVFS & be, const std::string & name, const std::string & value);
    void wait() override;
   private:
    /** A reference to the backend */
    BackendVFS &m_backend;
    /** The object name */
    const std::string m_name;
    /** The object value */
    std::string m_value;
     /** The future that will both do the job and allow synchronization with the caller. */
    std::future<void> m_job;
  };

  /**
   * A class mimicking AIO using C++ async tasks
   */
  class AsyncUpdater: public Backend::AsyncUpdater {
   public:
    AsyncUpdater(BackendVFS & be, const std::string & name, std::function <std::string(const std::string &)> & update);
    void wait() override;
   private:
    /** A reference to the backend */
    BackendVFS &m_backend;
    /** The object name */
    const std::string m_name;
    /** The operation on the object */
    std::function <std::string(const std::string &)> & m_update;
     /** The future that will both do the job and allow synchronization with the caller. */
    std::future<void> m_job;
  };

  /**
   * A class mimicking AIO using C++ async tasks
   */
  class AsyncDeleter: public Backend::AsyncDeleter {
   public:
    AsyncDeleter(BackendVFS & be, const std::string & name);
    void wait() override;
   private:
    /** A reference to the backend */
    BackendVFS &m_backend;
    /** The object name */
    const std::string m_name;
     /** The future that will both do the job and allow synchronization with the caller. */
    std::future<void> m_job;
  };

  /**
   * A class mimicking AIO using C++ async tasks
   */
  class AsyncLockfreeFetcher: public Backend::AsyncLockfreeFetcher, public cta::threading::Thread  {
   public:
    AsyncLockfreeFetcher(BackendVFS & be, const std::string & name);
    std::string wait() override;
   private:
    /** A reference to the backend */
    BackendVFS &m_backend;
    /** The object name */
    const std::string m_name;
    /** The fetched value */
    std::string m_value;
    /** The exception we might receive */
    std::exception_ptr m_exception = nullptr;
    /** A mutex to make helgrind happy */
    cta::threading::Mutex m_mutex;
    /** The thread that will both do the job and allow synchronization with the caller. */
    void run() override;
  };

  Backend::AsyncCreator* asyncCreate(const std::string& name, const std::string& value) override;

  Backend::AsyncUpdater* asyncUpdate(const std::string & name, std::function <std::string(const std::string &)> & update) override;

  Backend::AsyncDeleter* asyncDelete(const std::string & name) override;

  Backend::AsyncLockfreeFetcher* asyncLockfreeFetch(const std::string& name) override;

  class Parameters: public Backend::Parameters {
    friend class BackendVFS;
   public:
    /**
     * The standard-issue params to string for logging
     * @return a string representation of the parameters for logging
     */
    std::string toStr() override;

    /**
     * The standard-issue params to URL
     * @return a string representation of the parameters for logging
     */
    std::string toURL() override;

    /**
     * A more specific member, giving access to the path itself
     * @return the path to the VFS storage.
     */
    std::string getPath() { return m_path; }
   private:
    std::string m_path;
  };

  Parameters * getParams() override;


  std::string typeName() override {
    return "cta::objectstore::BackendVFS";
  }


 private:
  std::string m_root;
  bool m_deleteOnExit;
  ScopedLock * lockHelper(const std::string& name, int type, uint64_t timeout_us);
};


} // namespace cta::objectstore

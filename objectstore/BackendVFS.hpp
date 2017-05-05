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
#include <future>
#include <functional>

namespace cta { namespace objectstore {
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
  BackendVFS(int line=0, const char *file="");
  
  /**
   * Passive constructor, using an existing store. It will NOT destroy the 
   * storage on exit, but this can be changed with deleteOnExit()
   * @param path
   */
  BackendVFS(std::string path);
  
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

  void create(std::string name, std::string content) override;
  
  void atomicOverwrite(std::string name, std::string content) override;
  
  std::string read(std::string name) override;
  
  void remove(std::string name) override;
  
  bool exists(std::string name) override;
  
  std::list<std::string> list() override;
  
  class ScopedLock: public Backend::ScopedLock {
    friend class BackendVFS;
  public:
    void release() override;
    ~ScopedLock() override { release(); }
  private:
    ScopedLock(): m_fdSet(false) {}
    void set(int fd, const std::string & path) { m_fd=fd; m_fdSet=true; m_path=path; }
    bool m_fdSet;
    std::string m_path;
    int m_fd;
  };
  
  ScopedLock * lockExclusive(std::string name) override;

  ScopedLock * lockShared(std::string name) override;
  
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
  
  Backend::AsyncUpdater* asyncUpdate(const std::string & name, std::function <std::string(const std::string &)> & update) override;
  
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
  ScopedLock * lockHelper(std::string name, int type);
};


}} // end of cta::objectstore

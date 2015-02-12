#pragma once

#include "Backend.hpp"

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
  BackendVFS();
  
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
    
  virtual ~BackendVFS();

  virtual void create(std::string name, std::string content);
  
  virtual void atomicOverwrite(std::string name, std::string content);
  
  virtual std::string read(std::string name);
  
  virtual void remove(std::string name);
  
  virtual bool exists(std::string name);
  
  class ScopedLock: public Backend::ScopedLock {
    friend class BackendVFS;
  public:
    virtual void release();
    virtual ~ScopedLock() { release(); }
  private:
    ScopedLock(): m_fdSet(false) {}
    void set(int fd) { m_fd=fd; m_fdSet=true; }
    bool m_fdSet;
    int m_fd;
  };
  
  virtual ScopedLock * lockExclusive(std::string name);

  virtual ScopedLock * lockShared(std::string name);
  

  class Parameters: public Backend::Parameters {
    friend class BackendVFS;
  public:
    /**
     * The standard-issue params to string for logging
     * @return a string representation of the parameters for logging
     */
    virtual std::string toStr();
    
    /**
     * A more specific member, giving access to the path itself
     * @return the path to the VFS storage.
     */
    std::string getPath() { return m_path; }
  private:
    std::string m_path;
  };
  
  virtual Parameters * getParams();

private:
  std::string m_root;
  bool m_deleteOnExit;
  ScopedLock * lockHelper(std::string name, int type);
};


}} // end of cta::objectstore

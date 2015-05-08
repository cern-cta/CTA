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


namespace cta { namespace objectstore {
/**
 * An implementation of the object store primitives, using Rados.
 */
class BackendRados: public Backend {
public:
  /**
   * The constructor, connecting to the storage pool 'pool' using the user id
   * 'userId'
   * @param userId
   * @param pool
   */
  BackendRados(std::string userId, std::string pool);
  virtual ~BackendRados();
  virtual std::string user() {
    return m_user;
  }
  virtual std::string pool() {
    return m_pool;
  }
  

  virtual void create(std::string name, std::string content);
  
  virtual void atomicOverwrite(std::string name, std::string content);

  virtual std::string read(std::string name);
  
  virtual void remove(std::string name);
  
  virtual bool exists(std::string name);
  
  class ScopedLock: public Backend::ScopedLock {
    friend class BackendRados;
  public:
    virtual void release();
    virtual ~ScopedLock();
  private:
    ScopedLock(librados::IoCtx & ioCtx): m_lockSet(false), m_context(ioCtx) {}
    void set(const std::string & oid, const std::string clientId);
    bool m_lockSet;
    librados::IoCtx & m_context;
    std::string m_clientId;
    std::string m_oid;
  };
  
private:
  std::string createUniqueClientId();
  
public:  
  virtual ScopedLock * lockExclusive(std::string name);


  virtual ScopedLock * lockShared(std::string name);
  
  class Parameters: public Backend::Parameters {
    friend class BackendRados;
  public:
    /**
     * The standard-issue params to string for logging
     * @return a string representation of the parameters for logging
     */
    virtual std::string toStr();
  private:
    std::string m_userId;
    std::string m_pool;
  };
  
  virtual Parameters * getParams();

  virtual std::string typeName() {
    return "cta::objectstore::BackendRados";
  }
  
private:
  std::string m_user;
  std::string m_pool;
  librados::Rados m_cluster;
  librados::IoCtx m_radosCtx;
};

}} // end of cta::objectstore

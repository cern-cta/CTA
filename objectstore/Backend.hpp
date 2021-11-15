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

#include <string>
#include <list>
#include <functional>

#include "common/exception/Exception.hpp"

namespace cta { namespace objectstore {

/**
 * Interface to the backend stores that we can use.
 */

class Backend {
 public:
  virtual ~Backend() {}
  /**
   * Create an object (and possibly the necessary locking structures)
   * @param name name of the object
   * @param content the object's content
   */
  virtual void create(const std::string& name, const std::string& content) = 0;

  /**
   * Overwrite an existing object atomically
   * @param name name of the object
   * @param content new content of the object
   */
  virtual void atomicOverwrite(const std::string& name, const std::string& content) = 0;

  /**
   * Read the content of an object
   * @param name name of the object
   * @return the content of the object, as a string.
   */
  virtual std::string read(const std::string& name) = 0;

  /**
   * Delete an object (and possibly its locking structure)
   * @param name name of the object
   */
  virtual void remove(const std::string& name) = 0;

  /**
   * Tests the existence of the object
   * @param name
   * @return true if the object is found
   */
  virtual bool exists(const std::string& name) = 0;

  /**
   * Lists all objects
   * @return list of all object names
   */
  virtual std::list<std::string> list() = 0;

  /**
   * RAII class holding locks
   */
  class ScopedLock {
   public:
    /**
     * Explicitely releases the lock
     */
    virtual void release() = 0;

    /**
     * Destructor (implicitly releases the lock).
     */
    virtual ~ScopedLock() {}
  };

  /**
   * Locks the object shared
   * @param name name of the object
   * @return pointer to a newly created scoped lock object (for RAII)
   */
  virtual ScopedLock * lockShared(const std::string& name, uint64_t timeout_us = 0) = 0;

  /**
   * Locks the object exclusively
   * @param name name of the object
   * @return pointer to a newly created scoped lock object (for RAII)
   */
  virtual ScopedLock * lockExclusive(const std::string& name, uint64_t timeout_us = 0) = 0;

  /// A collection of exceptions allowing the user to find out which step failed.
  CTA_GENERATE_EXCEPTION_CLASS(WrongPreviousOwner);
  CTA_GENERATE_EXCEPTION_CLASS(CouldNotCreate);
  CTA_GENERATE_EXCEPTION_CLASS(CouldNotLock);
  CTA_GENERATE_EXCEPTION_CLASS(CouldNotFetch);
  CTA_GENERATE_EXCEPTION_CLASS(CouldNotUpdateValue);
  CTA_GENERATE_EXCEPTION_CLASS(CouldNotCommit);
  CTA_GENERATE_EXCEPTION_CLASS(CouldNotDelete);
  CTA_GENERATE_EXCEPTION_CLASS(CouldNotUnlock);
  CTA_GENERATE_EXCEPTION_CLASS(AsyncUpdateWithDelete);

  /**
   * A base class handling asynchronous creation of objects.
   */
  class AsyncCreator {
   public:
    /**
     * Waits for completion (success) of throws exception (failure).
     */
    virtual void wait() = 0;

    /**
     * Destructor
     */
    virtual ~AsyncCreator() {}
  };

  /**
   * Triggers the asynchronous object creator.
   *
   * @return pointer to a newly created AsyncUpdater (for RAII)
   */
  virtual AsyncCreator * asyncCreate(const std::string & name, const std::string & value) = 0;

  /**
   * A base class handling asynchronous sequence of lock exclusive, fetch, call user
   * operation, commit, unlock. Each operation will be asynchronous, and the result
   * (success or exception) will be returned via the wait() function call.
   */
  class AsyncUpdater {
   public:
    /**
     * Waits for completion (success) of throws exception (failure).
     */
    virtual void wait() = 0;

    /**
     * Destructor
     */
    virtual ~AsyncUpdater() {}
  };

  /**
   * Triggers the asynchronous object update sequence, as described in AsyncUpdater
   * class description.
   * @param update a callable/lambda that will receive the fetched value as a
   * parameter and return the updated value for commit.
   * @return pointer to a newly created AsyncUpdater (for RAII)
   */
  virtual AsyncUpdater * asyncUpdate(const std::string & name, std::function <std::string(const std::string &)> & update) = 0;


  /**
   * A base class handling asynchronous sequence of lock exclusive, delete.
   * Each operation will be asynchronous, and the result
   * (success or exception) will be returned via the wait() function call.
   */
  class AsyncDeleter {
   public:
    /**
     * Waits for completion (success) of throws exception (failure).
     */
    virtual void wait() = 0;

    /**
     * Destructor
     */
    virtual ~AsyncDeleter() {}
  };

  /**
   * Triggers the asynchronous object delete sequence, as described in
   * AsyncDeleter class description.
   *
   * @param name The name of the object to be deleted.
   * @return pointer to a newly created AsyncDeleter
   */
  virtual AsyncDeleter * asyncDelete(const std::string & name) = 0;

  /**
   * A base class handling asynchronous fetch (lockfree).
   * The operation will be asynchronous, and the result
   * (success or exception) will be returned via the wait() function call.
   */
  class AsyncLockfreeFetcher {
   public:
    /**
     * Waits for completion (success) of throws exception (failure).
     * The return value is the content of the object.
     */
    virtual std::string wait() = 0;

    /**
     * Destructor
     */
    virtual ~AsyncLockfreeFetcher() {}
  };

  /**
   * Triggers the asynchronous object fetch, as described in
   * AsyncLockfreeFetcher class description.
   *
   * @param name The name of the object to be deleted.
   * @return pointer to a newly created AsyncDeleter
   */
  virtual AsyncLockfreeFetcher * asyncLockfreeFetch(const std::string & name) = 0;

  /**
   * Base class for the representation of the parameters of the BackendStore.
   */
  class Parameters {
   public:
    /**
     * Turns parameter class into string representation
     * @return the string representation
     */
    virtual std::string toStr() = 0;

    /**
     * Turns parameter class into URL representation
     * @return the URL
     */
    virtual std::string toURL() = 0;

    /**
     * Virtual destructor
     */
    virtual ~Parameters() {}
  };

  /**
   * Returns a type specific representation of the parameters
   * @return pointer to the newly created representation.
   */
  virtual Parameters * getParams() = 0;

  /**
   * Return the name of the class. Mostly usefull in tests
   * @return name of the class
   */
  virtual std::string typeName() = 0;
};

}  // namespace objectstore
}  // namespace cta

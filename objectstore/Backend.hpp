#pragma once

#include <string>

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
  virtual void create(std::string name, std::string content) = 0;
  
  /**
   * Overwrite an existing object atomically
   * @param name name of the object
   * @param content new content of the object
   */
  virtual void atomicOverwrite(std::string name, std::string content) = 0;
  
  /**
   * Read the content of an object
   * @param name name of the object
   * @return the content of the object, as a string.
   */
  virtual std::string read(std::string name) = 0;
  
  /**
   * Delete an object (and possibly its locking structure)
   * @param name name of the object
   */
  virtual void remove(std::string name) = 0;
  
  /**
   * Tests the existence of the object
   * @param name 
   * @return true if the object is found
   */
  virtual bool exists(std::string name) = 0;  
  
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
  virtual ScopedLock * lockShared(std::string name) = 0;
  
  /**
   * Locks the object exclusively
   * @param name name of the object
   * @return pointer to a newly created scoped lock object (for RAII)
   */
  virtual ScopedLock * lockExclusive(std::string name) = 0;

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

}} // end of cta::objectstore


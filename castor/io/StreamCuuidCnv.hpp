#ifndef CASTOR_IO_CUUID_H
#define CASTOR_IO_CUUID_H

// Include Files
#include "castor/Exception.hpp"
#include "castor/IAddress.hpp"
#include "castor/ObjectCatalog.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/io/StreamBaseCnv.hpp"

namespace castor {

  // Forward declarations
  class IObject;

  namespace io {

    /**
     * class StreamCuuidCnv
     * A converter for marshalling/unmarshalling Cuuid into/from stl streams
     */
    class StreamCuuidCnv : public StreamBaseCnv {

    public:

      /**
       * Constructor
       */
      StreamCuuidCnv();

      /**
       * Destructor
       */
      virtual ~StreamCuuidCnv();

      /**
       * Gets the object type.
       * That is the type of object this converter can convert
       */
      static const unsigned int ObjType();

      /**
       * Gets the object type.
       * That is the type of object this converter can convert
       */
      virtual const unsigned int objType() const;

      /**
       * Creates foreign representation from a C++ Object.
       * @param address where to store the representation of
       * the object
       * @param object the object to deal with
       * @param alreadyDone the set of objects which representation
       * was already created. This is needed to avoid looping in case
       * of circular dependencies
       * @param autocommit whether the changes to the database
       * should be commited or not
       * @exception Exception throws an Exception in cas of error
       */
      virtual void createRep(castor::IAddress* address,
                             castor::IObject* object,
                             castor::ObjectSet& alreadyDone,
                             bool autocommit = true)
        throw (castor::Exception);

      /**
       * Updates foreign representation from a C++ Object.
       * @param address where the representation of
       * the object is stored
       * @param object the object to deal with
       * @param alreadyDone the set of objects which representation
       * was already updated. This is needed to avoid looping in case
       * of circular dependencies
       * @param autocommit whether the changes to the database
       * should be commited or not
       * @exception Exception throws an Exception in cas of error
       */
      virtual void updateRep(castor::IAddress* address,
                             castor::IObject* object,
                             castor::ObjectSet& alreadyDone,
                             bool autocommit = true)
        throw (castor::Exception);

      /**
       * Deletes foreign representation of a C++ Object.
       * @param address where the representation of
       * the object is stored
       * @param object the object to deal with
       * @param alreadyDone the set of objects which representation
       * was already deleted. This is needed to avoid looping in case
       * of circular dependencies
       * @param autocommit whether the changes to the database
       * should be commited or not
       * @exception Exception throws an Exception in cas of error
       */
      virtual void deleteRep(castor::IAddress* address,
                             castor::IObject* object,
                             castor::ObjectSet& alreadyDone,
                             bool autocommit = true)
        throw (castor::Exception);

      /**
       * Creates C++ object from foreign representation
       * @param address the place where to find the foreign
       * representation
       * @param newlyCreated a map of object that were created as part of the
       * last user call to createObj, indexed by id. If a reference to one if
       * these id is found, the existing associated object should be used.
       * This trick basically allows circular dependencies.
       * @return the C++ object created from its reprensentation
       * or 0 if unsuccessful. Note that the caller is responsible
       * for the deallocation of the newly created object
       * @exception Exception throws an Exception in cas of error
       */
      virtual castor::IObject* createObj(castor::IAddress* address,
                                         castor::ObjectCatalog& newlyCreated)
        throw (castor::Exception);

    }; // end of class StreamCuuidCnv

  }; // end of namespace io

}; // end of namespace castor

#endif // CASTOR_IO_CUUID_H

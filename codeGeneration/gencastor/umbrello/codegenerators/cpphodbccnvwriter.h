#ifndef CODEGENERATORS_CPPHODBCCNVWRITER_H 
#define CODEGENERATORS_CPPHODBCCNVWRITER_H 1

// Include files

// Local includes
#include "cpphbasecnvwriter.h"

class CppHOdbcCnvWriter : public CppHBaseCnvWriter {

 public:

  /**
   * Constructor
   */
  CppHOdbcCnvWriter(UMLDoc *parent, const char *name);

  /**
   * Destructor
   */
  ~CppHOdbcCnvWriter() {};

 public:

  /**
   * Initializes the writer. Only calls the method of
   * the parent, then fixes the namespace and calls postinit.
   */
  virtual bool init(UMLClassifier* c, QString fileName);

  /**
   * write the content of the file
   */
  void writeClass(UMLClassifier *c);

 private:

  /// writes members declarations
  void writeMembers();

};

#endif // CODEGENERATORS_CPPHODBCCNVWRITER_H

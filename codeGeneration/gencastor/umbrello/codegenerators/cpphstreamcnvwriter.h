#ifndef CODEGENERATORS_CPPHSTREAMCNVWRITER_H 
#define CODEGENERATORS_CPPHSTREAMCNVWRITER_H 1

// Include files

// Local includes
#include "cpphbasecnvwriter.h"

class CppHStreamCnvWriter : public CppHBaseCnvWriter {

 public:

  /**
   * Constructor
   */
  CppHStreamCnvWriter(UMLDoc *parent, const char *name);

  /**
   * Destructor
   */
  ~CppHStreamCnvWriter() {};

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

};

#endif // CODEGENERATORS_CPPHSTREAMCNVWRITER_H

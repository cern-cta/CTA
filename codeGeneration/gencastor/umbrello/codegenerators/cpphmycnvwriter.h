#ifndef CODEGENERATORS_CPPHMYCNVWRITER_H 
#define CODEGENERATORS_CPPHMYCNVWRITER_H 1

// Include files

// Local includes
#include "cpphbasecnvwriter.h"

class CppHMyCnvWriter : public CppHBaseCnvWriter {

 public:

  /**
   * Constructor
   */
  CppHMyCnvWriter(UMLDoc *parent, const char *name);

  /**
   * Destructor
   */
  ~CppHMyCnvWriter() {};

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

  /**
   * write the reset method declaration
   */
  void writeReset(UMLClassifier *c);

  /// writes fillRep methods declaration
  void writeFillRep();
    
  /// writes fillObj methods declaration
  void writeFillObj();
    
  /// writes one basic fillRep method declaration
  void writeBasicFillRep(Assoc* as);
    
  /// writes one basic fillObj method declaration
  void writeBasicFillObj(Assoc* as);
    
};

#endif // CODEGENERATORS_CPPHMYCNVWRITER_H

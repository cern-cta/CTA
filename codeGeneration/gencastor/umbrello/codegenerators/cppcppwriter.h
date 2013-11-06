#ifndef CODEGENERATORS_CPPCPPWRITER_H 
#define CODEGENERATORS_CPPCPPWRITER_H 1

// Include files
#include <qfile.h>

// Local includes
#include "cppbasewriter.h"

class CppCppWriter : public CppBaseWriter {

 public:
  /**
   * Constructor
   */
  CppCppWriter(UMLDoc *parent, const char *name);

  /**
   * Destructor
   */
  ~CppCppWriter() {};

  /**
   * Initializes the writer. This means creating the file
   * and writing some header in it.
   * It also inserts the header file in the list of includes
   */
  virtual bool init(UMLClassifier* c, QString fileName);

  /**
   * End of the initialization of the writer.
   * It creates the file and writes some header in it.
   */
  virtual bool postinit(UMLClassifier* c, QString fileName);

  /**
   * Finalizes the writer. This means writing the footer
   * of the file and closing it
   */
  virtual bool finalize(UMLClassifier* c);

};

#endif // CODEGENERATORS_CPPCPPWRITER_H

#ifndef CODEGENERATORS_CPPHWRITER_H 
#define CODEGENERATORS_CPPHWRITER_H 1

// Include files 
#include <qfile.h>
#include <qstring.h>
#include <qtextstream.h>

// Local includes
#include "cppbasewriter.h"
#include "../classifier.h"
#include "../umldoc.h"

class CppHWriter : public CppBaseWriter {
  
 public:
  /**
   * Constructor
   */
  CppHWriter(UMLDoc *parent, const char *name);

  /**
   * Destructor
   */
  ~CppHWriter() {};
   
  /**
   * Initializes the writer. Only calls the method of
   * the parent and postinit. Any overload of this function
   * should keep this two calls.
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
  virtual bool finalize();

  /**
   * Writes after the clasing of all scopes.
   * Called by finalize. Usefull for inline implementations.
   */
  virtual void writeAfterNSClose() {}

};

#endif // CODEGENERATORS_CPPHWRITER_H

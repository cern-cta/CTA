#ifndef CODEGENERATORS_CPPHBASECNVWRITER_H 
#define CODEGENERATORS_CPPHBASECNVWRITER_H 1

// Include files

// Local includes
#include "cpphwriter.h"

class CppHBaseCnvWriter : public CppHWriter {

 public:

  /**
   * Constructor
   */
  CppHBaseCnvWriter(UMLDoc *parent, const char *name);

  /**
   * Destructor
   */
  ~CppHBaseCnvWriter() {};

 protected:

  /**
   * writes methods declarations
   * @param delUpMethods whether to generate methods
   * for deletion and update of objects, default is yes
   */
  virtual void writeMethods(bool delUpMethods = true);

  /// get the class prefix
  QString& prefix() { return m_prefix; }

  /// set the class prefix
  void setPrefix(QString p) { m_prefix = p; }

  /// writes class declaration
  void writeClassDecl(const QString& doc);

 private:
  /// A prefix for the class names of the converters
  QString m_prefix;

};

#endif // CODEGENERATORS_CPPHBASECNVWRITER_H

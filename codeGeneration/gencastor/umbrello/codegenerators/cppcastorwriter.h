#ifndef CODEGENERATORS_CPPCASTORWRITER_H 
#define CODEGENERATORS_CPPCASTORWRITER_H 1

// Includes
#include <map>
#include <set>
#include <qstring.h>

// Local includes
#include "simplecodegenerator.h"
#include "../classifier.h"

/**
 * Castor cpp writer.
 * Codegenerator with a knowledge of castor predefined types
 */
class CppCastorWriter : public SimpleCodeGenerator {

 public:
  /** Constructor */
  CppCastorWriter(UMLDoc *parent, const char *name);

  /** Constructor */
  virtual ~CppCastorWriter() {};

  virtual bool isType (QString & type) { return type == "CppWriter"; }
  virtual QString getLanguage() { return "Cpp"; }

 protected:
  /**
   * Map of castor types with the associated include files
   */
  std::map<QString, QString> m_castorTypes;

  /**
   * Says whether a castor type is complex or not
   * Complex means basically that it is a struct
   */
  std::map<QString, bool> m_castorIsComplexType;

  /** The list of classes to fully ignore */
  std::set<QString> m_ignoreClasses;

};


#endif // CODEGENERATORS_CPPCASTORWRITER_H

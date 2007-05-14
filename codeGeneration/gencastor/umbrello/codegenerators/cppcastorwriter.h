#ifndef CODEGENERATORS_CPPCASTORWRITER_H 
#define CODEGENERATORS_CPPCASTORWRITER_H 1

// Includes
#include <map>
#include <set>
#include <qstring.h>

// Local includes
#include "../codegenerator.h"
#include "simplecodegenerator.h"
#include "../classifier.h"
#include "../datatype.h"

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
  virtual Uml::Programming_Language getLanguage() { return Uml::pl_Cpp; }

  /**
   * Helper method for opening a file
   * This is a replacement for CodeGenerator::openFile
   * that is not able to cope with the mode
   */
  bool openFile (QFile & file, QString fileName, int mode);

  /**
   * computes a fileName for a given class and a given extension
   * Written from the simpleCodeGenerator::findFileName method
   */
	QString computeFileName(UMLClassifier* concept, QString ext);

  /**
   * accessor to topNS
   */
  void setTopNS(QString value) { s_topNS = value; }

  /**
   * Gets the base type for a given type
   */
  static QString getSimpleType(QString type);

  /**
   * Gets the classifier for a given type
   */
  UMLClassifier* getClassifier(QString type);

  /**
   * Gets the datatype for a given type
   */
  UMLDatatype* getDatatype(QString type);

  /** The list of associations and members to ignore, except for dB stuff */
  std::set<QString> m_ignoreButForDB;

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

  /** The top namespace of the generated software */
  static QString s_topNS;

};


#endif // CODEGENERATORS_CPPCASTORWRITER_H

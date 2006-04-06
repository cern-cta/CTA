#ifndef CODEGENERATORS_CPPHCLASSWRITER_H 
#define CODEGENERATORS_CPPHCLASSWRITER_H 1

// Include files

// Local includes
#include "cpphwriter.h"
#include "umlrole.h"

class CppHClassWriter : public CppHWriter {

 public:
  /**
   * Constructor
   */
  CppHClassWriter(UMLDoc *parent, const char *name);

  /**
   * Destructor
   */
  ~CppHClassWriter() {};

 public:
  /**
   * write the content of the file
   */
  void writeClass(UMLClassifier *c);

  /**
   * Writes after the clasing of all scopes.
   * Called by finalize. Usefull for inline implementations.
   */
  virtual void writeAfterNSClose();

 private:
  /**
   * Writes class documentation and the class header
   */
  void writeClassDecl(UMLClassifier *c, QTextStream &cpp);

	/**
	 * Writes the constructor and destrcutor declarations
	 */
	void writeConstructorDecls(QTextStream &h); 

  /**
   * Writes all accessor methods for attributes and associations
   */
  void writeHeaderAccessorMethodDecl(UMLClassifier *c,
                                     Uml::Visibility permitVisibility,
                                     QTextStream &stream);

  /**
   * Writes accessor methods for attributes
   */
  void writeHeaderAttributeAccessorMethods (Uml::Visibility visibility,
                                            bool writeStatic,
                                            QTextStream &stream);

  /**
   * Write attributes and associations members declarations

   */
  void writeHeaderFieldDecls(UMLClassifier *c,
                            QTextStream &stream);

  /**
   * writes the Attribute declarations
   * @param visibility the visibility of the attribs to print out
   * @param writeStatic whether to write static or non-static attributes out
   * @param stream text stream
   */
  void writeAttributeDecls (Uml::Visibility visibility,
                            bool writeStatic,
                            QTextStream &stream);

  /**
   * Searches a list of associations for appropriate ones to write
   * out as attributes
   */
  void writeAssociationDecls(QPtrList<UMLAssociation> associations,
                             Uml::Visibility permit,
                             Uml::IDType id,
                             QTextStream &stream);

  /**
   * Writes out an association as an attribute using Vector
   */
  void writeAssociationRoleDecl(QString fieldClassName,
                                QString roleName,
                                Multiplicity multi,
                                QString doc,
                                QTextStream &stream);

  /**
   * Write associations accessors
   */
  void writeAssociationMethods (QPtrList<UMLAssociation> associations,
                                Uml::Visibility permitVisib,
                                bool writePointerVar,
                                Uml::IDType myID,
                                QTextStream &stream);
	/**
	 * calls @ref writeSingleAttributeAccessorMethods() or @ref
	 * writeVectorAttributeAccessorMethods() on the assocaition
	 * role
	 */
	void writeAssociationRoleMethod(QString fieldClassName,
                                  QString fieldNamespace,
                                  QString roleName,
                                  Multiplicity multi, 
                                  QString description,
                                  Uml::Changeability_Type change,  
                                  QTextStream &stream); 

	/**
	 * calls @ref writeSingleAttributeAccessorMethods() on each of the
   * attributes in attribs list.
	 */
	void writeAttributeMethods(QPtrList<UMLAttribute> *attribs,
                             QTextStream &stream);


	/**
	 * Writes getFoo() and setFoo() accessor methods for the attribute
	 */
	void writeSingleAttributeAccessorMethods(QString fieldClassName,
                                           QString fieldNamespace,
                                           QString fieldVarName,
                                           QString fieldName,
                                           QString description, 
                                           Uml::Changeability_Type change,
                                           bool isStatic,
                                           QTextStream &cpp);

	/**
	 * Writes addFoo() and removeFoo() accessor methods for the Vector attribute
	 */
	void writeVectorAttributeAccessorMethods(QString fieldClassName,
                                           QString fieldNamespace,
                                           QString fieldVarName,
                                           QString fieldName,
                                           QString description, 
                                           Uml::Changeability_Type change,
                                           QTextStream &cpp);

  /** builds an attribute name */
  QString getAttributeVariableName (UMLAttribute *at) {
    return QString("m_" + at->getName());
  }

};

#endif // CODEGENERATORS_CPPHCLASSWRITER_H

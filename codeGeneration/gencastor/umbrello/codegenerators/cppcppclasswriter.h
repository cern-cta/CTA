#ifndef CODEGENERATORS_CPPCPPCLASSWRITER_H 
#define CODEGENERATORS_CPPCPPCLASSWRITER_H 1

// Include files

// Local includes
#include "cppcppwriter.h"

class CppCppClassWriter : public CppCppWriter {

 public:
  /**
   * Constructor
   */
  CppCppClassWriter(UMLDoc *parent, const char *name);

  /**
   * Destructor
   */
  ~CppCppClassWriter() {};

 public:
  /**
   * write the content of the file
   */
  void writeClass(UMLClassifier *c);

 private:
  /**
   * Writes constructor and destructor
   */
	void writeConstructorMethods(QTextStream &cpp); 

  /**
   * Writes the initialization of a member in the constructor
   */
  void writeInitInConstructor(QString name,
                              QString type,
                              bool& first);

  /**
   * Writes the initialization of an association in the constructor
   */
  void writeAssocInitInConstructor (UMLAssociation *a,
                                    bool& first);

  /**
   * Writes the deletion of a member in the destructor
   * @param name the name of the member to be deleted
   * @param backName the name of the member in the object
   * to delete referencing the current object
   * @param multi multiplicity of the member to be deleted
   * @param backMulti the multiplicity of the member in the object
   * to delete referencing the current object
   * @param fulldelete whether to delete children or not. In
   * case not, only the relation is deleted
   */
  void writeDeleteInDestructor(QString name,
                               QString backName,
                               Multiplicity multi,
                               Multiplicity backMulti,
                               bool fulldelete = true);

  /**
   * Writes the deletion of an association in the destructor
   */
  void writeAssocDeleteInDestructor (UMLAssociation *a);

  /**
   * Writes the implementation of the id method
   */
  static void writeId(CppBaseWriter* obj,
                      QTextStream &stream);

  /**
   * Writes the implementation of the setId method
   */
  static void writeSetId(CppBaseWriter* obj,
                         QTextStream &stream);

  /**
   * Writes the implementation of the type method
   */
  static void writeType(CppBaseWriter* obj,
                        QTextStream &stream);

  /**
   * Writes the implementation of the TYPE method
   */
  static void writeTYPE(CppBaseWriter* obj,
                        QTextStream &stream);

  /**
   * Writes the implementation of the Clone method
   */
  static void writeClone(CppBaseWriter* obj,
                         QTextStream &stream);

  /**
   * Writes the print method with 3 arguments
   */
  static void writeFullPrint(CppBaseWriter* obj,
                             QTextStream &stream);

  /**
   * Writes the print method with no arguments
   */
  static void writePrint(CppBaseWriter* obj,
                         QTextStream &stream);

  /**
   * Writes a print statement for an association
   */
  static void writeAssocPrint(UMLAssociation * a,
                              CppBaseWriter* obj,
                              QTextStream &stream);

  /**
   * writes a simple print statement
   */
  static void writeSimplePrint(QString indent,
                               QString name,
                               QTextStream &stream);
  /**
   * writes an enum print statement
   */
  static void writeEnumPrint(QString indent,
                             QString name,
                             QString type,
                             QTextStream &stream);
  /**
   * Writes a print statement for a one to one association
   */
  static void writeAssoc1Print(QString name,
                               QString type,
                               CppBaseWriter* obj,
                               QTextStream &stream);
  
  /**
   * Writes a print statement for a one to n association
   */
  static void writeAssocNPrint(QString name,
                               QString type,
                               CppBaseWriter* obj,
                               QTextStream &stream);

  /**
   * Writes a print statement for a Cuuid object
   */
  static void writeCuuidPrint(CppBaseWriter* obj,
                              QTextStream &stream);

  /**
   * Writes a print statement for a StringResponse object
   */
  static void writeStringResponsePrint(CppBaseWriter* obj,
                                       QTextStream &stream);

};

#endif // CODEGENERATORS_CPPCPPCLASSWRITER_H

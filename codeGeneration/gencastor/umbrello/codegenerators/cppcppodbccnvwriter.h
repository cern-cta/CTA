#ifndef CODEGENERATORS_CPPCPPODBCCNVWRITER_H 
#define CODEGENERATORS_CPPCPPODBCCNVWRITER_H 1

// Include files

// Local includes
#include "cppcppbasecnvwriter.h"

class CppCppOdbcCnvWriter : public CppCppBaseCnvWriter {

 public:
  /**
   * Constructor
   */
  CppCppOdbcCnvWriter(UMLDoc *parent, const char *name);

  /**
   * Destructor
   */
  ~CppCppOdbcCnvWriter() {};

 public:

  /**
   * Initializes the writer. Only calls the method of
   * the parent, then fixes the namespace and calls postinit.
   * It also inserts the header file in the list of includes
   */
  virtual bool init(UMLClassifier* c, QString fileName);

  /**
   * write the content of the file
   */
  void writeClass(UMLClassifier *c);

 private:

  /// writes static constants initialization
  void writeConstants();

  /// writes SQL statements for creation/deletion of the database
  void writeSqlStatements();
    
  /// writes constructor and destructor
  void writeConstructors();

  /// writes createRep method's content
  void writeCreateRepContent();
    
  /// writes updateRep method's content
  void writeUpdateRepContent();
    
  /// writes deleteRep method's content
  void writeDeleteRepContent();
    
  /// writes updateObj method's content
  void writeUpdateObjContent();
    
  /// writes createObj method's content
  void writeCreateObjContent();

  /**
   * write a call to set*** for attribute.
   * @param statement the statement name, ie "insert" or "update"
   * @param attr the attribute given by the pair name, type
   * @param n the number of this attribute
   * @param isAssoc whether this attribute is issued from
   * an association. Default is not.
   * @param isAssoc whether this attribute is an enumeration type
   */
  void writeSingleSetIntoStatement(QString statement,
                                   Member pair,
                                   int n,
                                   bool isAssoc = false,
                                   bool isEnum = false);

  /**
   * write a call to get*** for attribute.
   * @param attr the attribute given by the pair name, type
   * @param n the number of this attribute
   * @param isAssoc whether this attribute is issued from
   * an association. Default is not.
   */
  void writeSingleGetFromSelect(Member mem,
                                int n,
                                bool isAssoc = false);

  /**
   * write a call to SQLBindCol for attribute.
   * @param attr the attribute
   * @param n the number of this attribute
   * @param isAssoc whether this attribute is issued from
   * an association. Default is not.
   */
  void writeSingleColBind(Member mem,
                          int n,
                          bool isAssoc = false);

  /**
   * Gets the ODBC type corresponding to a given C++ type.
   * The ODBC type is the one used in OCCI to set/get
   * members into/from statements
   */
  QString getOdbcType(QString& type);
    
  /**
   * Gets the SQL type corresponding to a given C++ type.
   * The SQL type is the one used in an SQL statement to
   * create the table
   */
  QString getSQLType(QString& type);

  /**
   * Says whether the current object is a request
   */
  bool isRequest();

};

#endif // CODEGENERATORS_CPPCPPODBCCNVWRITER_H

#ifndef CODEGENERATORS_CPPCPPORACNVWRITER_H 
#define CODEGENERATORS_CPPCPPORACNVWRITER_H 1

// Include files

// Local includes
#include "cppcppbasecnvwriter.h"

class CppCppOraCnvWriter : public CppCppBaseCnvWriter {

 public:
  /**
   * Constructor
   */
  CppCppOraCnvWriter(UMLDoc *parent, const char *name);

  /**
   * Destructor
   */
  ~CppCppOraCnvWriter() {};

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

  /// writes reset method
  void writeReset();

  /// Chooses an order for 2 members in an assoc
  void ordonnateMembersInAssoc(Assoc* as,
                               Member** firstMember,
                               Member** secondMember);
  /// writes fillRep methods
  void writeFillRep();
    
  /// writes fillObj methods
  void writeFillObj();
    
  /// writes one basic fillRep method for one to one assoc
  void writeBasicMult1FillRep(Assoc* as,
                              unsigned int n);
    
  /// writes one basic fillObj method for one to one assoc
  void writeBasicMult1FillObj(Assoc* as,
                              unsigned int n);
    
  /// writes one basic fillRep method for one to n assoc
  void writeBasicMultNFillRep(Assoc* as);
    
  /// writes one basic fillObj method for one to n assoc
  void writeBasicMultNFillObj(Assoc* as);
    
  /// writes createRep method's content
  void writeCreateRepContent();
    
  /// writes updateRep method's content
  void writeUpdateRepContent();
    
  /// writes deleteRep method's content
  void writeDeleteRepContent();
    
  /// writes createObj method's content
  void writeCreateObjContent();
    
  /// writes updateObj method's content
  void writeUpdateObjContent();

  /**
   * write a call to set*** for attribute.
   * @param statement the statement name, ie "insert" or "update"
   * @param attr the attribute given by the pair name, type
   * @param n the number of this attribute
   * @param isEnum whether this attribute is an enumeration type
   */
  void writeSingleSetIntoStatement(QString statement,
                                   Member pair,
                                   int n,
                                   bool isEnum = false);

  /**
   * write a call to get*** for attribute.
   * @param attr the attribute given by the pair name, type
   * @param n the number of this attribute
   * @param isAssoc whether this attribute is issued from
   * an association. Default is not.
   * @param isEnum whether this attribute is an enumeration type
   */
  void writeSingleGetFromSelect(Member pair,
                                int n,
                                bool isAssoc = false,
                                bool isEnum = false);

  /**
   * Gets the ORACLE type corresponding to a given C++ type.
   * The Oracle type is the one used in OCCI to set/get
   * members into/from statements
   */
  QString getOraType(QString& type);
    
  /**
   * Gets the SQL type corresponding to a given C++ type.
   * The SQL type is the one used in an SQL statement to
   * create the table
   */
  QString getSQLType(QString& type);

  /**
   * Writes code to display an SQL error with all details
   */
  void printSQLError(QString name,
                     MemberList& members,
                     AssocList& assocs);

  /**
   * Says whether the current object is a request
   */
  bool isRequest();

  /**
   * creates file oracle.sql and write beginning of it
   */
  void startSQLFile();

 private:

  // Says whether it is the first call to writeClass
  bool m_firstClass;

};

#endif // CODEGENERATORS_CPPCPPORACNVWRITER_H

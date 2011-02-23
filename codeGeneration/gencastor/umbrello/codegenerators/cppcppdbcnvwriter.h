#ifndef CODEGENERATORS_CPPCPPDBCNVWRITER_H 
#define CODEGENERATORS_CPPCPPDBCNVWRITER_H 1

// Include files

// Local includes
#include "cppcppbasecnvwriter.h"

class CppCppDbCnvWriter : public CppCppBaseCnvWriter {

 public:
  /**
   * Constructor
   */
  CppCppDbCnvWriter(UMLDoc *parent, const char *name);

  /**
   * Destructor
   */
  ~CppCppDbCnvWriter();

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

  /// writes SQL statements for creation/deletion of the databases
  void writeOraSqlStatements();
  void writePgSqlStatements();

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
  void writeBasicMult1FillRep(Assoc* as);
    
  /// writes one basic fillObj method for one to one assoc
  void writeBasicMult1FillObj(Assoc* as,
                              unsigned int n);
    
  /// writes one basic fillRep method for one to n assoc
  void writeBasicMultNFillRep(Assoc* as);
    
  /// writes one basic fillObj method for one to n assoc
  void writeBasicMultNFillObj(Assoc* as);

  /// writes the check statements part of the (bulk)createRep method
  void writeCreateRepCheckStatements(QTextStream &stream,
                                     MemberList &members,
                                     AssocList &assocs,
                                     bool bulk);

  /// writes the check statements part of the (bulk)createObj method
  void writeCreateObjCheckStatements(QString name);

  /**
   * writes the creation of an object from a resultSet in
   * the (bulk)createObj method
   */
  void writeBulkCreateObjCreateObject(MemberList& members,
                                      AssocList& assocs);

  /// writes the buffer creation code for one member in bulkCreateRep
  void writeCreateBufferForSelect(QTextStream &stream,
                                  bool &typeUsed,
                                  QString name,
                                  QString typeName,
                                  int n,
                                  QString stmt,
                                  bool isEnum = false,
                                  bool isAssoc = false,
                                  QString remoteTypeName = "");

  /// writes createRep method's content
  void writeCreateRepContent(QTextStream &stream, bool &addressUsed,
                             bool &endTransUsed, bool &typeUsed);
    
  /// writes bulkCreateRep method's content
  void writeBulkCreateRepContent(QTextStream &stream, bool &addressUsed,
                                 bool &typeUsed);
    
  /// writes updateRep method's content
  void writeUpdateRepContent(QTextStream &stream,
                             bool &addressUsed);

  /// writes deleteRep method's content
  void writeDeleteRepContent(QTextStream &stream,
                             bool &addressUsed);
    
  /// writes createObj method's content
  void writeCreateObjContent();
    
  /// writes bulkCreateObj method's content
  void writeBulkCreateObjContent();
    
  /// writes updateObj method's content
  void writeUpdateObjContent();

  /**
   * write a call to set*** for attribute.
   * @param stream the stream into which the code should be written
   * @param statement the statement name, ie "insert" or "update"
   * @param attr the attribute given by the pair name, type
   * @param n the number of this attribute
   * @param isEnum whether this attribute is an enumeration type
   */
  void writeSingleSetIntoStatement(QTextStream &stream,
                                   QString statement,
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
   * Gets the DBCLE type corresponding to a given C++ type.
   * The Dbcle type is the one used to set/get
   * members into/from statements
   */
  QString getDbType(QString& type);
    
  /**
   * Gets the DBCLE C type corresponding to a given C++ type.
   * The Dbcle C type is the one used in bulk operations
   * for handling the buffers
   */
  QString getDbCType(QString& type);
    
  /**
   * Gets the SQL type corresponding to a given C++ type.
   * The SQL type is the one used in an SQL statement to
   * create the table. Both Oracle and Postgres versions
   * are provided.
   */
  QString getOraSQLType(QString& type);

  QString getPgSQLType(QString& type);

  /**
   * Gets the db type constant corresponding to a given C++ type.
   */
  QString getDbTypeConstant(QString type);

  /**
   * Writes code to display an SQL error with all details
   */
  void printSQLError(QTextStream &stream,
                     QString name,
                     MemberList& members,
                     AssocList& assocs,
                     bool useAutoCommit);

  /**
   * Says whether the current object is a request that
   * needs storage in the newRequests table
   */
  bool isNewRequest();

  /**
   * creates file oracle.sql and write beginning of it
   */
  void startSQLFile();

  /**
   * write end of file oracle.sql
   */
  void endSQLFile();

  /**
   * insert the content of a file into a text stream
   */
  void insertFileintoStream(QTextStream &stream,
                            QString fileName);

 private:

  // Says whether it is the first call to writeClass
  bool m_firstClass;
};

#endif // CODEGENERATORS_CPPCPPDBCNVWRITER_H

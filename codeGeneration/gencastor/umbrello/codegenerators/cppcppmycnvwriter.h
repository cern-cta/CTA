#ifndef CODEGENERATORS_CPPCPPMYCNVWRITER_H 
#define CODEGENERATORS_CPPCPPMYCNVWRITER_H 1

// Include files

// Local includes
#include "cppcppbasecnvwriter.h"

class CppCppMyCnvWriter : public CppCppBaseCnvWriter {

 public:
  /**
   * Constructor
   */
  CppCppMyCnvWriter(UMLDoc *parent, const char *name);

  /**
   * Destructor
   */
  ~CppCppMyCnvWriter();

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
  void writeBasicMult1FillRep(Assoc* as);
    
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
   * Gets the MySQL type corresponding to a given C++ type.
   * The MySQL type is the one used in MySQL++API to set/get
   * members into/from statements
   */
  QString getMyType(QString& type);
    
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
   * Says whether the current object is a request that
   * needs storage in the newRequests table
   */
  bool isNewRequest();

  /**
   * Says whether the current object is a subrequest that
   * needs storage in the newSubRequests table
   */
  bool isNewSubRequest();

  /**
   * creates file mysql.sql and write beginning of it
   */
  void startSQLFile();

  /**
   * write end of file mysql.sql
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

#endif // CODEGENERATORS_CPPCPPMYCNVWRITER_H

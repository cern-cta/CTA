// Include files
#include <iostream>
#include <qmap.h>
#include <qfile.h>
#include <qregexp.h>
#include <qptrlist.h>
#include <qtextstream.h>

// local
#include "cppcpporacnvwriter.h"

//-----------------------------------------------------------------------------
// Implementation file for class : CppCppOraCnvWriter
//
// 2004-01-13 : Sebastien Ponce
//-----------------------------------------------------------------------------

//=============================================================================
// Standard constructor, initializes variables
//=============================================================================
CppCppOraCnvWriter::CppCppOraCnvWriter(UMLDoc *parent, const char *name) :
  CppCppBaseCnvWriter(parent, name), m_firstClass(true) {
  setPrefix("Ora");
}

//=============================================================================
// startSQLFile
//=============================================================================
void CppCppOraCnvWriter::startSQLFile() {
  // Preparing SQL file for creation/deletion of the database
  QFile file;
  openFile(file,
           "castor/db/oracle.sql",
           IO_WriteOnly | IO_Truncate);
  QTextStream stream(&file);
  stream << "/* SQL statements for object types */" << endl
         << "DROP INDEX I_RH_ID2TYPE_FULL;" << endl
         << "DROP TABLE RH_ID2TYPE;" << endl
         << "CREATE TABLE RH_ID2TYPE (id INTEGER PRIMARY KEY, type NUMBER);" << endl
         << "CREATE INDEX I_RH_ID2TYPE_FULL on RH_ID2TYPE (id, type);" << endl
         << endl
         << "/* SQL statements for indices */" << endl
         << "DROP INDEX I_RH_INDICES_FULL;" << endl
         << "DROP TABLE RH_INDICES;" << endl
         << "CREATE TABLE RH_INDICES (name CHAR(8), value NUMBER);" << endl
         << "CREATE INDEX I_RH_INDICES_FULL on RH_INDICES (name, value);" << endl
         << "INSERT INTO RH_INDICES (name, value) VALUES ('next_id', 1);" << endl
         << endl
         << "/* SQL statements for requests status */" << endl
         << "DROP INDEX I_RH_REQUESTSSTATUS_FULL;" << endl
         << "DROP TABLE RH_REQUESTSSTATUS;" << endl
         << "CREATE TABLE RH_REQUESTSSTATUS (id INTEGER PRIMARY KEY, status CHAR(8), creation DATE, lastChange DATE);" << endl
         << "CREATE INDEX I_RH_REQUESTSSTATUS_FULL on RH_REQUESTSSTATUS (id, status);" << endl
         << endl
         << "/* PL/SQL procedure for getting the next request to handle */" << endl
         << "CREATE OR REPLACE PROCEDURE getNRStatement(reqid OUT INTEGER) AS" << endl
         << "BEGIN" << endl
         << "  SELECT ID INTO reqid FROM rh_requestsStatus WHERE status = 'NEW' AND rownum <=1;" << endl
         << "  UPDATE rh_requestsStatus SET status = 'RUNNING', lastChange = SYSDATE WHERE ID = reqid;" << endl
         << "END;" << endl;
  file.close();
}

//=============================================================================
// Initialization
//=============================================================================
bool CppCppOraCnvWriter::init(UMLClassifier* c, QString fileName) {
  // call upper class init
  this->CppBaseWriter::init(c, fileName);
  // fixes the namespace
  m_originalPackage = m_classInfo->fullPackageName;
  m_classInfo->packageName = "castor::db::ora";
  m_classInfo->fullPackageName = "castor::db::ora::";
  // includes converter  header file and object header file
  m_includes.insert(QString("\"Ora") + m_classInfo->className + "Cnv.hpp\"");
  m_includes.insert(QString("\"") +
                    findFileName(m_class,".h") + ".hpp\"");
  // calls the postinit of this class
  postinit(c, fileName);
  return true;
}

//=============================================================================
// writeClass
//=============================================================================
void CppCppOraCnvWriter::writeClass(UMLClassifier */*c*/) {
  if (m_firstClass) {
    startSQLFile();
    m_firstClass = false;
  }
  // static factory declaration
  writeFactory();
  // static constants initialization
  writeConstants();
  // creation/deletion of the database
  writeSqlStatements();
  // constructor and destructor
  writeConstructors();
  // reset method
  writeReset();
  // objtype methods
  writeObjType();
  // FillRep methods
  writeFillRep();
  // FillObj methods
  writeFillObj();
  // createRep method
  writeCreateRep();
  // updateRep method
  writeUpdateRep();
  // deleteRep method
  writeDeleteRep();
  // createObj method
  writeCreateObj();
  // updateObj method
  writeUpdateObj();
}

//=============================================================================
// writeConstants
//=============================================================================
void CppCppOraCnvWriter::writeConstants() {
  writeWideHeaderComment("Static constants initialization",
                         getIndent(),
                         *m_stream);
  *m_stream << getIndent()
            << "/// SQL statement for request insertion"
            << endl << getIndent()
            << "const std::string "
            << m_classInfo->fullPackageName
            << "Ora" << m_classInfo->className
            << "Cnv::s_insertStatementString =" << endl
            << getIndent()
            << "\"INSERT INTO rh_" << m_classInfo->className
            << " (";
  int n = 0;
  // create a list of members
  MemberList members = createMembersList();
  // Go through the members
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    if (n > 0) *m_stream << ", ";
    *m_stream << mem->first;
    n++;
  }
  // create a list of associations
  AssocList assocs = createAssocsList();
  // Go through the associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE) {
      // One to One associations
      if (n > 0) *m_stream << ", ";
      *m_stream << as->second.first;
      n++;
    }
  }
  *m_stream << ") VALUES (";
  for (int i = 0; i < n; i++) {
    *m_stream << ":" << (i+1);
    if (i < n - 1) *m_stream << ",";
  }
  *m_stream << ")\";" << endl << endl << getIndent()
            << "/// SQL statement for request deletion"
            << endl << getIndent()
            << "const std::string "
            << m_classInfo->fullPackageName
            << "Ora" << m_classInfo->className
            << "Cnv::s_deleteStatementString =" << endl
            << getIndent()
            << "\"DELETE FROM rh_" << m_classInfo->className
            << " WHERE id = :1\";" << endl << endl
            << getIndent()
            << "/// SQL statement for request selection"
            << endl << getIndent()
            << "const std::string "
            << m_classInfo->fullPackageName
            << "Ora" << m_classInfo->className
            << "Cnv::s_selectStatementString =" << endl
            << getIndent()
            << "\"SELECT ";
  // Go through the members
  n = 0;
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    if (n > 0) *m_stream << ", ";
    *m_stream << mem->first;
    n++;
  }
  // Go through the associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE) {
      if (n > 0) *m_stream << ", ";
      *m_stream << as->second.first;
      n++;
    }
  }
  *m_stream << " FROM rh_" << m_classInfo->className
            << " WHERE id = :1\";" << endl << endl
            << getIndent()
            << "/// SQL statement for request update"
            << endl << getIndent()
            << "const std::string "
            << m_classInfo->fullPackageName
            << "Ora" << m_classInfo->className
            << "Cnv::s_updateStatementString =" << endl
            << getIndent()
            << "\"UPDATE rh_" << m_classInfo->className
            << " SET ";
  n = 0;
  // Go through the members
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    if (mem->first == "id") continue;
    if (n > 0) *m_stream << ", ";
    n++;
    *m_stream << mem->first << " = :" << n;
  }
  // Go through the associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE) {
      // One to One associations
      if (n > 0) *m_stream << ", ";
      n++;
      *m_stream << as->second.first << " = :" << n;
    }
  }
  *m_stream << " WHERE id = :" << n+1
            << "\";" << endl << endl << getIndent()
            << "/// SQL statement for type storage"
            << endl << getIndent()
            << "const std::string "
            << m_classInfo->fullPackageName
            << "Ora" << m_classInfo->className
            << "Cnv::s_storeTypeStatementString =" << endl
            << getIndent()
            << "\"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)\";"
            << endl << endl << getIndent()
            << "/// SQL statement for type deletion"
            << endl << getIndent()
            << "const std::string "
            << m_classInfo->fullPackageName
            << "Ora" << m_classInfo->className
            << "Cnv::s_deleteTypeStatementString =" << endl
            << getIndent()
            << "\"DELETE FROM rh_Id2Type WHERE id = :1\";"
            << endl << endl;
  // Status dedicated statements
  if (isRequest()) {
    *m_stream << getIndent()
              << "/// SQL statement for request status insertion"
              << endl << getIndent()
              << "const std::string "
              << m_classInfo->fullPackageName
              << "Ora" << m_classInfo->className
              << "Cnv::s_insertStatusStatementString =" << endl
              << getIndent()
              << "\"INSERT INTO rh_requestsStatus (id, status, creation, lastChange)"
              << " VALUES (:1, 'NEW', SYSDATE, SYSDATE)\";"
              << endl << endl << getIndent()
              << "/// SQL statement for request status deletion"
              << endl << getIndent()
              << "const std::string "
              << m_classInfo->fullPackageName
              << "Ora" << m_classInfo->className
              << "Cnv::s_deleteStatusStatementString =" << endl
              << getIndent()
              << "\"DELETE FROM rh_requestsStatus WHERE id = :1\";"
              << endl << endl;
  }
  // Associations dedicated statements
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_N ||
        as->first.second == COMPOS_PARENT ||
        as->first.second == AGGREG_PARENT) {
      *m_stream << getIndent()
                << "/// SQL insert statement for member "
                << as->second.first
                << endl << getIndent()
                << "const std::string "
                << m_classInfo->fullPackageName
                << "Ora" << m_classInfo->className
                << "Cnv::s_insert"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << "StatementString =" << endl << getIndent()
                << "\"INSERT INTO rh_"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << " (Parent, Child) VALUES (:1, :2)\";"
                << endl << endl << getIndent()
                << "/// SQL delete statement for member "
                << as->second.first
                << endl << getIndent()
                << "const std::string "
                << m_classInfo->fullPackageName
                << "Ora" << m_classInfo->className
                << "Cnv::s_delete"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << "StatementString =" << endl << getIndent()
                << "\"DELETE FROM rh_"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << " WHERE Parent = :1 AND Child = :2\";"
                << endl << endl << getIndent()
                << "/// SQL select statement for member "
                << as->second.first
                << endl << getIndent()
                << "const std::string "
                << m_classInfo->fullPackageName
                << "Ora" << m_classInfo->className
                << "Cnv::s_"
                << capitalizeFirstLetter(as->second.third)
                << "2" << capitalizeFirstLetter(as->second.second)
                << "StatementString =" << endl << getIndent()
                << "\"SELECT Child from rh_"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << " WHERE Parent = :1\";" << endl << endl;
    } else if (as->first.second == COMPOS_CHILD ||
               as->first.second == AGGREG_CHILD) {
      *m_stream << getIndent()
                << "/// SQL insert statement for member "
                << as->second.first
                << endl << getIndent()
                << "const std::string "
                << m_classInfo->fullPackageName
                << "Ora" << m_classInfo->className
                << "Cnv::s_insert"
                << capitalizeFirstLetter(as->second.second)
                << "2"
                << capitalizeFirstLetter(as->second.third)
                << "StatementString =" << endl << getIndent()
                << "\"INSERT INTO rh_"
                << capitalizeFirstLetter(as->second.second)
                << "2"
                << capitalizeFirstLetter(as->second.third)
                << " (Parent, Child) VALUES (:1, :2)\";"
                << endl << endl << getIndent()
                << "/// SQL delete statement for member "
                << as->second.first
                << endl << getIndent()
                << "const std::string "
                << m_classInfo->fullPackageName
                << "Ora" << m_classInfo->className
                << "Cnv::s_delete" << capitalizeFirstLetter(as->second.second)
                << "2"
                << capitalizeFirstLetter(as->second.third)
                << "StatementString =" << endl << getIndent()
                << "\"DELETE FROM rh_"
                << capitalizeFirstLetter(as->second.second)
                << "2"
                << capitalizeFirstLetter(as->second.third)
                << " WHERE Parent = :1 AND Child = :2\";"
                << endl << endl;
    }
  }
}

//=============================================================================
// writeSqlStatements
//=============================================================================
void CppCppOraCnvWriter::writeSqlStatements() {
  QFile file;
  openFile(file,
           "castor/db/oracle.sql",
           IO_WriteOnly | IO_Append);
  QTextStream stream(&file);
  stream << "/* SQL statements for type "
         << m_classInfo->className
         << " */"
         << endl
         << "DROP TABLE rh_"
         << m_classInfo->className
         << ";" << endl
         << "CREATE TABLE rh_"
         << m_classInfo->className
         << " (";
  int n = 0;
  // create a list of members
  MemberList members = createMembersList();
  // Go through the members
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    if (n > 0) stream << ", ";
    stream << mem->first << " "
           << getSQLType(mem->second);
    if (mem->first == "id") stream << " PRIMARY KEY";
    n++;
  }
  // create a list of associations
  AssocList assocs = createAssocsList();
  // Go through the associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE) {
      // One to One associations
      if (n > 0) stream << ", ";
      stream << as->second.first << " INTEGER";
      n++;
    }
  }
  stream << ");" << endl;
  // One to n associations dedicated statements
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.second == COMPOS_CHILD ||
        as->first.second == AGGREG_CHILD) {
      stream << "DROP TABLE rh_"
             << capitalizeFirstLetter(as->second.second)
             << "2"
             << capitalizeFirstLetter(as->second.third)
             << ";" << endl
             << "CREATE TABLE rh_"
             << capitalizeFirstLetter(as->second.second)
             << "2"
             << capitalizeFirstLetter(as->second.third)
             << " (Parent INTEGER, Child INTEGER);"
             << endl;
    }
  }
  stream << endl;
  file.close();
}

//=============================================================================
// writeConstructors
//=============================================================================
void CppCppOraCnvWriter::writeConstructors() {
  // constructor
  writeWideHeaderComment("Constructor", getIndent(), *m_stream);
  *m_stream << getIndent() << m_classInfo->fullPackageName
            << "Ora" << m_classInfo->className << "Cnv::Ora"
            << m_classInfo->className << "Cnv() :" << endl
            << getIndent() << "  OraBaseCnv()," << endl
            << getIndent() << "  m_insertStatement(0)," << endl
            << getIndent() << "  m_deleteStatement(0)," << endl
            << getIndent() << "  m_selectStatement(0)," << endl
            << getIndent() << "  m_updateStatement(0)," << endl;
  if (isRequest()) {
    *m_stream << getIndent() << "  m_insertStatusStatement(0)," << endl
              << getIndent() << "  m_deleteStatusStatement(0)," << endl;
  }
  *m_stream << getIndent() << "  m_storeTypeStatement(0)," << endl
            << getIndent() << "  m_deleteTypeStatement(0)";
  // Associations dedicated statements
  AssocList assocs = createAssocsList();
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_N  ||
        as->first.second == COMPOS_PARENT ||
        as->first.second == AGGREG_PARENT) {
      *m_stream << "," << endl << getIndent()
                << "  m_insert"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << "Statement(0)," << endl << getIndent()
                << "  m_delete"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << "Statement(0)," << endl << getIndent()
                << "  m_"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << "Statement(0)";
    } else if (as->first.second == COMPOS_CHILD ||
               as->first.second == AGGREG_CHILD) {
      *m_stream << "," << endl << getIndent()
                << "  m_insert"
                << capitalizeFirstLetter(as->second.second)
                << "2"
                << capitalizeFirstLetter(as->second.third)
                << "Statement(0)," << endl << getIndent()
                << "  m_delete"
                << capitalizeFirstLetter(as->second.second)
                << "2"
                << capitalizeFirstLetter(as->second.third)
                << "Statement(0)";
    }
  }
  *m_stream << " {}" << endl << endl;
  // Destructor
  writeWideHeaderComment("Destructor", getIndent(), *m_stream);
  *m_stream << getIndent() << m_classInfo->fullPackageName
            << "Ora" << m_classInfo->className << "Cnv::~Ora"
            << m_classInfo->className << "Cnv() throw() {" << endl;
  m_indent++;
  *m_stream << getIndent() << "reset();" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << endl;
}

//=============================================================================
// writeReset
//=============================================================================
void CppCppOraCnvWriter::writeReset() {
  // Header
  writeWideHeaderComment("reset", getIndent(), *m_stream);
  *m_stream << "void " << m_classInfo->packageName << "::"
            << m_prefix << m_classInfo->className
            << "Cnv::reset() throw() {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "//Here we attempt to delete the statements correctly"
            << endl << getIndent()
            << "// If something goes wrong, we just ignore it"
            << endl << getIndent() << "try {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "deleteStatement(m_insertStatement);"
            << endl << getIndent()
            << "deleteStatement(m_deleteStatement);"
            << endl << getIndent()
            << "deleteStatement(m_selectStatement);"
            << endl << getIndent()
            << "deleteStatement(m_updateStatement);"
            << endl;
  if (isRequest()) {
    *m_stream << getIndent()
              << "deleteStatement(m_insertStatusStatement);"
              << endl << getIndent()
              << "deleteStatement(m_deleteStatusStatement);"
              << endl;
  }
  *m_stream << getIndent() << "deleteStatement(m_storeTypeStatement);"
            << endl << getIndent()
            << "deleteStatement(m_deleteTypeStatement);"
            << endl;
  // Associations dedicated statements
  AssocList assocs = createAssocsList();
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_N ||
        as->first.second == COMPOS_PARENT ||
        as->first.second == AGGREG_PARENT) {
      *m_stream << getIndent()
                << "deleteStatement(m_insert"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << "Statement);" << endl << getIndent()
                << "deleteStatement(m_"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << "Statement);" << endl;
    } else if (as->first.second == COMPOS_CHILD ||
               as->first.second == AGGREG_CHILD) {
      *m_stream << getIndent()
                << "deleteStatement(m_insert"
                << capitalizeFirstLetter(as->second.second)
                << "2"
                << capitalizeFirstLetter(as->second.third)
                << "Statement);" << endl << getIndent()
                << "deleteStatement(m_delete"
                << capitalizeFirstLetter(as->second.second)
                << "2"
                << capitalizeFirstLetter(as->second.third)
                << "Statement);" << endl;
    }
  }
  m_indent--;
  *m_stream << getIndent()
            << "} catch (oracle::occi::SQLException e) {};"
            << endl << getIndent()
            << "// Now reset all pointers to 0"
            << endl << getIndent()
            << "m_insertStatement = 0;"
            << endl << getIndent()
            << "m_deleteStatement = 0;"
            << endl << getIndent()
            << "m_selectStatement = 0;"
            << endl << getIndent()
            << "m_updateStatement = 0;" << endl;
  if (isRequest()) {
    *m_stream << getIndent()
              << "m_insertStatusStatement = 0;"
              << endl << getIndent()
              << "m_deleteStatusStatement = 0;"
              << endl;
  }
  *m_stream << getIndent()
            << "m_storeTypeStatement = 0;"
            << endl << getIndent()
            << "m_deleteTypeStatement = 0;" << endl;
  // Associations dedicated statements
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_N ||
        as->first.second == COMPOS_PARENT ||
        as->first.second == AGGREG_PARENT) {
      *m_stream << getIndent()
                << "m_insert"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << "Statement = 0;" << endl << getIndent()
                << "m_delete"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << "Statement = 0;" << endl << getIndent()
                << "m_"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << "Statement = 0;" << endl;
    } else if (as->first.second == COMPOS_CHILD ||
               as->first.second == AGGREG_CHILD) {
      *m_stream << getIndent()
                << "m_insert" << capitalizeFirstLetter(as->second.second)
                << "2"
                << capitalizeFirstLetter(as->second.third)
                << "Statement = 0;" << endl << getIndent()
                << "m_delete"
                << capitalizeFirstLetter(as->second.second)
                << "2"
                << capitalizeFirstLetter(as->second.third)
                << "Statement = 0;" << endl;
    }
  }
  // End of the method
  m_indent--;
  *m_stream << "}" << endl << endl;
}

//=============================================================================
// writeFillRep
//=============================================================================
void CppCppOraCnvWriter::writeFillRep() {
  // First write the main function, dispatching the requests
  writeWideHeaderComment("fillRep", getIndent(), *m_stream);
  QString func = QString("void ") +
    m_classInfo->packageName + "::Ora" +
    m_classInfo->className + "Cnv::fillRep(";
  *m_stream << getIndent() << func
            << fixTypeName("IAddress*",
                           "castor",
                           "")
            << " address," << endl << getIndent();
  func.replace(QRegExp("."), " ");
  *m_stream << func  << fixTypeName("IObject*",
                                    "castor",
                                    "")
            << " object," << endl << getIndent()
            << func << "unsigned int type,"
            << endl << getIndent()
            << func << "bool autocommit)"
            << endl << getIndent() << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           "")
            << ") {"
            << endl;
  m_indent++;
  // Get the precise object
  *m_stream << getIndent() << m_originalPackage
            << m_classInfo->className << "* obj = " << endl
            << getIndent() << "  dynamic_cast<"
            << m_originalPackage
            << m_classInfo->className << "*>(object);"
            << endl;
  // Call the dedicated method
  *m_stream << getIndent() << "switch (type) {" << endl;
  AssocList assocs = createAssocsList();
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (!isEnum(as->second.second)) {
      if (as->first.first == MULT_ONE ||
          as->first.first == MULT_N) {
        addInclude("\"castor/Constants.hpp\"");
        *m_stream << getIndent() << "case castor::OBJ_"
                  << capitalizeFirstLetter(as->second.second)
                  << " :" << endl;
        m_indent++;
        *m_stream << getIndent() << "fillRep"
                  << capitalizeFirstLetter(as->second.second)
                  << "(obj);" << endl << getIndent()
                  << "break;" << endl;
        m_indent--;
      }
    }
  }
  *m_stream << getIndent() << "default :" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "castor::exception::InvalidArgument ex;"
            << endl << getIndent()
            << "ex.getMessage() << \"fillRep called on type \" << type "
            << endl << getIndent()
            << "                << \" on object of type \" << obj->type() "
            << endl << getIndent()
            << "                << \". This is meaningless.\";"
            << endl << getIndent()
            << "throw ex;" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << getIndent()
            << "if (autocommit) {" << endl;
  m_indent++;
  *m_stream << getIndent() << "cnvSvc()->getConnection()->commit();"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << endl;

  // Now write the dedicated fillRep Methods
  MemberList members = createMembersList();
  unsigned int n = members.count() - 1; // All but Id
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (!isEnum(as->second.second)) {
      if (as->first.first == MULT_ONE) {
        writeBasicMult1FillRep(as, n);
        n++;
      } else if  (as->first.first == MULT_N) {
        writeBasicMultNFillRep(as);
      }
    }
  }
}

//=============================================================================
// writeFillObj
//=============================================================================
void CppCppOraCnvWriter::writeFillObj() {
  // First write the main function, dispatching the requests
  writeWideHeaderComment("fillObj", getIndent(), *m_stream);
  QString func = QString("void ") +
    m_classInfo->packageName + "::Ora" +
    m_classInfo->className + "Cnv::fillObj(";
  *m_stream << getIndent() << func
            << fixTypeName("IAddress*",
                           "castor",
                           "")
            << " address," << endl << getIndent();
  func.replace(QRegExp("."), " ");
  *m_stream << func << fixTypeName("IObject*",
                                   "castor",
                                   "")
            << " object," << endl << getIndent()
            << func << "unsigned int type)"
            << endl << getIndent() << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           "")
            << ") {"
            << endl;
  m_indent++;
  // Get the precise object
  *m_stream << getIndent() << m_originalPackage
            << m_classInfo->className << "* obj = " << endl
            << getIndent() << "  dynamic_cast<"
            << m_originalPackage
            << m_classInfo->className << "*>(object);"
            << endl;
  // Call the dedicated method
  *m_stream << getIndent() << "switch (type) {" << endl;
  AssocList assocs = createAssocsList();
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (!isEnum(as->second.second)) {
      if (as->first.first == MULT_ONE ||
          as->first.first == MULT_N) {
        addInclude("\"castor/Constants.hpp\"");
        *m_stream << getIndent() << "case castor::OBJ_"
                  << capitalizeFirstLetter(as->second.second)
                  << " :" << endl;
        m_indent++;
        *m_stream << getIndent() << "fillObj"
                  << capitalizeFirstLetter(as->second.second)
                  << "(obj);" << endl << getIndent()
                  << "break;" << endl;
        m_indent--;
      }
    }
  }
  *m_stream << getIndent() << "default :" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "castor::exception::InvalidArgument ex;"
            << endl << getIndent()
            << "ex.getMessage() << \"fillObj called on type \" << type "
            << endl << getIndent()
            << "                << \" on object of type \" << obj->type() "
            << endl << getIndent()
            << "                << \". This is meaningless.\";"
            << endl << getIndent()
            << "throw ex;" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << endl;

  // Now write the dedicated fillRep Methods
  MemberList members = createMembersList();
  unsigned int n = members.count() - 1; // All but Id
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (!isEnum(as->second.second)) {
      if (as->first.first == MULT_ONE) {
        writeBasicMult1FillObj(as, n);
        n++;
      } else if  (as->first.first == MULT_N) {
        writeBasicMultNFillObj(as);
      }
    }
  }
}

//=============================================================================
// writeBasicMult1FillRep
//=============================================================================
void CppCppOraCnvWriter::writeBasicMult1FillRep(Assoc* as,
                                                unsigned int n) {
  writeWideHeaderComment("fillRep" +
                         capitalizeFirstLetter(as->second.second),
                         getIndent(), *m_stream);
  *m_stream << getIndent() << "void "
            << m_classInfo->packageName << "::Ora"
            << m_classInfo->className << "Cnv::fillRep"
            << capitalizeFirstLetter(as->second.second)
            << "("
            << fixTypeName(m_classInfo->className + "*",
                           getNamespace(m_classInfo->className),
                           "")
            << " obj)" << endl << getIndent()
            << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           "")
            << ") {"
            << endl;
  m_indent++;

  *m_stream << getIndent() << "// Check select statement"
            << endl << getIndent()
            << "if (0 == m_selectStatement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_selectStatement = createStatement(s_selectStatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << getIndent()
            << "// retrieve the object from the database"
            << endl << getIndent()
            << "m_selectStatement->setDouble(1, obj->id());"
            << endl << getIndent()
            << "oracle::occi::ResultSet *rset = m_selectStatement->executeQuery();"
            << endl << getIndent()
            << "if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << fixTypeName("NoEntry",
                           "castor.exception",
                           m_classInfo->packageName)
            << " ex;" << endl << getIndent()
            << "ex.getMessage() << \"No object found for id :\""
            << " << obj->id();" << endl
            << getIndent() << "throw ex;" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  writeSingleGetFromSelect(as->second, n, true);
  // Close request
  *m_stream << getIndent() << "// Close resultset" << endl
            << getIndent()
            << "m_selectStatement->closeResultSet(rset);"
            << endl;
  *m_stream << getIndent()
            << fixTypeName("DbAddress",
                           "castor::db",
                           m_classInfo->packageName)
            << " ad(" << as->second.first
            << "Id, \" \", 0);" << endl << getIndent()
            << "// Check whether old object should be deleted"
            << endl << getIndent()
            << "if (0 != " << as->second.first
            << "Id &&" << endl
            << getIndent() << "    0 != obj->"
            << as->second.first << "() &&" << endl
            << getIndent() << "    obj->"
            << as->second.first << "()->id() != "
            << as->second.first << "Id) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "cnvSvc()->deleteRepByAddress(&ad, false);"
            << endl << getIndent()
            << as->second.first << "Id = 0;" << endl;
  bool case1 = as->first.first == MULT_N ||
    as->first.second == COMPOS_PARENT ||
    as->first.second == AGGREG_PARENT;
  bool case2 = as->first.second == COMPOS_CHILD ||
    as->first.second == AGGREG_CHILD;
  QString s1, s2, t1, t2;
  if (case2) {
    s1 = as->second.second;
    s2 = as->second.third;
    t2 = "obj->id()";
    t1 = QString("obj->") + as->second.first + "()->id()";
  } else {
    s2 = as->second.second;
    s1 = as->second.third;      
    t1 = "obj->id()";
    t2 = QString("obj->") + as->second.first + "()->id()";
  }
  if (case1 || case2) {
    *m_stream << getIndent()
              << "if (0 == m_delete"
              << capitalizeFirstLetter(s1)
              << "2"
              << capitalizeFirstLetter(s2)
              << "Statement) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "m_delete"
              << capitalizeFirstLetter(s1)
              << "2"
              << capitalizeFirstLetter(s2)
              << "Statement = createStatement(s_delete"
              << capitalizeFirstLetter(s1)
              << "2"
              << capitalizeFirstLetter(s2)
              << "StatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl << getIndent()
              << "m_delete"
              << capitalizeFirstLetter(s1)
              << "2"
              << capitalizeFirstLetter(s2)
              << "Statement->setDouble(1, " << t1 << ");"
              << endl << getIndent()
              << "m_delete"
              << capitalizeFirstLetter(s1)
              << "2"
              << capitalizeFirstLetter(s2)
              << "Statement->setDouble(2, " << t2 << ");"
              << endl << getIndent()
              << "m_delete"
              << capitalizeFirstLetter(s1)
              << "2"
              << capitalizeFirstLetter(s2)
              << "Statement->executeUpdate();"
              << endl;
  }
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  *m_stream << getIndent() << "// Update object or create new one"
            << endl << getIndent()
            << "if (" << as->second.first
            << "Id == 0) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "if (0 != obj->"
            << as->second.first
            << "()) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "cnvSvc()->createRep(&ad, obj->"
            << as->second.first
            << "(), false);" << endl;
  if (case1 || case2) {
    *m_stream << getIndent()
              << "if (0 == m_insert"
              << capitalizeFirstLetter(s1)
              << "2"
              << capitalizeFirstLetter(s2)
              << "Statement) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "m_insert"
              << capitalizeFirstLetter(s1)
              << "2"
              << capitalizeFirstLetter(s2)
              << "Statement = createStatement(s_insert"
              << capitalizeFirstLetter(s1)
              << "2"
              << capitalizeFirstLetter(s2)
              << "StatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl
              << getIndent()
              << "m_insert"
              << capitalizeFirstLetter(s1)
              << "2"
              << capitalizeFirstLetter(s2)
              << "Statement->setDouble(1, " << t1 << ");"
              << endl << getIndent()
              << "m_insert"
              << capitalizeFirstLetter(s1)
              << "2"
              << capitalizeFirstLetter(s2)
              << "Statement->setDouble(2, " << t2 << ");"
              << endl << getIndent()
              << "m_insert"
              << capitalizeFirstLetter(s1)
              << "2"
              << capitalizeFirstLetter(s2)
              << "Statement->executeUpdate();"
              << endl;
  }
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  *m_stream << getIndent() << "} else {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "cnvSvc()->updateRep(&ad, obj->"
            << as->second.first
            << "(), false);" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;

  m_indent--;
  *m_stream << getIndent() << "}" << endl << endl;
}

//=============================================================================
// writeBasicMult1FillObj
//=============================================================================
void CppCppOraCnvWriter::writeBasicMult1FillObj(Assoc* as,
                                                unsigned int n) {
  writeWideHeaderComment("fillObj" +
                         capitalizeFirstLetter(as->second.second),
                         getIndent(), *m_stream);
  *m_stream << getIndent() << "void "
            << m_classInfo->packageName << "::Ora"
            << m_classInfo->className << "Cnv::fillObj"
            << capitalizeFirstLetter(as->second.second)
            << "("
            << fixTypeName(m_classInfo->className + "*",
                           getNamespace(m_classInfo->className),
                           "")
            << " obj)"
            << endl << getIndent() << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           m_classInfo->packageName)
            << ") {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << "// Check whether the statement is ok"
            << endl << getIndent()
            << "if (0 == m_selectStatement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_selectStatement = createStatement(s_selectStatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent()
            << "// retrieve the object from the database"
            << endl << getIndent()
            << "m_selectStatement->setDouble(1, obj->id());"
            << endl << getIndent()
            << "oracle::occi::ResultSet *rset = m_selectStatement->executeQuery();"
            << endl << getIndent()
            << "if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << fixTypeName("NoEntry",
                           "castor.exception",
                           m_classInfo->packageName)
            << " ex;" << endl << getIndent()
            << "ex.getMessage() << \"No object found for id :\""
            << " << obj->id();" << endl
            << getIndent() << "throw ex;" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  writeSingleGetFromSelect(as->second, n, true);
  *m_stream << getIndent() << "// Close ResultSet"
            << endl << getIndent()
            << "m_selectStatement->closeResultSet(rset);"
            << endl;
  *m_stream << getIndent()
            << "// Check whether something should be deleted"
            << endl << getIndent() <<"if (0 != obj->"
            << as->second.first << "() &&" << endl
            << getIndent()
            << "    (0 == " << as->second.first
            << "Id ||" << endl
            << getIndent() << "     obj->"
            << as->second.first
            << "()->id() != " << as->second.first
            << "Id)) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "delete obj->"
            << as->second.first << "();" << endl
            << getIndent()
            << "obj->set"
            << capitalizeFirstLetter(as->second.first)
            << "(0);" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  *m_stream << getIndent()
            << "// Update object or create new one"
            << endl << getIndent()
            << "if (0 != " << as->second.first
            << "Id) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "if (0 == obj->"
            << as->second.first << "()) {" << endl;
  m_indent++;
  *m_stream << getIndent() << "obj->set"
            << capitalizeFirstLetter(as->second.first)
            << endl << getIndent()
            << "  (dynamic_cast<"
            << fixTypeName(as->second.second,
                           getNamespace(as->second.second),
                           m_classInfo->packageName)
            << "*>" << endl << getIndent()
            << "   (cnvSvc()->getObjFromId(" << as->second.first
            << "Id)));"
            << endl;
  m_indent--;
  *m_stream << getIndent()
            << "} else if (obj->"
            << as->second.first
            << "()->id() == " << as->second.first
            << "Id) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "cnvSvc()->updateObj(obj->"
            << as->second.first
            << "());"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << endl;
}

//=============================================================================
// writeBasicMultNFillRep
//=============================================================================
void CppCppOraCnvWriter::writeBasicMultNFillRep(Assoc* as) {
  writeWideHeaderComment("fillRep" +
                         capitalizeFirstLetter(as->second.second),
                         getIndent(), *m_stream);
  *m_stream << getIndent()
            << "void " << m_classInfo->packageName
            << "::Ora" << m_classInfo->className
            << "Cnv::fillRep"
            << capitalizeFirstLetter(as->second.second)
            << "("
            << fixTypeName(m_classInfo->className + "*",
                           getNamespace(m_classInfo->className),
                           "")
            << " obj)"
            << endl << getIndent() << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           "")
            << ") {"
            << endl;
  m_indent++;
  *m_stream << getIndent() << "// check select statement"
            << endl << getIndent()
            << "if (0 == m_"
            << capitalizeFirstLetter(as->second.third)
            << "2"
            << capitalizeFirstLetter(as->second.second)
            << "Statement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_"
            << capitalizeFirstLetter(as->second.third)
            << "2"
            << capitalizeFirstLetter(as->second.second)
            << "Statement = createStatement(s_"
            << capitalizeFirstLetter(as->second.third)
            << "2"
            << capitalizeFirstLetter(as->second.second)
            << "StatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent() << "// Get current database data"
            << endl << getIndent()
            << fixTypeName("set", "", "")
            << "<int> " << as->second.first
            << "List;" << endl << getIndent()
            << "m_"
            << capitalizeFirstLetter(as->second.third)
            << "2"
            << capitalizeFirstLetter(as->second.second)
            << "Statement->setDouble(1, obj->id());"
            << endl << getIndent()
            << "oracle::occi::ResultSet *rset = "
            << "m_"
            << capitalizeFirstLetter(as->second.third)
            << "2"
            << capitalizeFirstLetter(as->second.second)
            << "Statement->executeQuery();"
            << endl << getIndent()
            << "while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << as->second.first
            << "List.insert(rset->getInt(1));"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent()
            << "m_"
            << capitalizeFirstLetter(as->second.third)
            << "2"
            << capitalizeFirstLetter(as->second.second)
            << "Statement->closeResultSet(rset);"
            << endl << getIndent()
            << "// update segments and create new ones"
            << endl << getIndent() << "for ("
            << fixTypeName("vector", "", "")
            << "<"
            << fixTypeName(as->second.second,
                           getNamespace(as->second.second),
                           m_classInfo->packageName)
            << "*>::iterator it = obj->"
            << as->second.first
            << "().begin();" << endl << getIndent()
            << "     it != obj->"
            << as->second.first
            << "().end();" << endl << getIndent()
            << "     it++) {"  << endl;
  m_indent++;
  *m_stream << getIndent()
            << fixTypeName("set", "", "")
            << "<int>::iterator item;" << endl
            << getIndent() << "if ((item = "
            << as->second.first
            << "List.find((*it)->id())) == "
            << as->second.first
            << "List.end()) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "cnvSvc()->createRep(0, *it, false);"
            << endl;
  if (as->first.second == COMPOS_PARENT ||
      as->first.second == AGGREG_PARENT) {
    *m_stream << getIndent()
              << "if (0 == m_insert"
              << capitalizeFirstLetter(as->second.third)
              << "2"
              << capitalizeFirstLetter(as->second.second)
              << "Statement) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "m_insert"
              << capitalizeFirstLetter(as->second.third)
              << "2"
              << capitalizeFirstLetter(as->second.second)
              << "Statement = createStatement(s_insert"
              << capitalizeFirstLetter(as->second.third)
              << "2"
              << capitalizeFirstLetter(as->second.second)
              << "StatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl
              << getIndent()
              << "m_insert"
              << capitalizeFirstLetter(as->second.third)
              << "2"
              << capitalizeFirstLetter(as->second.second)
              << "Statement->setDouble(1, obj->id());"
              << endl << getIndent()
              << "m_insert"
              << capitalizeFirstLetter(as->second.third)
              << "2"
              << capitalizeFirstLetter(as->second.second)
              << "Statement->setDouble(2, (*it)->id());"
              << endl << getIndent()
              << "m_insert"
              << capitalizeFirstLetter(as->second.third)
              << "2"
              << capitalizeFirstLetter(as->second.second)
              << "Statement->executeUpdate();"
              << endl;
  }
  m_indent--;
  *m_stream << getIndent() << "} else {" << endl;
  m_indent++;
  *m_stream << getIndent() << as->second.first
            << "List.erase(item);"
            << endl;
  *m_stream << getIndent()
            << "cnvSvc()->updateRep(0, *it, false);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent() << "// Delete old data"
            << endl << getIndent() << "for ("
            << fixTypeName("set", "", "")
            << "<int>::iterator it = "
            << as->second.first << "List.begin();"
            << endl << getIndent()
            << "     it != "
            << as->second.first << "List.end();"
            << endl << getIndent()
            << "     it++) {"  << endl;
  m_indent++;
  *m_stream << getIndent()
            << fixTypeName("DbAddress",
                           "castor::db",
                           m_classInfo->packageName)
            << " ad(" << "*it, \" \", 0);" << endl
            << getIndent()
            << "cnvSvc()->deleteRepByAddress(&ad, false);"
            << endl;
  if (as->first.second == COMPOS_PARENT ||
      as->first.second == AGGREG_PARENT) {
    *m_stream << getIndent()
              << "if (0 == m_delete"
              << capitalizeFirstLetter(as->second.third)
              << "2"
              << capitalizeFirstLetter(as->second.second)
              << "Statement) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "m_delete"
              << capitalizeFirstLetter(as->second.third)
              << "2"
              << capitalizeFirstLetter(as->second.second)
              << "Statement = createStatement(s_delete"
              << capitalizeFirstLetter(as->second.third)
              << "2"
              << capitalizeFirstLetter(as->second.second)
              << "StatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl << getIndent()
              << "m_delete"
              << capitalizeFirstLetter(as->second.third)
              << "2"
              << capitalizeFirstLetter(as->second.second)
              << "Statement->setDouble(1, obj->id());"
              << endl << getIndent()
              << "m_delete"
              << capitalizeFirstLetter(as->second.third)
              << "2"
              << capitalizeFirstLetter(as->second.second)
              << "Statement->setDouble(2, *it);"
              << endl << getIndent()
              << "m_delete"
              << capitalizeFirstLetter(as->second.third)
              << "2"
              << capitalizeFirstLetter(as->second.second)
              << "Statement->executeUpdate();"
              << endl;
  }
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << endl;
}

//=============================================================================
// writeBasicMultNFillObj
//=============================================================================
void CppCppOraCnvWriter::writeBasicMultNFillObj(Assoc* as) {
  writeWideHeaderComment("fillObj" +
                         capitalizeFirstLetter(as->second.second),
                         getIndent(), *m_stream);
  *m_stream << getIndent()
            << "void " << m_classInfo->packageName
            << "::Ora" << m_classInfo->className
            << "Cnv::fillObj"
            << capitalizeFirstLetter(as->second.second)
            << "("
            << fixTypeName(m_classInfo->className + "*",
                           getNamespace(m_classInfo->className),
                           "")
            << " obj)"
            << endl << getIndent() << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           "")
            << ") {"
            << endl;
  m_indent++;

  *m_stream << getIndent() << "// Check select statement"
            << endl << getIndent()
            << "if (0 == m_"
            << capitalizeFirstLetter(as->second.third)
            << "2"
            << capitalizeFirstLetter(as->second.second)
            << "Statement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_"
            << capitalizeFirstLetter(as->second.third)
            << "2"
            << capitalizeFirstLetter(as->second.second)
            << "Statement = createStatement(s_"
            << capitalizeFirstLetter(as->second.third)
            << "2"
            << capitalizeFirstLetter(as->second.second)
            << "StatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent()
            << "// retrieve the object from the database"
            << endl << getIndent()
            << fixTypeName("set", "", "")
            << "<int> " << as->second.first
            << "List;" << endl << getIndent()
            << "m_"
            << capitalizeFirstLetter(as->second.third)
            << "2"
            << capitalizeFirstLetter(as->second.second)
            << "Statement->setDouble(1, obj->id());"
            << endl << getIndent()
            << "oracle::occi::ResultSet *rset = "
            << "m_"
            << capitalizeFirstLetter(as->second.third)
            << "2"
            << capitalizeFirstLetter(as->second.second)
            << "Statement->executeQuery();"
            << endl << getIndent()
            << "while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << as->second.first
            << "List.insert(rset->getInt(1));"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent() << "// Close ResultSet"
            << endl << getIndent()
            << "m_"
            << capitalizeFirstLetter(as->second.third)
            << "2"
            << capitalizeFirstLetter(as->second.second)
            << "Statement->closeResultSet(rset);"
            << endl << getIndent()
            << "// Update objects and mark old ones for deletion"
            << endl << getIndent()
            << fixTypeName("vector", "", "") << "<"
            << fixTypeName(as->second.second,
                           getNamespace(as->second.second),
                           m_classInfo->packageName)
            << "*> toBeDeleted;"
            << endl << getIndent()
            << "for ("
            << fixTypeName("vector", "", "")
            << "<"
            << fixTypeName(as->second.second,
                           getNamespace(as->second.second),
                           m_classInfo->packageName)
            << "*>::iterator it = obj->"
            << as->second.first
            << "().begin();" << endl << getIndent()
            << "     it != obj->"
            << as->second.first
            << "().end();" << endl << getIndent()
            << "     it++) {"  << endl;
  m_indent++;
  *m_stream << getIndent()
            << fixTypeName("set", "", "")
            << "<int>::iterator item;" << endl
            << getIndent() << "if ((item = "
            << as->second.first
            << "List.find((*it)->id())) == "
            << as->second.first
            << "List.end()) {" << endl;
  m_indent++;
  *m_stream << getIndent() << "toBeDeleted.push_back(*it);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "} else {" << endl;
  m_indent++;
  *m_stream << getIndent() << as->second.first
            << "List.erase(item);"
            << endl << getIndent()
            << "cnvSvc()->updateObj((*it));"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  *m_stream << getIndent()
            << "// Delete old objects"
            << endl << getIndent()
            << "for ("
            << fixTypeName("vector", "", "")
            << "<"
            << fixTypeName(as->second.second,
                           getNamespace(as->second.second),
                           m_classInfo->packageName)
            << "*>::iterator it = toBeDeleted.begin();"
            << endl << getIndent()
            << "     it != toBeDeleted.end();"
            << endl << getIndent()
            << "     it++) {"  << endl;
  m_indent++;
  *m_stream << getIndent() << "obj->remove"
            << capitalizeFirstLetter(as->second.first)
            << "(*it);" << endl << getIndent()
            << "delete (*it);" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent()
            << "// Create new objects"
            << endl << getIndent() << "for ("
            << fixTypeName("set", "", "")
            << "<int>::iterator it = "
            << as->second.first << "List.begin();"
            << endl << getIndent()
            << "     it != "
            << as->second.first << "List.end();"
            << endl << getIndent()
            << "     it++) {"  << endl;
  m_indent++;
  *m_stream << getIndent() << "IObject* item"
            << " = cnvSvc()->getObjFromId(*it);"
            << endl << getIndent()
            << "obj->add"
            << capitalizeFirstLetter(as->second.first)
            << "(dynamic_cast<"
            << fixTypeName(as->second.second,
                           getNamespace(as->second.second),
                           m_classInfo->packageName)
            << "*>(item));" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;


  m_indent--;
  *m_stream << getIndent() << "}" << endl << endl;
}

//=============================================================================
// writeCreateRepContent
//=============================================================================
void CppCppOraCnvWriter::writeCreateRepContent() {
  // check whether something needs to be done
  *m_stream << getIndent()
            << "// check whether something needs to be done"
            << endl << getIndent()
            << "if (0 == obj) return;" << endl;
  // Start of try block for database statements
  *m_stream << getIndent() << "try {" << endl;
  m_indent++;
  // First check the statements
  *m_stream << getIndent()
            << "// Check whether the statements are ok"
            << endl << getIndent()
            << "if (0 == m_insertStatement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_insertStatement = createStatement(s_insertStatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  if (isRequest()) {
    *m_stream << getIndent()
              << "if (0 == m_insertStatusStatement) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "m_insertStatusStatement = createStatement(s_insertStatusStatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
  }
  *m_stream << getIndent()
            << "if (0 == m_storeTypeStatement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_storeTypeStatement = createStatement(s_storeTypeStatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << getIndent()
            << "// Get an id for the new object"
            << endl << getIndent()
            << "obj->setId(cnvSvc()->getIds(1));" << endl;
  // Insert the object into the database
  *m_stream << getIndent()
            << "// Now Save the current object"
            << endl << getIndent()
            << "m_storeTypeStatement->setDouble(1, obj->id());"
            << endl << getIndent()
            << "m_storeTypeStatement->setInt(2, obj->type());"
            << endl << getIndent()
            << "m_storeTypeStatement->executeUpdate();"
            << endl;
  if (isRequest()) {
    *m_stream << getIndent()
              << "m_insertStatusStatement->setDouble(1, obj->id());"
              << endl << getIndent()
              << "m_insertStatusStatement->executeUpdate();"
              << endl;
  }
  // create a list of members to be saved
  MemberList members = createMembersList();
  unsigned int n = 1;
  // Go through the members
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    writeSingleSetIntoStatement("insert", *mem, n);
    n++;
  }
  // create a list of associations
  AssocList assocs = createAssocsList();
  // Go through the associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE) {
      writeSingleSetIntoStatement
        ("insert", as->second, n, true, isEnum(as->second.second));
      n++;
    }
  }
  *m_stream << getIndent()
            << "m_insertStatement->executeUpdate();"
            << endl;
  // Commit if needed
  *m_stream << getIndent()
            << "if (autocommit) {" << endl;
  m_indent++;
  *m_stream << getIndent() << "cnvSvc()->getConnection()->commit();"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  // Catch exceptions if any
  *m_stream << getIndent()
            << "} catch (oracle::occi::SQLException e) {"
            << endl;
  m_indent++;
  printSQLError("insert", members, assocs);
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
}

//=============================================================================
// writeUpdateRepContent
//=============================================================================
void CppCppOraCnvWriter::writeUpdateRepContent() {
  // Get the precise object
  *m_stream << getIndent() << m_originalPackage
            << m_classInfo->className << "* obj = " << endl
            << getIndent() << "  dynamic_cast<"
            << m_originalPackage
            << m_classInfo->className << "*>(object);"
            << endl;
  // check whether something needs to be done
  *m_stream << getIndent()
            << "// check whether something needs to be done"
            << endl << getIndent()
            << "if (0 == obj) return;" << endl;
  // Start of try block for database statements
  *m_stream << getIndent() << "try {" << endl;
  m_indent++;
  // First check the statements
  *m_stream << getIndent()
            << "// Check whether the statements are ok"
            << endl;
  *m_stream << getIndent()
            << "if (0 == m_updateStatement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_updateStatement = createStatement(s_updateStatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  // Updates the objects in the database
  *m_stream << getIndent()
            << "// Update the current object"
            << endl;
  // Go through the members
  MemberList members = createMembersList();
  Member* idMem = 0;
  unsigned int n = 1;
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    if (mem->first == "id") {
      idMem = mem;
      continue;
    }
    writeSingleSetIntoStatement("update", *mem, n);
    n++;
  }
  // Go through the associations
  AssocList assocs = createAssocsList();
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE) {
      writeSingleSetIntoStatement
        ("update", as->second, n, true, isEnum(as->second.second));
      n++;
    }
  }
  // Last thing : the id
  if (0 == idMem) {
    *m_stream << getIndent()
              << "INTERNAL ERROR : NO ID ATTRIBUTE" << endl;
  } else {
    writeSingleSetIntoStatement("update", *idMem, n);
  }
  // Now execute statement
  *m_stream << getIndent()
            << "m_updateStatement->executeUpdate();"
            << endl;
  // Commit if needed
  *m_stream << getIndent()
            << "if (autocommit) {" << endl;
  m_indent++;
  *m_stream << getIndent() << "cnvSvc()->getConnection()->commit();"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  // Catch exceptions if any
  *m_stream << getIndent()
            << "} catch (oracle::occi::SQLException e) {"
            << endl;
  m_indent++;
  printSQLError("update", members, assocs);
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
}

//=============================================================================
// writeDeleteRepContent
//=============================================================================
void CppCppOraCnvWriter::writeDeleteRepContent() {
  // Get the precise object
  *m_stream << getIndent() << m_originalPackage
            << m_classInfo->className << "* obj = " << endl
            << getIndent() << "  dynamic_cast<"
            << m_originalPackage
            << m_classInfo->className << "*>(object);"
            << endl;
  // check whether something needs to be done
  *m_stream << getIndent()
            << "// check whether something needs to be done"
            << endl << getIndent()
            << "if (0 == obj) return;" << endl;
  // Start of try block for database statements
  *m_stream << getIndent() << "try {" << endl;
  m_indent++;
  // First check the statements
  *m_stream << getIndent()
            << "// Check whether the statements are ok"
            << endl << getIndent()
            << "if (0 == m_deleteStatement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_deleteStatement = createStatement(s_deleteStatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  if (isRequest()) {
    *m_stream << getIndent()
              << "if (0 == m_deleteStatusStatement) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "m_deleteStatusStatement = createStatement(s_deleteStatusStatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
  }
  *m_stream << getIndent()
            << "if (0 == m_deleteTypeStatement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_deleteTypeStatement = createStatement(s_deleteTypeStatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  // Delete the object from the database
  *m_stream << getIndent()
            << "// Now Delete the object"
            << endl << getIndent()
            << "m_deleteTypeStatement->setDouble(1, obj->id());"
            << endl << getIndent()
            << "m_deleteTypeStatement->executeUpdate();"
            << endl << getIndent()
            << "m_deleteStatement->setDouble(1, obj->id());"
            << endl << getIndent()
            << "m_deleteStatement->executeUpdate();"
            << endl;
  if (isRequest()) {
    *m_stream << getIndent()
              << "m_deleteStatusStatement->setDouble(1, obj->id());"
              << endl << getIndent()
              << "m_deleteStatusStatement->executeUpdate();"
              << endl;
  }
  // create a list of associations
  AssocList assocs = createAssocsList();
  // Go through dependant objects
  for (Assoc* p = assocs.first();
       0 != p;
       p = assocs.next()) {
    if (!isEnum(p->second.second) &&
        p->first.second == COMPOS_PARENT) {
      fixTypeName(p->second.second,
                  getNamespace(p->second.second),
                  m_classInfo->packageName);
      if (p->first.first == MULT_ONE) {
        // One to one association
        *m_stream << getIndent() << "if (obj->"
                  << p->second.first
                  << "() != 0) {" << endl;
        m_indent++;
        fixTypeName("OraCnvSvc",
                    "castor::db::ora",
                    m_classInfo->packageName);
        *m_stream << getIndent()
                  << "cnvSvc()->deleteRep(0, obj->"
                  << p->second.first << "(), false);"
                  << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl;
      } else if (p->first.first == MULT_N) {
        // One to n association, loop over the vector
        *m_stream << getIndent() << "for ("
                  << fixTypeName("vector", "", "")
                  << "<"
                  << fixTypeName(p->second.second,
                                 getNamespace(p->second.second),
                                 m_classInfo->packageName)
                  << "*>::iterator it = obj->"
                  << p->second.first
                  << "().begin();" << endl << getIndent()
                  << "     it != obj->"
                  << p->second.first
                  << "().end();" << endl << getIndent()
                  << "     it++) {"  << endl;
        m_indent++;
        fixTypeName("OraCnvSvc",
                    "castor::db::ora",
                    m_classInfo->packageName);
        *m_stream << getIndent()
                  << "cnvSvc()->deleteRep(0, *it, false);"
                  << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl;
      } else {
        *m_stream << getIndent() << "UNKNOWN MULT" << endl;
      }
    }
  }
  // Delete dependant objects
  if (! assocs.isEmpty()) {
    // Delete links to objects
    for (Assoc* p = assocs.first();
         0 != p;
         p = assocs.next()) {
      if (p->first.second == COMPOS_CHILD ||
          p->first.second == AGGREG_CHILD) {
        *m_stream << getIndent()
                  << "// Delete link to "
                  << p->second.first << " object"
                  << endl << getIndent()
                  << "if (0 != obj->"
                  << p->second.first << "()) {" << endl;
        m_indent++;
        *m_stream << getIndent()
                  << "// Check whether the statement is ok"
                  << endl << getIndent()
                  << "if (0 == m_delete"
                  << capitalizeFirstLetter(p->second.second)
                  << "2"
                  << capitalizeFirstLetter(p->second.third)
                  << "Statement) {" << endl;
        m_indent++;
        *m_stream << getIndent()
                  << "m_delete"
                  << capitalizeFirstLetter(p->second.second)
                  << "2"
                  << capitalizeFirstLetter(p->second.third)
                  << "Statement = createStatement(s_delete"
                  << capitalizeFirstLetter(p->second.second)
                  << "2"
                  << capitalizeFirstLetter(p->second.third)
                  << "StatementString);"
                  << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl
                  << getIndent()
                  << "// Delete links to objects"
                  << endl << getIndent()
                  << "m_delete"
                  << capitalizeFirstLetter(p->second.second)
                  << "2"
                  << capitalizeFirstLetter(p->second.third)
                  << "Statement->setDouble(1, obj->"
                  << p->second.first << "()->id());"
                  << endl << getIndent()
                  << "m_delete"
                  << capitalizeFirstLetter(p->second.second)
                  << "2"
                  << capitalizeFirstLetter(p->second.third)
                  << "Statement->setDouble(2, obj->id());"
                  << endl << getIndent()
                  << "m_delete"
                  << capitalizeFirstLetter(p->second.second)
                  << "2"
                  << capitalizeFirstLetter(p->second.third)
                  << "Statement->executeUpdate();"
                  << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl;
      }
    }
  }
  // Commit if needed
  *m_stream << getIndent()
            << "if (autocommit) {" << endl;
  m_indent++;
  *m_stream << getIndent() << "cnvSvc()->getConnection()->commit();"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  // Catch exceptions if any
  *m_stream << getIndent()
            << "} catch (oracle::occi::SQLException e) {"
            << endl;
  m_indent++;
  MemberList emptyList;
  printSQLError("delete", emptyList, assocs);
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
}

//=============================================================================
// writeCreateObjContent
//=============================================================================
void CppCppOraCnvWriter::writeCreateObjContent() {
  *m_stream << getIndent() << fixTypeName("DbAddress",
                                          "castor::db",
                                          m_classInfo->packageName)
            << "* ad = " << endl
            << getIndent() << "  dynamic_cast<"
            << fixTypeName("DbAddress",
                           "castor::db",
                           m_classInfo->packageName)
            << "*>(address);"
            << endl;
  // Start of try block for database statements
  *m_stream << getIndent() << "try {" << endl;
  m_indent++;
  // First check the select statement
  *m_stream << getIndent()
            << "// Check whether the statement is ok"
            << endl << getIndent()
            << "if (0 == m_selectStatement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_selectStatement = createStatement(s_selectStatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent() << "// retrieve the object from the database"
            << endl << getIndent()
            << "m_selectStatement->setDouble(1, ad->id());"
            << endl << getIndent()
            << "oracle::occi::ResultSet *rset = m_selectStatement->executeQuery();"
            << endl << getIndent()
            << "if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << fixTypeName("NoEntry",
                           "castor.exception",
                           m_classInfo->packageName)
            << " ex;" << endl << getIndent()
            << "ex.getMessage() << \"No object found for id :\""
            << " << ad->id();" << endl
            << getIndent() << "throw ex;" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << getIndent()
            << "// create the new Object" << endl
            << getIndent() << m_originalPackage
            << m_classInfo->className << "* object = new "
            << m_originalPackage << m_classInfo->className
            << "();" << endl << getIndent()
            << "// Now retrieve and set members" << endl;
  // create a list of members to be saved
  MemberList members = createMembersList();
  // Go through the members
  unsigned int n = 1;
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    writeSingleGetFromSelect(*mem, n);
    n++;
  }
  // Go through the one to one associations dealing with enums
  AssocList assocs = createAssocsList();
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE &&
        isEnum(as->second.second)) {
      writeSingleGetFromSelect(as->second, n, false, true);
    }
    n++;
  }
  // Close request
  *m_stream << getIndent()
            << "m_selectStatement->closeResultSet(rset);"
            << endl;
  // Return result
  *m_stream << getIndent() << "return object;" << endl;
  // Catch exceptions if any
  m_indent--;
  *m_stream << getIndent()
            << "} catch (oracle::occi::SQLException e) {"
            << endl;
  m_indent++;
  printSQLError("select", members, assocs);
  m_indent--;
  *m_stream << getIndent()
            << "}" << endl;
}

//=============================================================================
// writeUpdateObjContent
//=============================================================================
void CppCppOraCnvWriter::writeUpdateObjContent() {
  // Start of try block for database statements
  *m_stream << getIndent() << "try {" << endl;
  m_indent++;
  // First check the select statement
  *m_stream << getIndent()
            << "// Check whether the statement is ok"
            << endl << getIndent()
            << "if (0 == m_selectStatement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_selectStatement = createStatement(s_selectStatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent()
            << "// retrieve the object from the database"
            << endl << getIndent()
            << "m_selectStatement->setDouble(1, obj->id());"
            << endl << getIndent()
            << "oracle::occi::ResultSet *rset = m_selectStatement->executeQuery();"
            << endl << getIndent()
            << "if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << fixTypeName("NoEntry",
                           "castor.exception",
                           m_classInfo->packageName)
            << " ex;" << endl << getIndent()
            << "ex.getMessage() << \"No object found for id :\""
            << " << obj->id();" << endl
            << getIndent() << "throw ex;" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << getIndent()
            << "// Now retrieve and set members" << endl;
  // Get the precise object
  *m_stream << getIndent() << m_originalPackage
            << m_classInfo->className << "* object = " << endl
            << getIndent() << "  dynamic_cast<"
            << m_originalPackage
            << m_classInfo->className << "*>(obj);"
            << endl;
  // Go through the members
  MemberList members = createMembersList();
  unsigned int n = 1;
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    writeSingleGetFromSelect(*mem, n);
    n++;
  }
  // create a list of associations
  AssocList assocs = createAssocsList();
  // Go through the one to one associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE) {
      // Enums are a simple case
      if (isEnum(as->second.second)) {
        writeSingleGetFromSelect(as->second, n, false, true);
      } else {
        //writeBasicMult1FillObj
      }
      n++;
    }
  }
  // Close request
  *m_stream << getIndent()
            << "m_selectStatement->closeResultSet(rset);"
            << endl;
  // Go through one to n associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_N) {
      // writeBasicMultNFillObj
    }
  }
  // Catch exceptions if any
  m_indent--;
  *m_stream << getIndent()
            << "} catch (oracle::occi::SQLException e) {"
            << endl;
  m_indent++;
  printSQLError("update", members, assocs);
  m_indent--;
  *m_stream << getIndent()
            << "}" << endl;
}

//=============================================================================
// writeSingleSetIntoStatement
//=============================================================================
void
CppCppOraCnvWriter::writeSingleSetIntoStatement(QString statement,
                                                Member pair,
                                                int n,
                                                bool isAssoc,
                                                bool isEnum) {
  // deal with arrays of chars
  bool isArray = pair.second.find('[') > 0;
  if (isArray) {
    int i1 = pair.second.find('[');
    int i2 = pair.second.find(']');
    QString length = pair.second.mid(i1+1, i2-i1-1);
    *m_stream << getIndent() << fixTypeName("string", "", "")
              << " " << pair.first << "S((const char*)" << "obj->"
              << pair.first << "(), " << length << ");"
              << endl;
  }
  *m_stream << getIndent()
            << "m_" << statement << "Statement->set";
  if (isAssoc) {
    *m_stream << "Double";
  } else {
    *m_stream << getOraType(pair.second);
  }
  *m_stream << "(" << n << ", ";
  if (isArray) {
    *m_stream << pair.first << "S";
  } else {
    if (isEnum)
      *m_stream << "(int)";
    *m_stream << "obj->"
              << pair.first
              << "()";
    if (isAssoc && !isEnum) *m_stream << " ? obj->"
                                      << pair.first
                                      << "()->id() : 0";
  }
  *m_stream << ");" << endl;
}

//=============================================================================
// writeSingleGetFromSelect
//=============================================================================
void CppCppOraCnvWriter::writeSingleGetFromSelect(Member pair,
                                                  int n,
                                                  bool isAssoc,
                                                  bool isEnum) {
  *m_stream << getIndent();
  // deal with arrays of chars
  bool isArray = pair.second.find('[') > 0;
  if (isAssoc) {
    *m_stream << "u_signed64 " << pair.first
              << "Id = ";
  } else {
    *m_stream << "object->set"
              << capitalizeFirstLetter(pair.first)
              << "(";
  }
  if (isArray) {
    *m_stream << "("
              << pair.second.left(pair.second.find('['))
              << "*)";
  }
  if (isEnum) {
    *m_stream << "(enum "
              << fixTypeName(pair.second,
                             getNamespace(pair.second),
                             m_classInfo->packageName)
              << ")";
  }
  if (isAssoc || pair.second == "u_signed64") {
    *m_stream << "(unsigned long long)";
  }
  *m_stream << "rset->get";
  if (isEnum) {
    *m_stream << "Int";
  } else if (isAssoc) {
    *m_stream << "Double";
  } else {
    *m_stream << getOraType(pair.second);
  }
  *m_stream << "(" << n << ")";
  if (isArray) *m_stream << ".data()";
  if (!isAssoc) *m_stream << ")";
  *m_stream << ";" << endl;
}

//=============================================================================
// getOraType
//=============================================================================
QString CppCppOraCnvWriter::getOraType(QString& type) {
  QString oraType = getSimpleType(type);
  if (oraType == "short" ||
      oraType == "long" ||
      oraType == "bool" ||
      oraType == "int") {
    oraType = "int";
  } else if (oraType == "u_signed64") {
    oraType = "double";
  }
  if (oraType.startsWith("char")) oraType = "string";
  oraType = capitalizeFirstLetter(oraType);
  return oraType;
}

//=============================================================================
// getSQLType
//=============================================================================
QString CppCppOraCnvWriter::getSQLType(QString& type) {
  QString SQLType = getSimpleType(type);
  SQLType = SQLType;
  if (SQLType == "short" ||
      SQLType == "long" ||
      SQLType == "bool" ||
      SQLType == "int") {
    SQLType = "NUMBER";
  } else if (type == "u_signed64") {
    SQLType = "INTEGER";
  } else if (SQLType == "string") {
    SQLType = "VARCHAR(255)";
  } else if (SQLType.left(5) == "char["){
    QString res = "CHAR(";
    res.append(SQLType.mid(5, SQLType.find("]")-5));
    res.append(")");
    SQLType = res;
  } else if (m_castorTypes.find(SQLType) != m_castorTypes.end()) {
    SQLType = "NUMBER";
  }
  return SQLType;
}

//=============================================================================
// printSQLError
//=============================================================================
void CppCppOraCnvWriter::printSQLError(QString name,
                                       MemberList& members,
                                       AssocList& assocs) {
  fixTypeName("OraCnvSvc", "castor::db::ora", m_classInfo->packageName);
  *m_stream << getIndent() << "try {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "// Always try to rollback" << endl
            << getIndent()
            << "cnvSvc()->getConnection()->rollback();"
            << endl << getIndent()
            << "if (3114 == e.getErrorCode() || 28 == e.getErrorCode()) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << "// We've obviously lost the ORACLE connection here"
            << endl << getIndent() << "cnvSvc()->dropConnection();"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  *m_stream << getIndent()
            << "} catch (oracle::occi::SQLException e) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << "// rollback failed, let's drop the connection for security"
            << endl << getIndent() << "cnvSvc()->dropConnection();"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << getIndent()
            << fixTypeName("InvalidArgument",
                           "castor.exception",
                           m_classInfo->packageName)
            << " ex; // XXX Fix it, depending on ORACLE error"
            << endl << getIndent()
            << "ex.getMessage() << \"Error in " << name
            << " request :\""
            << endl << getIndent()
            << "                << std::endl << e.what() << std::endl"
            << endl << getIndent()
            << "                << \"Statement was :\" << std::endl"
            << endl << getIndent()
            << "                << s_" << name
            << "StatementString << std::endl";
  if (name == "select") {
    *m_stream << endl << getIndent()
              << "                << \"and id was \" << ad->id() << std::endl;";
  } else if (name == "delete") {
    *m_stream << endl << getIndent()
              << "                << \"and id was \" << obj->id() << std::endl;";
  } else if (name == "update") {
    *m_stream << endl << getIndent()
              << "                << \"and id was \" << obj->id() << std::endl;";
  } else if (name == "insert") {
    *m_stream << endl << getIndent()
              << "                << \"and parameters' values were :\" << std::endl";
    for (Member* mem = members.first();
         0 != mem;
         mem = members.next()) {
      *m_stream << endl << getIndent()
                << "                << \"  "
                << mem->first << " : \" << obj->"
                << mem->first << "() << std::endl";
    }
    // Go through the associations
    for (Assoc* as = assocs.first();
         0 != as;
         as = assocs.next()) {
      if (as->first.first == MULT_ONE) {
        *m_stream << endl << getIndent()
                  << "                << \"  "
                  << as->second.first << " : \" << obj->"
                  << as->second.first << "() << std::endl";
      }
    }
  }
  *m_stream << ";" << endl << getIndent()
            << "throw ex;" << endl;
}

//=============================================================================
// isRequest
//=============================================================================
bool CppCppOraCnvWriter::isRequest() {
  UMLObject* obj = m_doc->findUMLObject(QString("Request"),
                                        Uml::ot_Class);
  const UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
  return m_classInfo->allSuperclasses.contains(concept);
}

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
         << "/* SQL statements for type Cuuid */" << endl
         << "DROP TABLE rh_Cuuid;" << endl
         << "CREATE TABLE rh_Cuuid (id INTEGER PRIMARY KEY, time_low NUMBER, time_mid NUMBER, time_hv NUMBER, clock_hi NUMBER, clock_low NUMBER, node CHAR(6));" << endl
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
    if (as->first.first == MULT_N) {
      *m_stream << getIndent()
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
    if (as->first.first == MULT_N) {
      *m_stream << "," << endl << getIndent()
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
    if (as->first.first == MULT_N) {
      *m_stream << getIndent()
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
    if (as->first.first == MULT_N) {
      *m_stream << getIndent()
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
            << "// Mark the current object as done"
            << endl << getIndent()
            << "alreadyDone.insert(obj);" << endl;
  // Prepare id count
  *m_stream << getIndent()
            << "// Set ids of all objects"
            << endl << getIndent()
            << "int nids = obj->id() == 0 ? 1 : 0;"
            << endl;
  // create a list of associations
  AssocList assocs = createAssocsList();
  // list dependant objects
  if (! assocs.isEmpty()) {
    *m_stream << getIndent()
              << "// check which objects need to be saved/updated and keeps a list of them"
              << endl << getIndent()
              << fixTypeName("list", "", "")
              << "<" << fixTypeName("IObject*",
                                    "castor",
                                    m_classInfo->packageName)
              << "> toBeSaved;"
              << endl << getIndent()
              << fixTypeName("list", "", "")
              << "<" << fixTypeName("IObject*",
                                    "castor",
                                    m_classInfo->packageName)
              << "> toBeUpdated;"
              << endl;
    for (Assoc* p = assocs.first();
         0 != p;
         p = assocs.next()) {
      if (!isEnum(p->second.second)) {
        fixTypeName(p->second.second,
                    getNamespace(p->second.second),
                    m_classInfo->packageName);
        if (p->first.first == MULT_ONE) {
          if (p->first.second != COMPOS_CHILD) {
            *m_stream << getIndent() << "if (recursive) {"
                      << endl;
            m_indent++;
          }
          // One to one association
          *m_stream << getIndent() << "if (alreadyDone.find(obj->"
                    << p->second.first
                    << "()) == alreadyDone.end() &&" << endl
                    << getIndent() << "    obj->"
                    << p->second.first
                    << "() != 0) {" << endl;
          m_indent++;
          *m_stream << getIndent() << "if (0 == obj->"
                    << p->second.first
                    << "()->id()) {" << endl;
          m_indent++;
          if (p->first.second == COMPOS_CHILD) {
            *m_stream << getIndent() << "if (!recursive) {"
                      << endl;
            m_indent++;
            *m_stream << getIndent()
                      << "castor::exception::InvalidArgument ex;"
                      << endl << getIndent()
                      << "ex.getMessage() << \"CreateNoRep called on type "
                      << m_classInfo->className
                      << " while its " << p->second.first
                      << " does not exist in the database.\";"
                      << endl << getIndent()
                      << "throw ex;" << endl;
            m_indent--;
            *m_stream << getIndent() << "}" << endl;
          }
          *m_stream << getIndent()
                    << "toBeSaved.push_back(obj->"
                    << p->second.first << "());" << endl
                    << getIndent() << "nids++;" << endl;
          m_indent--;
          *m_stream << getIndent() << "} else {" << endl;
          m_indent++;
          if (p->first.second == COMPOS_CHILD) {
            *m_stream << getIndent() << "if (recursive) {"
                      << endl;
            m_indent++;
          }
          *m_stream << getIndent()
                    << "toBeUpdated.push_back(obj->"
                    << p->second.first << "());" << endl;
          if (p->first.second == COMPOS_CHILD) {
            m_indent--;
            *m_stream << getIndent() << "}" << endl;
          }
          m_indent--;
          *m_stream << getIndent() << "}" << endl;
          m_indent--;
          *m_stream << getIndent() << "}" << endl;
          if (p->first.second != COMPOS_CHILD) {
            m_indent--;
            *m_stream << getIndent() << "}" << endl;
          }
        } else if (p->first.first == MULT_N) {
          // One to n association, loop over the vector
          *m_stream << getIndent() << "if (recursive) {"
                    << endl;
          m_indent++;
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
          *m_stream << getIndent()
                    << "if (alreadyDone.find(*it) == alreadyDone.end()) {"
                    << endl;
          m_indent++;
          *m_stream << getIndent() << "if (0 == (*it)->id()) {" << endl;
          m_indent++;
          *m_stream << getIndent()
                    << "toBeSaved.push_back(*it);" << endl
                    << getIndent() << "nids++;" << endl;
          m_indent--;
          *m_stream << getIndent() << "} else {" << endl;
          m_indent++;
          *m_stream << getIndent()
                    << "toBeUpdated.push_back(*it);" << endl;
          m_indent--;
          *m_stream << getIndent() << "}" << endl;
          m_indent--;
          *m_stream << getIndent() << "}" << endl;
          m_indent--;
          *m_stream << getIndent() << "}" << endl;
          m_indent--;
          *m_stream << getIndent() << "}"
                    << endl;
        }
      }
    }
  }
  fixTypeName("OraCnvSvc", "castor::db::ora", m_classInfo->packageName);
  *m_stream << getIndent()
            << "u_signed64 id = cnvSvc()->getIds(nids);"
            << endl << getIndent()
            << "if (0 == obj->id()) obj->setId(id++);" << endl;
  if (! assocs.isEmpty()) {
    *m_stream << getIndent()
              << "for (" << fixTypeName("list" ,"","")
              << "<" << fixTypeName("IObject*",
                                    "castor",
                                    m_classInfo->packageName)
              << ">::const_iterator it = toBeSaved.begin();"
              << endl << getIndent() << "     it != toBeSaved.end();"
              << endl << getIndent() << "     it++) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "(*it)->setId(id++);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
  }
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
  // extract the blobs and their lengths
  QMap<QString,QString> blobs = extractBlobsFromMembers(members);
  unsigned int n = 1;
  // Go through the members
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    writeSingleSetIntoStatement("insert", *mem, n);
    n++;
  }
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
  // Save dependant objects
  if (! assocs.isEmpty()) {
    *m_stream << getIndent() << "if (recursive) {"
              << endl;
    m_indent++;
    *m_stream << getIndent()
              << "// Save dependant objects that need it"
              << endl << getIndent()
              << "for (" << fixTypeName("list" ,"","")
              << "<" << fixTypeName("IObject*",
                                    "castor",
                                    m_classInfo->packageName)
              << ">::iterator it = toBeSaved.begin();"
              << endl << getIndent() << "     it != toBeSaved.end();"
              << endl << getIndent() << "     it++) {" << endl;
    m_indent++;
    fixTypeName("OraCnvSvc", "castor::db::ora", m_classInfo->packageName);
    *m_stream << getIndent()
              << "cnvSvc()->createRep(0, *it, alreadyDone, false, true);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
    *m_stream << getIndent()
              << "// Update dependant objects that need it"
              << endl << getIndent()
              << "for (" << fixTypeName("list" ,"","")
              << "<" << fixTypeName("IObject*",
                                    "castor",
                                    m_classInfo->packageName)
              << ">::iterator it = toBeUpdated.begin();"
              << endl << getIndent() << "     it != toBeUpdated.end();"
              << endl << getIndent() << "     it++) {" << endl;
    m_indent++;
    fixTypeName("OraCnvSvc", "castor::db::ora", m_classInfo->packageName);
    *m_stream << getIndent()
              << "cnvSvc()->updateRep(0, *it, alreadyDone, false, true);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
    // Save links to objects for one to n associations
    for (Assoc* p = assocs.first();
         0 != p;
         p = assocs.next()) {
      if (p->first.second == COMPOS_CHILD ||
          p->first.second == AGGREG_CHILD) {
        *m_stream << getIndent()
                  << "// Deal with " << p->second.first
                  << endl << getIndent()
                  << "if (0 != obj->"
                  << p->second.first << "()) {"
                  << endl;
        m_indent++;
        *m_stream << getIndent()
                  << "if (0 == m_insert"
                  << capitalizeFirstLetter(p->second.second)
                  << "2"
                  << capitalizeFirstLetter(p->second.third)
                  << "Statement) {" << endl;
        m_indent++;
        *m_stream << getIndent()
                  << "m_insert"
                  << capitalizeFirstLetter(p->second.second)
                  << "2"
                  << capitalizeFirstLetter(p->second.third)
                  << "Statement = createStatement(s_insert"
                  << capitalizeFirstLetter(p->second.second)
                  << "2"
                  << capitalizeFirstLetter(p->second.third)
                  << "StatementString);"
                  << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl
                  << getIndent() << "m_insert"
                  << capitalizeFirstLetter(p->second.second)
                  << "2"
                  << capitalizeFirstLetter(p->second.third)
                  << "Statement->setDouble(1, obj->"
                  << p->second.first << "()->id());"
                  << endl << getIndent()
                  << "m_insert"
                  << capitalizeFirstLetter(p->second.second)
                  << "2"
                  << capitalizeFirstLetter(p->second.third)
                  << "Statement->setDouble(2, obj->id());"
                  << endl << getIndent()
                  << "m_insert"
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
  *m_stream << getIndent() << "}" << endl
            << getIndent() << "if (0 == m_updateStatement) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << fixTypeName("Internal",
                           "castor.exception",
                           m_classInfo->packageName)
            << " ex;" << endl << getIndent()
            << "ex.getMessage() << \"Unable to create statement :\""
            << " << std::endl" << endl << getIndent()
            << "                << s_updateStatementString;"
            << endl << getIndent() << "throw ex;" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  // create a list of associations and check if there are
  // 1 to 1 associations
  AssocList assocs = createAssocsList();
  bool one2oneAssocs = false;
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE && !isEnum(as->second.second)) {
      one2oneAssocs = true;
      break;
    }
  }
  if (one2oneAssocs) {
    *m_stream << getIndent() << "if (recursive) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "if (0 == m_selectStatement) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "m_selectStatement = createStatement(s_selectStatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl
              << getIndent() << "if (0 == m_selectStatement) {"
              << endl;
    m_indent++;
    *m_stream << getIndent()
              << fixTypeName("Internal",
                             "castor.exception",
                             m_classInfo->packageName)
              << " ex;" << endl << getIndent()
              << "ex.getMessage() << \"Unable to create statement :\""
              << " << std::endl" << endl << getIndent()
              << "                << s_selectStatementString;"
              << endl << getIndent() << "throw ex;" << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
  }
  *m_stream << getIndent()
            << "// Mark the current object as done"
            << endl << getIndent()
            << "alreadyDone.insert(obj);" << endl;
  // First deal with 1 to 1 associations
  // create a list of members
  MemberList members = createMembersList();
  unsigned int n = 1;
  if (one2oneAssocs) {
    for (Member* mem = members.first(); 0 != mem; mem = members.next()) n++;
    // extract the blobs and their lengths
    QMap<QString,QString> blobs = extractBlobsFromMembers(members);
    *m_stream << getIndent() << "if (recursive) {" << endl;
    m_indent++;
    // get current object in the database, in order to
    // compare with new one
    *m_stream << getIndent()
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
    // Go through the one to one associations
    for (Assoc* as = assocs.first();
         0 != as;
         as = assocs.next()) {
      if (as->first.first == MULT_ONE) {
        if (!isEnum(as->second.second)) {
          // One to One association
          *m_stream << getIndent()
                    << "// Dealing with " << as->second.first
                    << endl << getIndent() << "{" << endl;
          m_indent++;
          writeSingleGetFromSelect(as->second, n, true);
          *m_stream << getIndent()
                    << fixTypeName("DbAddress",
                                   "castor::db",
                                   m_classInfo->packageName)
                    << " ad(" << as->second.first
                    << "Id, \" \", 0);" << endl
                    << getIndent()
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
          if (as->first.second == COMPOS_CHILD ||
              as->first.second == AGGREG_CHILD) {
            *m_stream << getIndent()
                      << "if (0 == m_delete"
                      << capitalizeFirstLetter(as->second.second)
                      << "2"
                      << capitalizeFirstLetter(as->second.third)
                      << "Statement) {" << endl;
            m_indent++;
            *m_stream << getIndent()
                      << "m_delete"
                      << capitalizeFirstLetter(as->second.second)
                      << "2"
                      << capitalizeFirstLetter(as->second.third)
                      << "Statement = createStatement(s_delete"
                      << capitalizeFirstLetter(as->second.second)
                      << "2"
                      << capitalizeFirstLetter(as->second.third)
                      << "StatementString);"
                      << endl;
            m_indent--;
            *m_stream << getIndent() << "}" << endl << getIndent()
                      << "m_delete"
                      << capitalizeFirstLetter(as->second.second)
                      << "2"
                      << capitalizeFirstLetter(as->second.third)
                      << "Statement->setDouble(1, obj->"
                      << as->second.first << "()->id());"
                      << endl << getIndent()
                      << "m_delete"
                      << capitalizeFirstLetter(as->second.second)
                      << "2"
                      << capitalizeFirstLetter(as->second.third)
                      << "Statement->setDouble(2, obj->id());"
                      << endl << getIndent()
                      << "m_delete"
                      << capitalizeFirstLetter(as->second.second)
                      << "2"
                      << capitalizeFirstLetter(as->second.third)
                      << "Statement->executeUpdate();"
                      << endl;
          }
          m_indent--;
          *m_stream << getIndent() << "}" << endl;
          *m_stream << getIndent()
                    << "if (" << as->second.first
                    << "Id == 0) {" << endl;
          m_indent++;
          *m_stream << getIndent()
                    << "if (0 != obj->"
                    << as->second.first
                    << "()) {" << endl;
          m_indent++;
          *m_stream << getIndent()
                    << "if (alreadyDone.find(obj->"
                    << as->second.first
                    << "()) == alreadyDone.end()) {"
                    << endl;
          m_indent++;
          *m_stream << getIndent()
                    << "cnvSvc()->createRep(&ad, obj->"
                    << as->second.first
                    << "(), alreadyDone, false, true);" << endl;
          if (as->first.second == COMPOS_CHILD ||
              as->first.second == AGGREG_CHILD) {
            *m_stream << getIndent()
                      << "if (0 == m_insert"
                      << capitalizeFirstLetter(as->second.second)
                      << "2"
                      << capitalizeFirstLetter(as->second.third)
                      << "Statement) {" << endl;
            m_indent++;
            *m_stream << getIndent()
                      << "m_insert"
                      << capitalizeFirstLetter(as->second.second)
                      << "2"
                      << capitalizeFirstLetter(as->second.third)
                      << "Statement = createStatement(s_insert"
                      << capitalizeFirstLetter(as->second.second)
                      << "2"
                      << capitalizeFirstLetter(as->second.third)
                      << "StatementString);"
                      << endl;
            m_indent--;
            *m_stream << getIndent() << "}" << endl
                      << getIndent()
                      << "m_insert"
                      << capitalizeFirstLetter(as->second.second)
                      << "2"
                      << capitalizeFirstLetter(as->second.third)
                      << "Statement->setDouble(1, obj->"
                      << as->second.first << "()->id());"
                      << endl << getIndent()
                      << "m_insert"
                      << capitalizeFirstLetter(as->second.second)
                      << "2"
                      << capitalizeFirstLetter(as->second.third)
                      << "Statement->setDouble(2, obj->id());"
                      << endl << getIndent()
                      << "m_insert"
                      << capitalizeFirstLetter(as->second.second)
                      << "2"
                      << capitalizeFirstLetter(as->second.third)
                      << "Statement->executeUpdate();"
                      << endl;
          }
          m_indent--;
          *m_stream << getIndent() << "}" << endl;
          m_indent--;
          *m_stream << getIndent() << "}" << endl;
          m_indent--;
          *m_stream << getIndent() << "} else {" << endl;
          m_indent++;
          *m_stream << getIndent()
                    << "if (alreadyDone.find(obj->"
                    << as->second.first
                    << "()) == alreadyDone.end()) {"
                    << endl;
          m_indent++;
          *m_stream << getIndent()
                    << "cnvSvc()->updateRep(&ad, obj->"
                    << as->second.first
                    << "(), alreadyDone, false, recursive);" << endl;
          m_indent--;
          *m_stream << getIndent() << "}" << endl;
          m_indent--;
          *m_stream << getIndent() << "}" << endl;
          m_indent--;
          *m_stream << getIndent() << "}" << endl;
        }
        n++;
      }
    }
    // Close request
    *m_stream << getIndent()
              << "m_selectStatement->closeResultSet(rset);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
  }
  // Updates the objects in the database
  *m_stream << getIndent()
            << "// Now Update the current object"
            << endl;
  n = 1;
  // Go through the members
  Member* idMem = 0;
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
  // Go through 1 to N associations
  if (!assocs.isEmpty()) {
    *m_stream << getIndent() << "if (recursive) {" << endl;
    m_indent++;
    for (Assoc* p = assocs.first();
         0 != p;
         p = assocs.next()) {
      if (p->first.first == MULT_N) {
        *m_stream << getIndent()
                  << "// Dealing with " << p->second.first
                  << endl << getIndent() << "{" << endl;
        m_indent++;
        *m_stream << getIndent()
                  << "if (0 == m_"
                  << capitalizeFirstLetter(p->second.third)
                  << "2"
                  << capitalizeFirstLetter(p->second.second)
                  << "Statement) {" << endl;
        m_indent++;
        *m_stream << getIndent()
                  << "m_"
                  << capitalizeFirstLetter(p->second.third)
                  << "2"
                  << capitalizeFirstLetter(p->second.second)
                  << "Statement = createStatement(s_"
                  << capitalizeFirstLetter(p->second.third)
                  << "2"
                  << capitalizeFirstLetter(p->second.second)
                  << "StatementString);"
                  << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl
                  << getIndent()
                  << fixTypeName("set", "", "")
                  << "<int> " << p->second.first
                  << "List;" << endl << getIndent()
                  << "m_"
                  << capitalizeFirstLetter(p->second.third)
                  << "2"
                  << capitalizeFirstLetter(p->second.second)
                  << "Statement->setDouble(1, obj->id());"
                  << endl << getIndent()
                  << "oracle::occi::ResultSet *rset = "
                  << "m_"
                  << capitalizeFirstLetter(p->second.third)
                  << "2"
                  << capitalizeFirstLetter(p->second.second)
                  << "Statement->executeQuery();"
                  << endl << getIndent()
                  << "while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {"
                  << endl;
        m_indent++;
        *m_stream << getIndent()
                  << p->second.first
                  << "List.insert(rset->getInt(1));"
                  << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl
                  << getIndent()
                  << "m_"
                  << capitalizeFirstLetter(p->second.third)
                  << "2"
                  << capitalizeFirstLetter(p->second.second)
                  << "Statement->closeResultSet(rset);"
                  << endl << getIndent()
                  << "for ("
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
        *m_stream << getIndent()
                  << fixTypeName("set", "", "")
                  << "<int>::iterator item;" << endl
                  << getIndent() << "if ((item = "
                  << p->second.first
                  << "List.find((*it)->id())) == "
                  << p->second.first
                  << "List.end()) {" << endl;
        m_indent++;
        *m_stream << getIndent()
                  << "if (alreadyDone.find(*it) == alreadyDone.end()) {"
                  << endl;
        m_indent++;
        *m_stream << getIndent()
                  << "cnvSvc()->createRep(0, *it, alreadyDone, false, true);"
                  << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl;
        m_indent--;
        *m_stream << getIndent() << "} else {" << endl;
        m_indent++;
        *m_stream << getIndent() << p->second.first
                  << "List.erase(item);"
                  << endl << getIndent()
                  << "if (alreadyDone.find(*it) == alreadyDone.end()) {"
                  << endl;
        m_indent++;
        *m_stream << getIndent()
                  << "cnvSvc()->updateRep(0, *it, alreadyDone, false, recursive);"
                  << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl
                  << getIndent() << "for ("
                  << fixTypeName("set", "", "")
                  << "<int>::iterator it = "
                  << p->second.first << "List.begin();"
                  << endl << getIndent()
                  << "     it != "
                  << p->second.first << "List.end();"
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
        m_indent--;
        *m_stream << getIndent() << "}" << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl;
      }
    }
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
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
  *m_stream << getIndent() << "}" << endl << getIndent()
            << "// Mark the current object as done"
            << endl << getIndent()
            << "alreadyDone.insert(obj);" << endl;
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
        *m_stream << getIndent() << "if (alreadyDone.find(obj->"
                  << p->second.first
                  << "()) == alreadyDone.end() &&" << endl
                  << getIndent() << "    obj->"
                  << p->second.first
                  << "() != 0) {" << endl;
        m_indent++;
        fixTypeName("OraCnvSvc",
                    "castor::db::ora",
                    m_classInfo->packageName);
        *m_stream << getIndent()
                  << "cnvSvc()->deleteRep(0, obj->"
                  << p->second.first << "(), alreadyDone, false);"
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
        *m_stream << getIndent()
                  << "if (alreadyDone.find(*it) == alreadyDone.end()) {"
                  << endl;
        m_indent++;
        fixTypeName("OraCnvSvc",
                    "castor::db::ora",
                    m_classInfo->packageName);
        *m_stream << getIndent()
                  << "cnvSvc()->deleteRep(0, *it, alreadyDone, false);"
                  << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl;
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
  //   *m_stream << getIndent()
  //             << "// Get the precise address" << endl
  //             << getIndent()
  //             << fixTypeName("DbAddress",
  //                            "castor::db",
  //                            m_classInfo->packageName)
  //             << "* ad = " << endl
  //             << getIndent() << "  dynamic_cast<"
  //             << fixTypeName("DbAddress",
  //                            "castor::db",
  //                            m_classInfo->packageName)
  //             << "*>(address);"
  //             << endl << getIndent()
  //             << "// create the new Object" << endl
  //             << getIndent() << m_originalPackage
  //             << m_classInfo->className << "* object = new "
  //             << m_originalPackage << m_classInfo->className
  //             << "();" << endl
  //             << getIndent() << "object->setId(ad->id());"
  //             << endl << getIndent()
  //             << "updateObj(object, newlyCreated);" << endl
  //             << getIndent() << "return object;" << endl;
  // Get the precise address
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
            << getIndent() << "if (0 == m_selectStatement) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << fixTypeName("Internal",
                           "castor.exception",
                           m_classInfo->packageName)
            << " ex;" << endl << getIndent()
            << "ex.getMessage() << \"Unable to create statement :\""
            << " << std::endl" << endl << getIndent()
            << "                << s_selectStatementString;"
            << endl << getIndent() << "throw ex;" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  *m_stream << getIndent() << "// retrieve the object from the database"
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
  // extract the blobs and their lengths
  QMap<QString,QString> blobs = extractBlobsFromMembers(members);
  // Go through the members
  unsigned int n = 1;
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    writeSingleGetFromSelect(*mem, n);
    n++;
  }
  // Add new object to the list of newly created objects
  *m_stream << getIndent()
            << "newlyCreated[object->id()] = object;"
            << endl;
  // create a list of associations
  AssocList assocs = createAssocsList();
  // Go through the one to one associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE) {
      // do not consider enums
      if (isEnum(as->second.second)) {
        writeSingleGetFromSelect(as->second, n, false, true);
      } else {
        // One to One association
        writeSingleGetFromSelect(as->second, n, true);
        *m_stream << getIndent()
                  << "IObject* obj"
                  << capitalizeFirstLetter(as->second.first)
                  << " = cnvSvc()->getObjFromId("
                  << as->second.first
                  << "Id, newlyCreated);"
                  << endl << getIndent()
                  << "object->set"
                  << capitalizeFirstLetter(as->second.first)
                  << "(dynamic_cast<"
                  << fixTypeName(as->second.second,
                                 getNamespace(as->second.second),
                                 m_classInfo->packageName)
                  << "*>(obj"
                  << capitalizeFirstLetter(as->second.first)
                  << "));" << endl;
      }
      n++;
    }
  }
  // Close request
  *m_stream << getIndent()
            << "m_selectStatement->closeResultSet(rset);"
            << endl;
  // Go through one to n associations
  bool first = true;
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_N) {
      if (first) {
        first = false;
        *m_stream << getIndent()
                  << "// Get ids of objs to retrieve"
                  << endl;
      }
      *m_stream << getIndent()
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
      *m_stream << getIndent() << "}"
                << endl << getIndent()
                << "m_"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << "Statement->setDouble(1, ad->id());"
                << endl << getIndent()
                << "rset = "
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
                << "IObject* obj"
                << " = cnvSvc()->getObjFromId(rset->getInt(1), newlyCreated);"
                << endl << getIndent()
                << "object->add"
                << capitalizeFirstLetter(as->second.first)
                << "(dynamic_cast<"
                << fixTypeName(as->second.second,
                               getNamespace(as->second.second),
                               m_classInfo->packageName)
                << "*>(obj));" << endl;
      m_indent--;
      *m_stream << getIndent() << "}" << endl;
      *m_stream << getIndent()
                << "m_"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << "Statement->closeResultSet(rset);" << endl;
    }
  }
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
            << getIndent() << "if (0 == m_selectStatement) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << fixTypeName("Internal",
                           "castor.exception",
                           m_classInfo->packageName)
            << " ex;" << endl << getIndent()
            << "ex.getMessage() << \"Unable to create statement :\""
            << " << std::endl" << endl << getIndent()
            << "                << s_selectStatementString;"
            << endl << getIndent() << "throw ex;" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  *m_stream << getIndent()
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
  // create a list of members to be saved
  MemberList members = createMembersList();
  // extract the blobs and their lengths
  QMap<QString,QString> blobs = extractBlobsFromMembers(members);
  // Go through the members
  unsigned int n = 1;
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    writeSingleGetFromSelect(*mem, n);
    n++;
  }
  // Add object to the list of objects done
  *m_stream << getIndent()
            << "alreadyDone[obj->id()] = obj;"
            << endl;
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
        // One to One association
        *m_stream << getIndent()
                  << "// Dealing with " << as->second.first
                  << endl;
        writeSingleGetFromSelect(as->second, n, true);
        *m_stream << getIndent()
                  <<"if (0 != object->"
                  << as->second.first << "() &&" << endl
                  << getIndent()
                  << "    (0 == " << as->second.first
                  << "Id ||" << endl
                  << getIndent() << "     object->"
                  << as->second.first
                  << "()->id() != " << as->second.first
                  << "Id)) {" << endl;
        m_indent++;
        *m_stream << getIndent()
                  << "delete object->"
                  << as->second.first << "();" << endl
                  << getIndent()
                  << "object->set"
                  << capitalizeFirstLetter(as->second.first)
                  << "(0);" << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl;
        *m_stream << getIndent()
                  << "if (0 != " << as->second.first
                  << "Id) {" << endl;
        m_indent++;
        *m_stream << getIndent()
                  << "if (0 == object->"
                  << as->second.first << "()) {" << endl;
        m_indent++;
        *m_stream << getIndent() << "object->set"
                  << capitalizeFirstLetter(as->second.first)
                  << endl << getIndent()
                  << "  (dynamic_cast<"
                  << fixTypeName(as->second.second,
                                 getNamespace(as->second.second),
                                 m_classInfo->packageName)
                  << "*>" << endl << getIndent()
                  << "   (cnvSvc()->getObjFromId(" << as->second.first
                  << "Id, alreadyDone)));"
                  << endl;
        m_indent--;
        *m_stream << getIndent()
                  << "} else if (object->"
                  << as->second.first
                  << "()->id() == " << as->second.first
                  << "Id) {" << endl;
        m_indent++;
        *m_stream << getIndent()
                  << "if (alreadyDone.find(object->"
                  << as->second.first
                  << "()->id()) == alreadyDone.end()) {" << endl;
        m_indent++;
        *m_stream << getIndent()
                  << "cnvSvc()->updateObj(object->"
                  << as->second.first
                  << "(), alreadyDone);"
                  << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl;
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
      *m_stream << getIndent()
                << "// Deal with " << as->second.first
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
                << fixTypeName("set", "", "")
                << "<int> " << as->second.first
                << "List;" << endl << getIndent()
                << "m_"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << "Statement->setDouble(1, obj->id());"
                << endl << getIndent()
                << "rset = "
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
                << endl << getIndent() << "{" << endl;
      m_indent++;
      *m_stream << getIndent()
                << fixTypeName("vector", "", "")
                << "<"
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
                << "*>::iterator it = object->"
                << as->second.first
                << "().begin();" << endl << getIndent()
                << "     it != object->"
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
                << "cnvSvc()->updateObj((*it), alreadyDone);"
                << endl;
      m_indent--;
      *m_stream << getIndent() << "}" << endl;
      m_indent--;
      *m_stream << getIndent() << "}" << endl;
      *m_stream << getIndent()
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
      *m_stream << getIndent() << "object->remove"
                << capitalizeFirstLetter(as->second.first)
                << "(*it);" << endl << getIndent()
                << "delete (*it);" << endl;
      m_indent--;
      *m_stream << getIndent() << "}" << endl;
      m_indent--;
      *m_stream << getIndent() << "}" << endl
                << getIndent() << "for ("
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
                << " = cnvSvc()->getObjFromId(*it, alreadyDone);"
                << endl << getIndent()
                << "object->add"
                << capitalizeFirstLetter(as->second.first)
                << "(dynamic_cast<"
                << fixTypeName(as->second.second,
                               getNamespace(as->second.second),
                               m_classInfo->packageName)
                << "*>(item));" << endl;
      m_indent--;
      *m_stream << getIndent() << "}" << endl;
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
  *m_stream << getIndent() ;
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
  *m_stream << "rset->get";
  if (isAssoc | isEnum) {
    *m_stream << "Int";
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

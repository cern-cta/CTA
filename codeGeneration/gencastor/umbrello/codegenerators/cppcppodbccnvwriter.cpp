// Include files
#include <qmap.h>
#include <qfile.h>
#include <qptrlist.h>
#include <qtextstream.h>

// local
#include "cppcppodbccnvwriter.h"

//-----------------------------------------------------------------------------
// Implementation file for class : CppCppOdbcCnvWriter
//
// 2004-01-13 : Sebastien Ponce
//-----------------------------------------------------------------------------

//=============================================================================
// Standard constructor, initializes variables
//=============================================================================
CppCppOdbcCnvWriter::CppCppOdbcCnvWriter(UMLDoc *parent, const char *name) :
  CppCppBaseCnvWriter(parent, name) {
  setPrefix("Odbc");
}

//=============================================================================
// Initialization
//=============================================================================
bool CppCppOdbcCnvWriter::init(UMLClassifier* c, QString fileName) {
  // call upper class init
  this->CppBaseWriter::init(c, fileName);
  // fixes the namespace
  m_originalPackage = m_classInfo->fullPackageName;
  m_classInfo->packageName = "castor::db::odbc";
  m_classInfo->fullPackageName = "castor::db::odbc::";
  // includes converter  header file and object header file
  m_includes.insert(QString("\"Odbc") + m_classInfo->className + "Cnv.hpp\"");
  m_includes.insert(QString("\"") +
                    findFileName(m_class,".h") + ".hpp\"");
  // calls the postinit of this class
  postinit(c, fileName);
  return true;
}

//=============================================================================
// writeClass
//=============================================================================
void CppCppOdbcCnvWriter::writeClass(UMLClassifier */*c*/) {
  // static factory declaration
  writeFactory();
  // static constants initialization
  writeConstants();
  // creation/deletion of the database
  writeSqlStatements();
  // constructor and destructor
  writeConstructors();
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
void CppCppOdbcCnvWriter::writeConstants() {
  writeWideHeaderComment("Static constants initialization",
                         getIndent(),
                         *m_stream);
  *m_stream << getIndent()
            << "/// SQL statement for request insertion"
            << endl << getIndent()
            << "const std::string "
            << m_classInfo->fullPackageName
            << "Odbc" << m_classInfo->className
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
    *m_stream << "?";
    if (i < n - 1) *m_stream << ",";
  }
  *m_stream << ")\";" << endl << endl << getIndent()
            << "/// SQL statement for request deletion"
            << endl << getIndent()
            << "const std::string "
            << m_classInfo->fullPackageName
            << "Odbc" << m_classInfo->className
            << "Cnv::s_deleteStatementString =" << endl
            << getIndent()
            << "\"DELETE FROM rh_" << m_classInfo->className
            << " WHERE id = ?\";" << endl << endl
            << getIndent()
            << "/// SQL statement for request selection"
            << endl << getIndent()
            << "const std::string "
            << m_classInfo->fullPackageName
            << "Odbc" << m_classInfo->className
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
            << " WHERE id = ?\";" << endl << endl
            << getIndent()
            << "/// SQL statement for request update"
            << endl << getIndent()
            << "const std::string "
            << m_classInfo->fullPackageName
            << "Odbc" << m_classInfo->className
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
    *m_stream << mem->first << " = ?";
  }
  // Go through the associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE) {
      // One to One associations
      if (n > 0) *m_stream << ", ";
      n++;
      *m_stream << as->second.first << " = ?";
    }
  }
  *m_stream << " WHERE id = ?"
            << "\";" << endl << endl << getIndent()
            << "/// SQL statement for type storage"
            << endl << getIndent()
            << "const std::string "
            << m_classInfo->fullPackageName
            << "Odbc" << m_classInfo->className
            << "Cnv::s_storeTypeStatementString =" << endl
            << getIndent()
            << "\"INSERT INTO rh_Id2Type (id, type) VALUES (?, ?)\";"
            << endl << endl << getIndent()
            << "/// SQL statement for type deletion"
            << endl << getIndent()
            << "const std::string "
            << m_classInfo->fullPackageName
            << "Odbc" << m_classInfo->className
            << "Cnv::s_deleteTypeStatementString =" << endl
            << getIndent()
            << "\"DELETE FROM rh_Id2Type WHERE id = ?\";"
            << endl << endl;
  // Status dedicated statements
  if (isRequest()) {
    *m_stream << getIndent()
              << "/// SQL statement for request status insertion"
              << endl << getIndent()
              << "const std::string "
              << m_classInfo->fullPackageName
              << "Odbc" << m_classInfo->className
              << "Cnv::s_insertStatusStatementString =" << endl
              << getIndent()
              << "\"INSERT INTO rh_requestsStatus (id, status, creation, lastChange)"
              << " VALUES (?, 'NEW', SYSDATE, SYSDATE)\";"
              << endl << endl << getIndent()
              << "/// SQL statement for request status deletion"
              << endl << getIndent()
              << "const std::string "
              << m_classInfo->fullPackageName
              << "Odbc" << m_classInfo->className
              << "Cnv::s_deleteStatusStatementString =" << endl
              << getIndent()
              << "\"DELETE FROM rh_requestsStatus WHERE id = ?\";"
              << endl << endl;
  }
  // One to n associations dedicated statements
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_N) {
      *m_stream << getIndent()
                << "/// SQL insert statement for member "
                << as->second.first
                << endl << getIndent()
                << "const std::string "
                << m_classInfo->fullPackageName
                << "Odbc" << m_classInfo->className
                << "Cnv::s_insert" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "StatementString =" << endl << getIndent()
                << "\"INSERT INTO rh_"
                << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << " (Parent, Child) VALUES (?, ?)\";"
                << endl << endl << getIndent()
                << "/// SQL delete statement for member "
                << as->second.first
                << endl << getIndent()
                << "const std::string "
                << m_classInfo->fullPackageName
                << "Odbc" << m_classInfo->className
                << "Cnv::s_delete" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "StatementString =" << endl << getIndent()
                << "\"DELETE FROM rh_"
                << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << " WHERE Parent = :1\";"
                << endl << endl << getIndent()
                << "/// SQL select statement for member "
                << as->second.first
                << endl << getIndent()
                << "const std::string "
                << m_classInfo->fullPackageName
                << "Odbc" << m_classInfo->className
                << "Cnv::s_" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "StatementString =" << endl << getIndent()
                << "\"SELECT Child from rh_"
                << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << " WHERE Parent = ?\";" << endl << endl;
    }
  }
}

//=============================================================================
// writeSqlStatements
//=============================================================================
void CppCppOdbcCnvWriter::writeSqlStatements() {
  QFile file;
  openFile(file,
           "castor/db/odbc.sql",
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
      stream << as->second.first << " NUMBER";
      n++;
    }
  }
  stream << ");" << endl;
  // One to n associations dedicated statements
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_N) {
      stream << "DROP TABLE rh_"
             << m_classInfo->className
             << "2"
             << capitalizeFirstLetter(as->second.first)
             << ";" << endl
             << "CREATE TABLE rh_"
             << m_classInfo->className
             << "2"
             << capitalizeFirstLetter(as->second.first)
             << " (Parent NUMBER, Child NUMBER);"
             << endl;
    }
  }
  stream << endl;
  file.close();
}

//=============================================================================
// writeConstructors
//=============================================================================
void CppCppOdbcCnvWriter::writeConstructors() {
  // constructor
  writeWideHeaderComment("Constructor", getIndent(), *m_stream);
  *m_stream << getIndent() << m_classInfo->fullPackageName
            << "Odbc" << m_classInfo->className << "Cnv::Odbc"
            << m_classInfo->className << "Cnv("
            << fixTypeName("Services*",
                           "castor",
                           m_classInfo->packageName)
            << " svcs) :" << endl
            << getIndent() << "  OdbcBaseCnv(svcs)," << endl
            << getIndent() << "  m_insertStatement(SQL_NULL_STMT)," << endl
            << getIndent() << "  m_deleteStatement(SQL_NULL_STMT)," << endl
            << getIndent() << "  m_selectStatement(SQL_NULL_STMT)," << endl
            << getIndent() << "  m_updateStatement(SQL_NULL_STMT)," << endl;
  if (isRequest()) {
    *m_stream << getIndent() << "  m_insertStatusStatement(SQL_NULL_STMT)," << endl
              << getIndent() << "  m_deleteStatusStatement(SQL_NULL_STMT)," << endl;
  }
  *m_stream << getIndent() << "  m_storeTypeStatement(SQL_NULL_STMT)," << endl
            << getIndent() << "  m_deleteTypeStatement(SQL_NULL_STMT)";
  // One to n associations dedicated statements
  AssocList assocs = createAssocsList();
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_N) {
      *m_stream << "," << endl << getIndent()
                << "  m_insert" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "Statement(SQL_NULL_STMT)," << endl << getIndent()
                << "  m_delete" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "Statement(SQL_NULL_STMT)," << endl << getIndent()
                << "  m_" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "Statement(SQL_NULL_STMT)";
    }
  }
  *m_stream << " {}"
            << endl << endl;
  // Destructor
  writeWideHeaderComment("Destructor", getIndent(), *m_stream);
  *m_stream << getIndent() << m_classInfo->fullPackageName
            << "Odbc" << m_classInfo->className << "Cnv::~Odbc"
            << m_classInfo->className << "Cnv() throw() {" << endl;
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
  *m_stream << getIndent()
            << "deleteStatement(m_storeTypeStatement);"
            << endl << getIndent()
            << "deleteStatement(m_deleteTypeStatement);"
            << endl;
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_N) {
      *m_stream << getIndent() << "deleteStatement(m_insert"
                << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "Statement);" << endl
                << getIndent() << "deleteStatement(m_delete"
                << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "Statement);" << endl << getIndent()
                << "deleteStatement(m_"
                << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "Statement);";
    }
  }
  m_indent--;
  *m_stream << "}" << endl << endl;
}

//=============================================================================
// writeCreateRepContent
//=============================================================================
void CppCppOdbcCnvWriter::writeCreateRepContent() {
  // check whether something needs to be done
  *m_stream << getIndent()
            << "// check whether something needs to be done"
            << endl << getIndent()
            << "if (0 == obj) return;" << endl;
  // First check the statements
  *m_stream << getIndent()
            << "// Check whether the statements are ok"
            << endl << getIndent()
            << "if (SQL_NULL_STMT == m_insertStatement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_insertStatement = createStatement(getConnection(), s_insertStatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  if (isRequest()) {
    *m_stream << getIndent()
              << "if (SQL_NULL_STMT == m_insertStatusStatement) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "m_insertStatusStatement = createStatement(getConnection(), s_insertStatusStatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
  }
  *m_stream << getIndent()
            << "if (SQL_NULL_STMT == m_storeTypeStatement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_storeTypeStatement = createStatement(getConnection(), s_storeTypeStatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << getIndent()
            << "// Mark the current object as done"
            << endl << getIndent()
            << "alreadyDone.insert(obj);" << endl;
  // create a list of associations
  AssocList assocs = createAssocsList();
  // list dependant objects
  if (! assocs.isEmpty()) {
    *m_stream << getIndent()
              << "// check which objects need to be saved and keeps a list of them"
              << endl << getIndent()
              << fixTypeName("list", "", "")
              << "<" << fixTypeName("IObject*",
                                    "castor",
                                    m_classInfo->packageName)
              << "> toBeSaved;"
              << endl;
    for (Assoc* p = assocs.first();
         0 != p;
         p = assocs.next()) {
      if (!isEnum(p->second.second)) {
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
                    << "() != 0)" << endl
                    << getIndent()
                    << "  toBeSaved.push_back(obj->"
                    << p->second.first << "());" << endl;
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
                    << "if (alreadyDone.find(*it) == alreadyDone.end())"
                    << endl << getIndent()
                    << "  toBeSaved.push_back(*it);"
                    << endl;
          m_indent--;
          *m_stream << getIndent() << "}" << endl;
        }
      }
    }
  }
  // Now create ids for all objects needing one
  *m_stream << getIndent()
            << "// Set ids of all objects"
            << endl << getIndent()
            << "int nids = obj->id() == 0 ? 1 : 0;"
            << endl;
  if (! assocs.isEmpty()) {
    *m_stream << getIndent()
              << "nids += toBeSaved.size();" << endl;
  }
  fixTypeName("OdbcCnvSvc", "castor::db::odbc", m_classInfo->packageName);
  *m_stream << getIndent()
            << "unsigned long id = cnvSvc()->getIds(nids);"
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
              << endl << getIndent() << "     it++) { " << endl;
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
            << "m_storeTypeStatementParam1 = obj->id();"
            << endl << getIndent()
            << "m_storeTypeStatementParam2 = obj->type();"
            << endl << getIndent()
            << "SQLExecuteWrapper(m_storeTypeStatement, m_storeTypeStatementString);"
            << endl;
  if (isRequest()) {
    *m_stream << getIndent()
              << "m_insertStatusStatementParam1 = obj->id();"
              << endl << getIndent()
              << "SQLExecuteWrapper(m_insertStatusStatement, m_insertStatementString);"
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
            << "SQLExecuteWrapper(m_insertStatement, m_insertStatementString);"
            << endl;
  // Save dependant objects
  if (! assocs.isEmpty()) {
    *m_stream << getIndent()
              << "// Save dependant objects that need it"
              << endl << getIndent()
              << "for (" << fixTypeName("list" ,"","")
              << "<" << fixTypeName("IObject*",
                                    "castor",
                                    m_classInfo->packageName)
              << ">::iterator it = toBeSaved.begin();"
              << endl << getIndent() << "     it != toBeSaved.end();"
              << endl << getIndent() << "     it++) { " << endl;
    m_indent++;
    fixTypeName("OdbcCnvSvc", "castor::db:odbc", m_classInfo->packageName);
    *m_stream << getIndent()
              << "cnvSvc()->createRep(0, *it, alreadyDone);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
    // Save links to objects for one to n associations
    bool first = true;
    for (Assoc* p = assocs.first();
         0 != p;
         p = assocs.next()) {
      if (p->first.first == MULT_N) {
        if (first) {
          *m_stream << getIndent()
                    << "// Check whether the statement is ok"
                    << endl << getIndent()
                    << "if (SQL_NULL_STMT == m_insert"
                    << m_classInfo->className << "2"
                    << capitalizeFirstLetter(p->second.first)
                    << "Statement) {" << endl;
          m_indent++;
          *m_stream << getIndent()
                    << "m_insert"
                    << m_classInfo->className << "2"
                    << capitalizeFirstLetter(p->second.first)
                    << "Statement = createStatement(getConnection(), s_insert"
                    << m_classInfo->className << "2"
                    << capitalizeFirstLetter(p->second.first)
                    << "StatementString);"
                    << endl;
          m_indent--;
          *m_stream << getIndent() << "}" << endl
                    << getIndent()
                    << "// Save links to objects"
                    << endl << getIndent()
                    << "m_insert"
                    << m_classInfo->className << "2"
                    << capitalizeFirstLetter(p->second.first)
                    << "StatementParam1 = obj->id();"
                    << endl;
          first = false;
        }
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
                  << "m_insert"
                  << m_classInfo->className << "2"
                  << capitalizeFirstLetter(p->second.first)
                  << "StatementParam2 = (*it)->id();"
                  << endl << getIndent()
                  << "SQLExecuteWrapper(m_insert"
                  << m_classInfo->className << "2"
                  << capitalizeFirstLetter(p->second.first)
                  << "Statement, m_insert"
                  << m_classInfo->className << "2"
                  << capitalizeFirstLetter(p->second.first)
                  << "StatementString);"
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
  *m_stream << getIndent()
            << "SQLTransactWrapper(m_environment, m_connection, SQL_COMMIT);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
}

//=============================================================================
// writeUpdateRepContent
//=============================================================================
void CppCppOdbcCnvWriter::writeUpdateRepContent() {
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
  // First check the statements
  *m_stream << getIndent()
            << "// Check whether the statements are ok"
            << endl << getIndent()
            << "if (SQL_NULL_STMT == m_updateStatement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_updateStatement = createStatement(getConnection(), s_updateStatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << getIndent()
            << "// Mark the current object as done"
            << endl << getIndent()
            << "alreadyDone.insert(obj);" << endl;
  // create a list of associations
  AssocList assocs = createAssocsList();
  // list dependant objects
  if (! assocs.isEmpty()) {
    *m_stream << getIndent()
              << "// check which objects need to be updated and keeps a list of them"
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
          // One to one association
          *m_stream << getIndent() << "if (alreadyDone.find(obj->"
                    << p->second.first
                    << "()) == alreadyDone.end() &&" << endl
                    << getIndent() << "    obj->"
                    << p->second.first
                    << "() != 0)" << endl
                    << getIndent()
                    << "  toBeUpdated.push_back(obj->"
                    << p->second.first << "());" << endl;
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
                    << "if (alreadyDone.find(*it) == alreadyDone.end())"
                    << endl << getIndent()
                    << "  toBeUpdated.push_back(*it);"
                    << endl;
          m_indent--;
          *m_stream << getIndent() << "}" << endl;
        }
      }
    }
  }
  // Updates the objects in the database
  *m_stream << getIndent()
            << "// Now Update the current object"
            << endl;
  // create a list of members to be saved
  MemberList members = createMembersList();
  // extract the blobs and their lengths
  QMap<QString,QString> blobs = extractBlobsFromMembers(members);
  unsigned int n = 1;
  // Go through the members
  Member* idMem;
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
            << "SQLExecuteWrapper(m_updateStatement, m_updateStatementString);"
            << endl;
  // Update dependant objects
  if (! assocs.isEmpty()) {
    *m_stream << getIndent()
              << "// Update dependant objects that need it"
              << endl << getIndent()
              << "for (" << fixTypeName("list" ,"","")
              << "<" << fixTypeName("IObject*",
                                    "castor",
                                    m_classInfo->packageName)
              << ">::iterator it = toBeUpdated.begin();"
              << endl << getIndent() << "     it != toBeUpdated.end();"
              << endl << getIndent() << "     it++) { " << endl;
    m_indent++;
    fixTypeName("OdbcCnvSvc", "castor::db::odbc", m_classInfo->packageName);
    *m_stream << getIndent()
              << "cnvSvc()->updateRep(0, *it, alreadyDone);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
  }
  // Commit if needed
  *m_stream << getIndent()
            << "if (autocommit) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "SQLTransactWrapper(m_environment, m_connection, SQL_COMMIT);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
}

//=============================================================================
// writeDeleteRepContent
//=============================================================================
void CppCppOdbcCnvWriter::writeDeleteRepContent() {
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
            << "if (SQL_NULL_STMT == m_deleteStatement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_deleteStatement = createStatement(getConnection(), s_deleteStatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}";
  if (isRequest()) {
    *m_stream << getIndent()
              << "if (SQL_NULL_STMT == m_deleteStatusStatement) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "m_deleteStatusStatement = createStatement(getConnection(), s_deleteStatusStatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
  }
  *m_stream << getIndent()
            << "if (SQL_NULL_STMT == m_deleteTypeStatement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_deleteTypeStatement = createStatement(getConnection(), s_deleteTypeStatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << getIndent()
            << "// Mark the current object as done"
            << endl << getIndent()
            << "alreadyDone.insert(obj);" << endl;
  // create a list of associations
  AssocList assocs = createAssocsList();
  // list dependant objects
  if (! assocs.isEmpty()) {
    *m_stream << getIndent()
              << "// check which objects need to be deleted and keeps a list of them"
              << endl << getIndent()
              << fixTypeName("list", "", "")
              << "<" << fixTypeName("IObject*",
                                    "castor",
                                    m_classInfo->packageName)
              << "> toBeDeleted;"
              << endl;
    for (Assoc* p = assocs.first();
         0 != p;
         p = assocs.next()) {
      if (!isEnum(p->second.second)) {
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
                    << "() != 0)" << endl
                    << getIndent()
                    << "  toBeDeleted.push_back(obj->"
                    << p->second.first << "());" << endl;
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
                    << "if (alreadyDone.find(*it) == alreadyDone.end())"
                    << endl << getIndent()
                    << "  toBeDeleted.push_back(*it);"
                    << endl;
          m_indent--;
          *m_stream << getIndent() << "}" << endl;
        }
      }
    }
  }
  // Delete the object from the database
  *m_stream << getIndent()
            << "// Now Delete the object"
            << endl << getIndent()
            << "m_deleteTypeStatementParam1 = obj->id();"
            << endl << getIndent()
            << "SQLExecuteWrapper(m_deleteTypeStatement, m_deleteTypeStatementString);"
            << endl << getIndent()
            << "m_deleteStatementParam1 = obj->id();"
            << endl << getIndent()
            << "SQLExecuteWrapper(m_deleteStatement, m_deleteStatementString);"
            << endl;
  if (isRequest()) {
    *m_stream << getIndent()
              << "m_deleteStatusStatementParam1 = obj->id();"
              << endl << getIndent()
              << "SQLExecuteWrapper(m_deleteStatusStatement, m_deleteStatusStatementString);"
              << endl;
  }
  // Delete dependant objects
  if (! assocs.isEmpty()) {
    *m_stream << getIndent()
              << "// Delete dependant objects"
              << endl << getIndent()
              << "for (" << fixTypeName("list" ,"","")
              << "<" << fixTypeName("IObject*",
                                    "castor",
                                    m_classInfo->packageName)
              << ">::iterator it = toBeDeleted.begin();"
              << endl << getIndent() << "     it != toBeDeleted.end();"
              << endl << getIndent() << "     it++) { " << endl;
    m_indent++;
    fixTypeName("OdbcCnvSvc", "castor::db::odbc", m_classInfo->packageName);
    *m_stream << getIndent()
              << "cnvSvc()->deleteRep(0, *it, alreadyDone);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
    // Save links to objects for one to n associations
    for (Assoc* p = assocs.first();
         0 != p;
         p = assocs.next()) {
      if (p->first.first == MULT_N) {
        *m_stream << getIndent()
                  << "// Check whether the statement is ok"
                  << endl << getIndent()
                  << "if (SQL_NULL_STMT == m_delete"
                  << m_classInfo->className << "2"
                  << capitalizeFirstLetter(p->second.first)
                  << "Statement) {" << endl;
        m_indent++;
        *m_stream << getIndent()
                  << "m_delete"
                  << m_classInfo->className << "2"
                  << capitalizeFirstLetter(p->second.first)
                  << "Statement = createStatement(getConnection(), s_delete"
                  << m_classInfo->className << "2"
                  << capitalizeFirstLetter(p->second.first)
                  << "StatementString);"
                  << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl
                  << getIndent()
                  << "// Delete links to objects"
                  << endl << getIndent()
                  << "m_delete"
                  << m_classInfo->className << "2"
                  << capitalizeFirstLetter(p->second.first)
                  << "StatementParam1 = obj->id();"
                  << endl << getIndent()
                  << "SQLExecuteWrapper(m_delete"
                  << m_classInfo->className << "2"
                  << capitalizeFirstLetter(p->second.first)
                  << "Statement, m_delete"
                  << m_classInfo->className << "2"
                  << capitalizeFirstLetter(p->second.first)
                  << "StatementString);"
                  << endl;
      }
    }
  }
  // Commit if needed
  *m_stream << getIndent()
            << "if (autocommit) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "SQLTransactWrapper(m_environment, m_connection, SQL_COMMIT);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
}

//=============================================================================
// writeCreateObjContent
//=============================================================================
void CppCppOdbcCnvWriter::writeCreateObjContent() {
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
  // First check the select statement
  *m_stream << getIndent()
            << "// Check whether the statement is ok"
            << endl << getIndent()
            << "if (SQL_NULL_STMT == m_selectStatement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_selectStatement = createStatement(getConnection(), s_selectStatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent() << "if (SQL_NULL_STMT == m_selectStatement) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << "castor::Exception ex;" << endl << getIndent()
            << "ex.getMessage() << \"Unable to create statement :\""
            << " << std::endl" << endl << getIndent()
            << "                << s_selectStatementString << std::endl;"
            << endl << getIndent() << "throw ex;" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  *m_stream << getIndent() << "// retrieve the object from the database"
            << endl << getIndent()
            << "m_selectStatementParam1 = ad->id();"
            << endl << getIndent()
            << "SQLExecuteWrapper(m_selectStatement, m_selectStatementString);"
            << endl;
  *m_stream << "// Now bind the columns" << endl;
  // create a list of members to be saved
  MemberList members = createMembersList();
  // extract the blobs and their lengths
  QMap<QString,QString> blobs = extractBlobsFromMembers(members);
  // Go through the members
  unsigned int n = 1;
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    writeSingleColBind(*mem, n);
    n++;
  }
  // create a list of associations
  AssocList assocs = createAssocsList();
  // Go through the one to one associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE) {
      // do not consider enums
      if (isEnum(as->second.second)) continue;
      // One to One association
      writeSingleColBind(as->second, n, true);
      n++;
    }
  }
  *m_stream << getIndent()
            << "if (!SQLFetchWrapper(m_selectStatement, m_selectStatementString)) {"
            << endl;
  m_indent++;
  *m_stream << getIndent() << "castor::Exception ex;" << endl << getIndent()
            << "ex.getMessage() << \"No object found for id :\""
            << " << ad->id() << std::endl;" << endl
            << getIndent() << "throw ex;" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << getIndent()
            << "// create the new Object" << endl
            << getIndent() << m_originalPackage
            << m_classInfo->className << "* object = new "
            << m_originalPackage << m_classInfo->className
            << "();" << endl << getIndent();
  // Go through the members
  n = 1;
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
  // Go through the one to one associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE) {
      // do not consider enums
      if (isEnum(as->second.second)) continue;
      // One to One association
      writeSingleGetFromSelect(as->second, n, true);
      *m_stream << getIndent()
                << "IObject* obj"
                << capitalizeFirstLetter(as->second.first)
                << " = getObjFromId("
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
      n++;
    }
  }
  // Close request
  *m_stream << getIndent()
            << " SQLFreeStmt(m_selectStatement, SQL_CLOSE);"
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
                << "if (SQL_NULL_STMT == m_" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "Statement) {" << endl;
      m_indent++;
      *m_stream << getIndent()
                << "m_" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "Statement = createStatement(getConnection(), s_"
                << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "StatementString);"
                << endl << getIndent()
                << "SQLBindParameterWrapper(&"
                << "m_" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "Statement, 1, SQL_PARAM_INPUT,"
                << endl << getIndent()
                << "                        "
                << "SQL_C_ULONG, SQL_INTEGER, 0, 0,"
                << endl << getIndent()
                << "                        "
                << "&m_" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "StatementParameter, 0, 0,"
                << endl << getIndent()
                << "                        "
                << "m_set" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "StatementString);" << endl;
      m_indent--;
      *m_stream << getIndent() << "}" << endl
                << getIndent() << "SDWORD cblid;"
                << endl << getIndent() << "int id;"
                << endl << getIndent()
                << "SQLFetchWrapper(m_" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "Statement, m_" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "StatementString);"
                << endl << getIndent()
                << "SQLBindColWrapper(m_" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "Statement, 1, SQL_INTEGER,"
                << endl << getIndent()
                << "                  "
                << "&id, 0, &cblid,"
                << endl << getIndent()
                << "                  "
                << "m_" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "StatementString);"
                << endl << getIndent()
                << "while (SQLFetchWrapper(m_" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "Statement, m_" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "StatementString) {"
                << endl;
      m_indent++;
      *m_stream << getIndent()
                << "IObject* obj"
                << " = getObjFromId(id, newlyCreated);"
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
                << "m_" << m_classInfo->className
                << "2" << capitalizeFirstLetter(as->second.first)
                << "Statement->closeResultSet(rset);" << endl;
    }
  }
  // Return result
  *m_stream << getIndent() << "return object;" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
}

//=============================================================================
// writeUpdateObjContent
//=============================================================================
void CppCppOdbcCnvWriter::writeUpdateObjContent() {
}

//=============================================================================
// writeSingleSetIntoStatement
//=============================================================================
void
CppCppOdbcCnvWriter::writeSingleSetIntoStatement(QString statement,
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
            << "m_" << statement << "StatementParam"
            << n << " = ";
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
  *m_stream << ";" << endl;
}

//=============================================================================
// writeSingleGetFromSelect
//=============================================================================
void CppCppOdbcCnvWriter::writeSingleGetFromSelect(Member mem,
                                                   int n,
                                                   bool isAssoc) {
  *m_stream << getIndent() ;
  // deal with arrays of chars
  bool isArray = mem.second.find('[') > 0;
  if (isAssoc) {
    *m_stream << "unsigned long " << mem.first
              << "Id = ";
  } else {
    *m_stream << "object->set"
              << capitalizeFirstLetter(mem.first)
              << "(";
  }
  if (isArray) {
    *m_stream << "("
              << mem.second.left(mem.second.find('['))
              << "*)";
  }
  *m_stream << "rset->get";
  if (isAssoc) {
    *m_stream << "Int";
  } else {
    *m_stream << getOdbcType(mem.second);
  }
  *m_stream << "(" << n << ")";
  if (isArray) *m_stream << ".data()";
  if (!isAssoc) *m_stream << ")";
  *m_stream << ";" << endl;
}

//=============================================================================
// writeSingleColBind
//=============================================================================
void CppCppOdbcCnvWriter::writeSingleColBind(Member mem,
                                             int n,
                                             bool isAssoc) {
  *m_stream << getIndent() << "BindCol(..)" << endl;
}

//=============================================================================
// getOdbcType
//=============================================================================
QString CppCppOdbcCnvWriter::getOdbcType(QString& type) {
  QString odbcType = getSimpleType(type);
  if (odbcType == "short" ||
      odbcType == "long" ||
      odbcType == "bool" ||
      odbcType == "int" ||
      odbcType == "u_signed64")
    odbcType = "int";
  if (odbcType.startsWith("char")) odbcType = "string";
  odbcType = capitalizeFirstLetter(odbcType);
  return odbcType;
}

//=============================================================================
// getSQLType
//=============================================================================
QString CppCppOdbcCnvWriter::getSQLType(QString& type) {
  QString SQLType = getSimpleType(type);
  SQLType = SQLType;
  if (SQLType == "short" ||
      SQLType == "long" ||
      SQLType == "bool" ||
      SQLType == "int") {
    SQLType = "NUMBER";
  } else if (SQLType == "string") {
    SQLType = "VARCHAR(255)";
  } else if (SQLType.left(5) == "char["){
    QString res = "CHAR[";
    res.append(SQLType.mid(5, SQLType.find("]")-5));
    SQLType = res;
  } else if (m_castorTypes.find(SQLType) != m_castorTypes.end()) {
    SQLType = "NUMBER";
  }
  return SQLType;
}

//=============================================================================
// isRequest
//=============================================================================
bool CppCppOdbcCnvWriter::isRequest() {
  UMLObject* obj = m_doc->findUMLObject(QString("Request"),
                                        Uml::ot_Class);
  const UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
  return m_classInfo->allSuperclasses.contains(concept);
}



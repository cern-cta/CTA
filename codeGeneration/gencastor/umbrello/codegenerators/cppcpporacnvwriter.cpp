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
// Standard destructor
//=============================================================================
CppCppOraCnvWriter::~CppCppOraCnvWriter() {
  if (!m_firstClass) {
    endSQLFile();
  }
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
  insertFileintoStream(stream, "castor/db/oracleHeader.sql");
  file.close();
}

//=============================================================================
// insertIntoSQLFile
//=============================================================================
void CppCppOraCnvWriter::insertFileintoStream(QTextStream &stream,
                                              QString fileName) {
  QFile file;
  openFile(file, fileName, IO_ReadOnly);
  QTextStream source(&file);
  QString line = source.readLine();
  while (! line.isNull()) {
    stream << line << endl;
    line = source.readLine();
  }
  file.close();
}

//=============================================================================
// endSQLFile
//=============================================================================
void CppCppOraCnvWriter::endSQLFile() {
  // Preparing SQL file for creation/deletion of the database
  QFile file;
  openFile(file,
           "castor/db/oracle.sql",
           IO_WriteOnly | IO_Append);
  QTextStream stream(&file);
  insertFileintoStream(stream, "castor/db/oracleTrailer.sql");
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
void CppCppOraCnvWriter::ordonnateMembersInAssoc(Assoc* as,
                                                 Member** firstMember,
                                                 Member** secondMember) {
  // N to N association
  // Here we will use a dedicated table for the association
  // Find out the parent and child in this table
  if (as->type.kind == COMPOS_PARENT ||
      as->type.kind == AGGREG_PARENT) {
    *firstMember = &(as->localPart);
    *secondMember = &(as->remotePart);
  } else if (as->type.kind == COMPOS_CHILD ||
             as->type.kind == AGGREG_CHILD) {
    *firstMember = &(as->remotePart);
    *secondMember = &(as->localPart);
  } else {
    // Case of a symetrical association
    if (as->remotePart.name < as->localPart.name) {
      *firstMember = &(as->remotePart);
      *secondMember = &(as->localPart);
    } else {
      *firstMember = &(as->localPart);
      *secondMember = &(as->remotePart);
    }
  }
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
            << "\"INSERT INTO " << m_classInfo->className
            << " (";
  bool first = true;
  // create a list of members
  MemberList members = createMembersList();
  // Go through the members
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    if (!first) *m_stream << ", ";
    *m_stream << mem->name;
    first = false;
  }
  // create a list of associations
  AssocList assocs = createAssocsList();
  // Go through the associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->type.multiRemote == MULT_ONE &&
        as->remotePart.name != "") {
      // One to One associations
      if (!first) *m_stream << ", ";
      first = false;
      *m_stream << as->remotePart.name;
    }
  }
  *m_stream << ") VALUES (";
  int n = 1;
  first = true;
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    if (!first) *m_stream << ",";
    first = false;
    if (mem->name == "id") {
      *m_stream << "ids_seq.nextval";
    } else {
      *m_stream << ":" << n;
      n++;
    }
  }
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->type.multiRemote == MULT_ONE &&
        as->remotePart.name != "") {
      if (!first) *m_stream << ",";
      first = false;
      *m_stream << ":" << n;
      n++;
    }
  }
  *m_stream << ") RETURNING id INTO :" << n
            << "\";" << endl << endl << getIndent()
            << "/// SQL statement for request deletion"
            << endl << getIndent()
            << "const std::string "
            << m_classInfo->fullPackageName
            << "Ora" << m_classInfo->className
            << "Cnv::s_deleteStatementString =" << endl
            << getIndent()
            << "\"DELETE FROM " << m_classInfo->className
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
    *m_stream << mem->name;
    n++;
  }
  // Go through the associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->type.multiRemote == MULT_ONE &&
        as->remotePart.name != "") {
      if (n > 0) *m_stream << ", ";
      *m_stream << as->remotePart.name;
      n++;
    }
  }
  *m_stream << " FROM " << m_classInfo->className
            << " WHERE id = :1\";" << endl << endl
            << getIndent()
            << "/// SQL statement for request update"
            << endl << getIndent()
            << "const std::string "
            << m_classInfo->fullPackageName
            << "Ora" << m_classInfo->className
            << "Cnv::s_updateStatementString =" << endl
            << getIndent()
            << "\"UPDATE " << m_classInfo->className
            << " SET ";
  n = 0;
  // Go through the members
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    if (mem->name == "id") continue;
    if (n > 0) *m_stream << ", ";
    n++;
    *m_stream << mem->name << " = :" << n;
  }
  // Go through dependant objects
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (isEnum(as->remotePart.typeName)) {
      if (n > 0) *m_stream << ", ";
      n++;
      *m_stream << as->remotePart.name << " = :" << n;
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
            << "\"INSERT INTO Id2Type (id, type) VALUES (:1, :2)\";"
            << endl << endl << getIndent()
            << "/// SQL statement for type deletion"
            << endl << getIndent()
            << "const std::string "
            << m_classInfo->fullPackageName
            << "Ora" << m_classInfo->className
            << "Cnv::s_deleteTypeStatementString =" << endl
            << getIndent()
            << "\"DELETE FROM Id2Type WHERE id = :1\";"
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
              << "\"INSERT INTO requestsStatus (id, status, creation, lastChange)"
              << " VALUES (:1, 'NEW', SYSDATE, SYSDATE)\";"
              << endl << endl << getIndent()
              << "/// SQL statement for request status deletion"
              << endl << getIndent()
              << "const std::string "
              << m_classInfo->fullPackageName
              << "Ora" << m_classInfo->className
              << "Cnv::s_deleteStatusStatementString =" << endl
              << getIndent()
              << "\"DELETE FROM requestsStatus WHERE id = :1\";"
              << endl << endl;
  }
  // Associations dedicated statements
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->remotePart.name == "" ||
        isEnum(as->remotePart.typeName)) continue;
    if (as->type.multiRemote == MULT_N &&
        as->type.multiLocal == MULT_N) {
      // N to N association
      // Here we will use a dedicated table for the association
      // Find out the parent and child in this table
      Member* firstMember = 0;
      Member* secondMember = 0;
      ordonnateMembersInAssoc(as, &firstMember, &secondMember);
      QString firstRole, secondRole;
      if (firstMember == &as->localPart) {
        firstRole = "Parent";
        secondRole = "Child";
      } else {
        firstRole = "Child";
        secondRole = "Parent";        
      }
      *m_stream << getIndent()
                << "/// SQL insert statement for member "
                << as->remotePart.name
                << endl << getIndent()
                << "const std::string "
                << m_classInfo->fullPackageName
                << "Ora" << m_classInfo->className
                << "Cnv::s_insert"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "StatementString =" << endl << getIndent()
                << "\"INSERT INTO "
                << capitalizeFirstLetter(firstMember->typeName)
                << "2"
                << capitalizeFirstLetter(secondMember->typeName)
                << " (" << firstRole << ", " << secondRole
                << ") VALUES (:1, :2)\";"
                << endl << endl << getIndent()
                << "/// SQL delete statement for member "
                << as->remotePart.name
                << endl << getIndent()
                << "const std::string "
                << m_classInfo->fullPackageName
                << "Ora" << m_classInfo->className
                << "Cnv::s_delete"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "StatementString =" << endl << getIndent()
                << "\"DELETE FROM "
                << capitalizeFirstLetter(firstMember->typeName)
                << "2"
                << capitalizeFirstLetter(secondMember->typeName)
                << " WHERE " << firstRole << " = :1 AND "
                << secondRole << " = :2\";"
                << endl << endl << getIndent()
                << "/// SQL select statement for member "
                << as->remotePart.name
                << endl << getIndent()
                << "const std::string "
                << m_classInfo->fullPackageName
                << "Ora" << m_classInfo->className
                << "Cnv::s_select"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "StatementString =" << endl << getIndent()
                << "\"SELECT " << secondRole << " from "
                << capitalizeFirstLetter(firstMember->typeName)
                << "2"
                << capitalizeFirstLetter(secondMember->typeName)
                << " WHERE " << firstRole << " = :1 FOR UPDATE\";"
                << endl << endl;
    } else {
      if (as->type.multiLocal == MULT_ONE &&
          as->type.multiRemote != MULT_UNKNOWN &&
          !as->remotePart.abstract) {
        // 1 to * association
        *m_stream << getIndent()
                  << "/// SQL select statement for member "
                  << as->remotePart.name
                  << endl << getIndent()
                  << "const std::string "
                  << m_classInfo->fullPackageName
                  << "Ora" << m_classInfo->className
                  << "Cnv::s_select"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "StatementString =" << endl
                  << getIndent()
                  << "\"SELECT id from "
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << " WHERE " << as->localPart.name
                  << " = :1 FOR UPDATE\";" << endl << endl
                  << getIndent()
                  << "/// SQL delete statement for member "
                  << as->remotePart.name
                  << endl << getIndent()
                  << "const std::string "
                  << m_classInfo->fullPackageName
                  << "Ora" << m_classInfo->className
                  << "Cnv::s_delete"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "StatementString =" << endl << getIndent()
                  << "\"UPDATE "
                  << as->remotePart.typeName
                  << " SET " << as->localPart.name
                  << " = 0 WHERE id = :1\";" << endl << endl
                  << getIndent()
                  << "/// SQL remote update statement for member "
                  << as->remotePart.name
                  << endl << getIndent()
                  << "const std::string "
                  << m_classInfo->fullPackageName
                  << "Ora" << m_classInfo->className
                  << "Cnv::s_remoteUpdate"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "StatementString =" << endl << getIndent()
                  << "\"UPDATE "
                  << as->remotePart.typeName
                  << " SET " << as->localPart.name
                  << " = :1 WHERE id = :2\";" << endl << endl;
      }
      if (as->type.multiRemote == MULT_ONE) {
        // * to 1
        if (!as->remotePart.abstract) {
          *m_stream << getIndent()
                    << "/// SQL existence statement for member "
                    << as->remotePart.name
                    << endl << getIndent()
                    << "const std::string "
                    << m_classInfo->fullPackageName
                    << "Ora" << m_classInfo->className
                    << "Cnv::s_check"
                    << capitalizeFirstLetter(as->remotePart.typeName)
                    << "ExistStatementString =" << endl
                    << getIndent()
                    << "\"SELECT id from "
                    << capitalizeFirstLetter(as->remotePart.typeName)
                    << " WHERE id = :1\";" << endl << endl;
        }
        *m_stream << getIndent()
                  << "/// SQL update statement for member "
                  << as->remotePart.name
                  << endl << getIndent()
                  << "const std::string "
                  << m_classInfo->fullPackageName
                  << "Ora" << m_classInfo->className
                  << "Cnv::s_update"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "StatementString =" << endl
                  << getIndent()
                  << "\"UPDATE "
                  << m_classInfo->className
                  << " SET " << as->remotePart.name
                  << " = : 1 WHERE id = :2\";" << endl << endl;
      }
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
         << "DROP TABLE "
         << m_classInfo->className
         << ";" << endl
         << "CREATE TABLE "
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
    stream << mem->name << " "
           << getSQLType(mem->typeName);
    if (mem->name == "id") stream << " PRIMARY KEY";
    n++;
  }
  // create a list of associations
  AssocList assocs = createAssocsList();
  // Go through the associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->type.multiRemote == MULT_ONE &&
        as->remotePart.name != "") {
      // One to One associations
      if (n > 0) stream << ", ";
      stream << as->remotePart.name << " INTEGER";
      n++;
    }
  }
  stream << ");" << endl;
  // Associations dedicated statements
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->remotePart.name == "" ||
        isEnum(as->remotePart.typeName)) continue;
    if (as->type.multiRemote == MULT_N &&
        as->type.multiLocal == MULT_N) {
      // N to N association
      // Here we will use a dedicated table for the association
      // Find out the parent and child in this table
      Member* firstMember = 0;
      Member* secondMember = 0;
      ordonnateMembersInAssoc(as, &firstMember, &secondMember);
      if (firstMember == &as->localPart) {
        stream << getIndent()
               << "DROP INDEX I_"
               << capitalizeFirstLetter(firstMember->typeName)
               << "2"
               << capitalizeFirstLetter(secondMember->typeName)
               << "_Child;"
               << endl << getIndent()
               << "DROP INDEX I_"
               << capitalizeFirstLetter(firstMember->typeName)
               << "2"
               << capitalizeFirstLetter(secondMember->typeName)
               << "_Parent;"
               << endl << "DROP TABLE "
               << capitalizeFirstLetter(firstMember->typeName)
               << "2"
               << capitalizeFirstLetter(secondMember->typeName)
               << ";" << endl
               << "CREATE TABLE "
               << capitalizeFirstLetter(firstMember->typeName)
               << "2"
               << capitalizeFirstLetter(secondMember->typeName)
               << " (Parent INTEGER, Child INTEGER);"
               << endl << getIndent()
               << "CREATE INDEX I_"
               << capitalizeFirstLetter(firstMember->typeName)
               << "2"
               << capitalizeFirstLetter(secondMember->typeName)
               << "_Child on "
               << capitalizeFirstLetter(firstMember->typeName)
               << "2"
               << capitalizeFirstLetter(secondMember->typeName)
               << " (child);"
               << endl << getIndent()
               << "CREATE INDEX I_"
               << capitalizeFirstLetter(firstMember->typeName)
               << "2"
               << capitalizeFirstLetter(secondMember->typeName)
               << "_Parent on "
               << capitalizeFirstLetter(firstMember->typeName)
               << "2"
               << capitalizeFirstLetter(secondMember->typeName)
               << " (parent);"
               << endl;
      }
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
            << m_classInfo->className << "Cnv("
            << fixTypeName("ICnvSvc*",
                           "castor",
                           m_classInfo->packageName)
            << " cnvSvc) :" << endl
            << getIndent() << "  OraBaseCnv(cnvSvc)," << endl
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
    if (as->remotePart.name == "" ||
        isEnum(as->remotePart.typeName)) continue;
    if (as->type.multiRemote == MULT_N &&
        as->type.multiLocal == MULT_N) {
      // N to N association
      // Here we will use a dedicated table for the association
      // Find out the parent and child in this table
      *m_stream << "," << endl << getIndent()
                << "  m_insert"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "Statement(0)," << endl << getIndent()
                << "  m_delete"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "Statement(0)," << endl << getIndent()
                << "  m_select"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "Statement(0)";
    } else {
      if (as->type.multiLocal == MULT_ONE &&
          as->type.multiRemote != MULT_UNKNOWN &&
          !as->remotePart.abstract) {
        // 1 to * association
        *m_stream << "," << endl << getIndent()
                  << "  m_select"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement(0)" << "," << endl
                  << getIndent() << "  m_delete"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement(0)," << endl << getIndent()
                  << "  m_remoteUpdate"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement(0)";
      }
      if (as->type.multiRemote == MULT_ONE) {
        // * to 1
        if (!as->remotePart.abstract) {
        *m_stream << "," << endl << getIndent()
                  << "  m_check"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "ExistStatement(0)";
        }
        *m_stream << "," << endl
                  << getIndent() << "  m_update"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement(0)";
      }
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
    if (as->remotePart.name == "" ||
        isEnum(as->remotePart.typeName)) continue;
    if (as->type.multiRemote == MULT_N &&
        as->type.multiLocal == MULT_N) {
      // N to N association
      // Here we will use a dedicated table for the association
      // Find out the parent and child in this table
      *m_stream << getIndent()
                << "deleteStatement(m_insert"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "Statement);" << endl << getIndent()
                << "deleteStatement(m_delete"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "Statement);" << endl << getIndent()
                << "deleteStatement(m_select"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "Statement);" << endl;
    } else {
      if (as->type.multiLocal == MULT_ONE &&
          as->type.multiRemote != MULT_UNKNOWN &&
          !as->remotePart.abstract) {
        // 1 to * association
        *m_stream << getIndent()
                  << "deleteStatement(m_delete"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement);" << endl << getIndent()
                  << "deleteStatement(m_select"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement);" << endl
                  << getIndent()
                  << "deleteStatement(m_remoteUpdate"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement);" << endl;
      }
      if (as->type.multiRemote == MULT_ONE) {
        // * to 1
        if (!as->remotePart.abstract) {
          *m_stream << getIndent()
                    << "deleteStatement(m_check"
                    << capitalizeFirstLetter(as->remotePart.typeName)
                    << "ExistStatement);" << endl;
        }
        *m_stream << getIndent()
                  << "deleteStatement(m_update"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement);" << endl;
      }
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
    if (as->remotePart.name == "" ||
        isEnum(as->remotePart.typeName)) continue;
    if (as->type.multiRemote == MULT_N &&
        as->type.multiLocal == MULT_N) {
      // N to N association
      // Here we will use a dedicated table for the association
      // Find out the parent and child in this table
      *m_stream << getIndent()
                << "m_insert"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "Statement = 0;" << endl << getIndent()
                << "m_delete"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "Statement = 0;" << endl << getIndent()
                << "m_select"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "Statement = 0;" << endl;
    } else {
      if (as->type.multiLocal == MULT_ONE &&
          as->type.multiRemote != MULT_UNKNOWN &&
          !as->remotePart.abstract) {
        // 1 to * association
        *m_stream << getIndent()
                  << "m_select" << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement = 0;" << endl << getIndent()
                  << "m_delete" << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement = 0;" << endl << getIndent()
                  << "m_remoteUpdate" << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement = 0;" << endl;
      }
      if (as->type.multiRemote == MULT_ONE) {
        // * to 1
        if (!as->remotePart.abstract) {
          *m_stream << getIndent()
                    << "m_check" << capitalizeFirstLetter(as->remotePart.typeName)
                    << "ExistStatement = 0;" << endl;
        }
        *m_stream << getIndent()
                  << "m_update" << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement = 0;" << endl;
      }
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
  *m_stream << getIndent() << "try {" << endl;
  m_indent++;
  *m_stream << getIndent() << "switch (type) {" << endl;
  AssocList assocs = createAssocsList();
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->remotePart.name != "" &&
        !isEnum(as->remotePart.typeName)) {
      if (as->type.multiRemote == MULT_ONE ||
          as->type.multiRemote == MULT_N) {
        addInclude("\"castor/Constants.hpp\"");
        *m_stream << getIndent() << "case castor::OBJ_"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << " :" << endl;
        m_indent++;
        *m_stream << getIndent() << "fillRep"
                  << capitalizeFirstLetter(as->remotePart.typeName)
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
            << "ex.getMessage() << \"fillRep called for type \" << type "
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
  *m_stream << getIndent()
            << "} catch (oracle::occi::SQLException e) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << fixTypeName("Internal",
                           "castor.exception",
                           m_classInfo->packageName)
            << " ex; // XXX Fix it, depending on ORACLE error"
            << endl << getIndent()
            << "ex.getMessage() << \"Error in fillRep for type \" << type"
            << endl << getIndent()
            << "                << std::endl << e.what() << std::endl;"
            << endl << getIndent() << "throw ex;" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << endl;

  // Now write the dedicated fillRep Methods
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->remotePart.name != "" &&
        !isEnum(as->remotePart.typeName)) {
      if (as->type.multiRemote == MULT_ONE) {
        writeBasicMult1FillRep(as);
      } else if  (as->type.multiRemote == MULT_N) {
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
    if (as->remotePart.name != "" &&
        !isEnum(as->remotePart.typeName)) {
      if (as->type.multiRemote == MULT_ONE ||
          as->type.multiRemote == MULT_N) {
        addInclude("\"castor/Constants.hpp\"");
        *m_stream << getIndent() << "case castor::OBJ_"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << " :" << endl;
        m_indent++;
        *m_stream << getIndent() << "fillObj"
                  << capitalizeFirstLetter(as->remotePart.typeName)
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
  unsigned int n = members.count();
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->remotePart.name != "" &&
        !isEnum(as->remotePart.typeName)) {
      if (as->type.multiRemote == MULT_ONE) {
        n++;
        writeBasicMult1FillObj(as, n);
      } else if  (as->type.multiRemote == MULT_N) {
        writeBasicMultNFillObj(as);
      }
    } else if (isEnum(as->remotePart.typeName)) {
      n++;
    }
  }
}

//=============================================================================
// writeBasicMult1FillRep
//=============================================================================
void CppCppOraCnvWriter::writeBasicMult1FillRep(Assoc* as) {
  writeWideHeaderComment("fillRep" +
                         capitalizeFirstLetter(as->remotePart.typeName),
                         getIndent(), *m_stream);
  *m_stream << getIndent() << "void "
            << m_classInfo->packageName << "::Ora"
            << m_classInfo->className << "Cnv::fillRep"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "("
            << fixTypeName(m_classInfo->className + "*",
                           getNamespace(m_classInfo->className),
                           "")
            << " obj)" << endl << getIndent()
            << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           "")
            << ", oracle::occi::SQLException) {"
            << endl;
  m_indent++;
  if (as->type.multiLocal == MULT_ONE &&
      !as->remotePart.abstract) {
    // 1 to 1, wee need to check whether the old remote object
    // should be updated
    *m_stream << getIndent() << "// Check select"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << " statement" << endl << getIndent()
              << "if (0 == m_select"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement) {" << endl;
    m_indent++;
    *m_stream << getIndent() << "m_select"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement = createStatement(s_select"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "StatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl << getIndent()
              << "// retrieve the object from the database"
              << endl << getIndent() << "m_select"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement->setDouble(1, obj->id());"
              << endl << getIndent()
              << "oracle::occi::ResultSet *rset = m_select"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement->executeQuery();"
              << endl;
    *m_stream << getIndent()
              << "if (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {"
              << endl;
    m_indent++;
    writeSingleGetFromSelect(as->remotePart, 1, true);
    *m_stream << getIndent()
              << "if (0 != " << as->remotePart.name
              << "Id &&" << endl
              << getIndent() << "    (0 == obj->"
              << as->remotePart.name << "() ||" << endl
              << getIndent() << "     obj->"
              << as->remotePart.name << "()->id() != "
              << as->remotePart.name << "Id)) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "if (0 == m_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "m_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement = createStatement(s_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "StatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl << getIndent()
              << "m_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement->setDouble(1, "
              << as->remotePart.name
              << "Id);"
              << endl << getIndent() << "m_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement->executeUpdate();"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
    // Close request
    *m_stream << getIndent() << "// Close resultset" << endl
              << getIndent()
              << "m_select"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement->closeResultSet(rset);"
              << endl;
  }
  if (!as->remotePart.abstract) {
    // Common part to all * to 1 associations, except if remote
    // end is abstract :
    // See whether the remote end exists and create it if not
    *m_stream << getIndent() << "if (0 != obj->"
              << as->remotePart.name << "()) {" << endl;
    m_indent++;
    *m_stream << getIndent() << "// Check check"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Exist statement" << endl << getIndent()
              << "if (0 == m_check"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "ExistStatement) {" << endl;
    m_indent++;
    *m_stream << getIndent() << "m_check"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "ExistStatement = createStatement(s_check"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "ExistStatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl << getIndent()
              << "// retrieve the object from the database"
              << endl << getIndent() << "m_check"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "ExistStatement->setDouble(1, obj->"
              << as->remotePart.name << "()->id());"
              << endl << getIndent()
              << "oracle::occi::ResultSet *rset = m_check"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "ExistStatement->executeQuery();"
              << endl;
    *m_stream << getIndent()
              << "if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {"
              << endl;
    m_indent++;
    addInclude("\"castor/Constants.hpp\"");
    *m_stream << getIndent()
              << fixTypeName("BaseAddress",
                             "castor",
                             "")
              << " ad;" << endl << getIndent()
              << "ad.setCnvSvcName(\"OraCnvSvc\");"
              << endl << getIndent()
              << "ad.setCnvSvcType(castor::SVC_ORACNV);"
              << endl << getIndent()
              << "cnvSvc()->createRep(&ad, obj->"
              << as->remotePart.name
              << "(), false";
    if (as->type.multiLocal == MULT_ONE) {
      *m_stream << ", OBJ_" << as->localPart.typeName;
    }
    *m_stream << ");" << endl;
    m_indent--;
    if (as->type.multiLocal == MULT_ONE) {
      *m_stream << getIndent() << "} else {" << endl;
      m_indent++;
      *m_stream << getIndent() << "// Check remote update statement"
                << endl << getIndent()
                << "if (0 == m_remoteUpdate" << as->remotePart.typeName
                << "Statement) {" << endl;
      m_indent++;
      *m_stream << getIndent()
                << "m_remoteUpdate" << as->remotePart.typeName
                << "Statement = createStatement(s_remoteUpdate"
                << as->remotePart.typeName
                << "StatementString);"
                << endl;
      m_indent--;
      *m_stream << getIndent() << "}" << endl << getIndent()
                << "// Update remote object"
                << endl << getIndent()
                << "m_remoteUpdate" << as->remotePart.typeName
                << "Statement->setDouble(1, obj->id());"
                << endl << getIndent()
                << "m_remoteUpdate" << as->remotePart.typeName
                << "Statement->setDouble(2, obj->"
                << as->remotePart.name
                << "()->id());"
                << endl << getIndent()
                << "m_remoteUpdate" << as->remotePart.typeName
                << "Statement->executeUpdate();"
                << endl;
      m_indent--;
    }
    *m_stream << getIndent() << "}" << endl
              << getIndent() << "// Close resultset" << endl
              << getIndent()
              << "m_check"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "ExistStatement->closeResultSet(rset);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
  }
  // Last bit, common to all * to 1 associations :
  // update the local object
  *m_stream << getIndent() << "// Check update statement"
            << endl << getIndent()
            << "if (0 == m_update" << as->remotePart.typeName
            << "Statement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_update" << as->remotePart.typeName
            << "Statement = createStatement(s_update"
            << as->remotePart.typeName
            << "StatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << getIndent()
            << "// Update local object"
            << endl << getIndent()
            << "m_update" << as->remotePart.typeName
            << "Statement->setDouble(1, 0 == obj->"
            << as->remotePart.name
            << "() ? 0 : obj->"
            << as->remotePart.name
            << "()->id());"
            << endl << getIndent()
            << "m_update" << as->remotePart.typeName
            << "Statement->setDouble(2, obj->id());"
            << endl << getIndent()
            << "m_update" << as->remotePart.typeName
            << "Statement->executeUpdate();"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << endl;
}

//=============================================================================
// writeBasicMult1FillObj
//=============================================================================
void CppCppOraCnvWriter::writeBasicMult1FillObj(Assoc* as,
                                                unsigned int n) {
  writeWideHeaderComment("fillObj" +
                         capitalizeFirstLetter(as->remotePart.typeName),
                         getIndent(), *m_stream);
  *m_stream << getIndent() << "void "
            << m_classInfo->packageName << "::Ora"
            << m_classInfo->className << "Cnv::fillObj"
            << capitalizeFirstLetter(as->remotePart.typeName)
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
  writeSingleGetFromSelect(as->remotePart, n, true);
  *m_stream << getIndent() << "// Close ResultSet"
            << endl << getIndent()
            << "m_selectStatement->closeResultSet(rset);"
            << endl;
  *m_stream << getIndent()
            << "// Check whether something should be deleted"
            << endl << getIndent() <<"if (0 != obj->"
            << as->remotePart.name << "() &&" << endl
            << getIndent()
            << "    (0 == " << as->remotePart.name
            << "Id ||" << endl
            << getIndent() << "     obj->"
            << as->remotePart.name
            << "()->id() != " << as->remotePart.name
            << "Id)) {" << endl;
  m_indent++;
  if (as->localPart.name != "") {
    *m_stream << getIndent()
              << "obj->"
              << as->remotePart.name << "()->";
    if (as->type.multiLocal == MULT_ONE) {
      *m_stream << "set"
                << capitalizeFirstLetter(as->localPart.name)
                << "(0)";
    } else {
      *m_stream << "remove"
                << capitalizeFirstLetter(as->localPart.name)
                << "(obj)";
    }
    *m_stream << ";" << endl;
  }
  *m_stream << getIndent()
            << "obj->set"
            << capitalizeFirstLetter(as->remotePart.name)
            << "(0);" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  *m_stream << getIndent()
            << "// Update object or create new one"
            << endl << getIndent()
            << "if (0 != " << as->remotePart.name
            << "Id) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "if (0 == obj->"
            << as->remotePart.name << "()) {" << endl;
  m_indent++;
  *m_stream << getIndent() << "obj->set"
            << capitalizeFirstLetter(as->remotePart.name)
            << endl << getIndent()
            << "  (dynamic_cast<"
            << fixTypeName(as->remotePart.typeName,
                           getNamespace(as->remotePart.typeName),
                           m_classInfo->packageName)
            << "*>" << endl << getIndent()
            << "   (cnvSvc()->getObjFromId(" << as->remotePart.name
            << "Id)));"
            << endl;
  m_indent--;
  *m_stream << getIndent()
            << "} else {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "cnvSvc()->updateObj(obj->"
            << as->remotePart.name
            << "());"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  if (as->localPart.name != "") {
    // Update back link
    *m_stream << getIndent() << "obj->"
              << as->remotePart.name << "()->";
    if (as->type.multiLocal == MULT_ONE) {
      *m_stream << "set";
    } else {
      *m_stream << "add";
    }
    *m_stream << capitalizeFirstLetter(as->localPart.name)
              << "(obj);" << endl;
  }
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
                         capitalizeFirstLetter(as->remotePart.typeName),
                         getIndent(), *m_stream);
  *m_stream << getIndent()
            << "void " << m_classInfo->packageName
            << "::Ora" << m_classInfo->className
            << "Cnv::fillRep"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "("
            << fixTypeName(m_classInfo->className + "*",
                           getNamespace(m_classInfo->className),
                           "")
            << " obj)"
            << endl << getIndent() << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           "")
            << ", oracle::occi::SQLException) {"
            << endl;
  m_indent++;
  *m_stream << getIndent() << "// check select statement"
            << endl << getIndent()
            << "if (0 == m_select"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "Statement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_select"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "Statement = createStatement(s_select"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "StatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent() << "// Get current database data"
            << endl << getIndent()
            << fixTypeName("set", "", "")
            << "<int> " << as->remotePart.name
            << "List;" << endl << getIndent()
            << "m_select"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "Statement->setDouble(1, obj->id());"
            << endl << getIndent()
            << "oracle::occi::ResultSet *rset = "
            << "m_select"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "Statement->executeQuery();"
            << endl << getIndent()
            << "while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << as->remotePart.name
            << "List.insert(rset->getInt(1));"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent()
            << "m_select"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "Statement->closeResultSet(rset);"
            << endl << getIndent()
            << "// update " << as->remotePart.name
            << " and create new ones"
            << endl << getIndent() << "for ("
            << fixTypeName("vector", "", "")
            << "<"
            << fixTypeName(as->remotePart.typeName,
                           getNamespace(as->remotePart.typeName),
                           m_classInfo->packageName)
            << "*>::iterator it = obj->"
            << as->remotePart.name
            << "().begin();" << endl << getIndent()
            << "     it != obj->"
            << as->remotePart.name
            << "().end();" << endl << getIndent()
            << "     it++) {"  << endl;
  m_indent++;
  *m_stream << getIndent() << "if (0 == (*it)->id()) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << "cnvSvc()->createRep(0, *it, false";
  if (as->type.multiLocal == MULT_ONE) {
    *m_stream << ", OBJ_" << as->localPart.typeName;
  }
  *m_stream << ");" << endl;
  if (as->type.multiLocal == MULT_ONE &&
      !as->remotePart.abstract) {
    m_indent--;
    *m_stream << getIndent() << "} else {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "// Check remote update statement"
              << endl << getIndent()
              << "if (0 == m_remoteUpdate" << as->remotePart.typeName
              << "Statement) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "m_remoteUpdate" << as->remotePart.typeName
              << "Statement = createStatement(s_remoteUpdate"
              << as->remotePart.typeName
              << "StatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl << getIndent()
              << "// Update remote object"
              << endl << getIndent()
              << "m_remoteUpdate" << as->remotePart.typeName
              << "Statement->setDouble(1, obj->id());"
              << endl << getIndent()
              << "m_remoteUpdate" << as->remotePart.typeName
              << "Statement->setDouble(2, (*it)->id());"
              << endl << getIndent()
              << "m_remoteUpdate" << as->remotePart.typeName
              << "Statement->executeUpdate();"
              << endl;
  }
  if (as->type.multiLocal == MULT_N) {
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
  }
  *m_stream << getIndent()
            << fixTypeName("set", "", "")
            << "<int>::iterator item;" << endl
            << getIndent() << "if ((item = "
            << as->remotePart.name
            << "List.find((*it)->id())) != "
            << as->remotePart.name
            << "List.end()) {"
            << endl;
  m_indent++;
  *m_stream << getIndent() << as->remotePart.name
            << "List.erase(item);"
            << endl;
  if (as->type.multiLocal == MULT_N) {
    // N to N association
    // Here we will use a dedicated table for the association
    // Find out the parent and child in this table
    m_indent--;
    *m_stream << getIndent() << "} else {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "if (0 == m_insert"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "m_insert"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement = createStatement(s_insert"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "StatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl
              << getIndent()
              << "m_insert"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement->setDouble(1, obj->id());"
              << endl << getIndent()
              << "m_insert"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement->setDouble(2, (*it)->id());"
              << endl << getIndent()
              << "m_insert"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement->executeUpdate();"
              << endl;
  }
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  if (as->type.multiLocal == MULT_ONE &&
      !as->remotePart.abstract) {
    m_indent--;  
    *m_stream << getIndent() << "}" << endl;
  }
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent() << "// Delete old links"
            << endl << getIndent() << "for ("
            << fixTypeName("set", "", "")
            << "<int>::iterator it = "
            << as->remotePart.name << "List.begin();"
            << endl << getIndent()
            << "     it != "
            << as->remotePart.name << "List.end();"
            << endl << getIndent()
            << "     it++) {"  << endl;
  m_indent++;
  if (as->type.multiLocal == MULT_N) {
    // N to N association
    // Here we will use a dedicated table for the association
    // Find out the parent and child in this table
    *m_stream << getIndent()
              << "if (0 == m_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "m_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement = createStatement(s_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "StatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl << getIndent()
              << "m_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement->setDouble(1, obj->id());"
              << endl << getIndent() << "m_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement->setDouble(2, *it);"
              << endl << getIndent() << "m_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement->executeUpdate();"
              << endl;
  } else {
    // 1 to N association
    // We need to update the remote table that contains the link
    *m_stream << getIndent()
              << "if (0 == m_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement) {" << endl;
    m_indent++;
    *m_stream << getIndent()
              << "m_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement = createStatement(s_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "StatementString);"
              << endl;
    m_indent--;
    *m_stream << getIndent() << "}" << endl << getIndent()
              << "m_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
              << "Statement->setDouble(1, *it);"
              << endl << getIndent() << "m_delete"
              << capitalizeFirstLetter(as->remotePart.typeName)
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
                         capitalizeFirstLetter(as->remotePart.typeName),
                         getIndent(), *m_stream);
  *m_stream << getIndent()
            << "void " << m_classInfo->packageName
            << "::Ora" << m_classInfo->className
            << "Cnv::fillObj"
            << capitalizeFirstLetter(as->remotePart.typeName)
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
            << "if (0 == m_select"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "Statement) {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "m_select"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "Statement = createStatement(s_select"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "StatementString);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent()
            << "// retrieve the object from the database"
            << endl << getIndent()
            << fixTypeName("set", "", "")
            << "<int> " << as->remotePart.name
            << "List;" << endl << getIndent()
            << "m_select"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "Statement->setDouble(1, obj->id());"
            << endl << getIndent()
            << "oracle::occi::ResultSet *rset = "
            << "m_select"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "Statement->executeQuery();"
            << endl << getIndent()
            << "while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {"
            << endl;
  m_indent++;
  *m_stream << getIndent()
            << as->remotePart.name
            << "List.insert(rset->getInt(1));"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent() << "// Close ResultSet"
            << endl << getIndent()
            << "m_select"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "Statement->closeResultSet(rset);"
            << endl << getIndent()
            << "// Update objects and mark old ones for deletion"
            << endl << getIndent()
            << fixTypeName("vector", "", "") << "<"
            << fixTypeName(as->remotePart.typeName,
                           getNamespace(as->remotePart.typeName),
                           m_classInfo->packageName)
            << "*> toBeDeleted;"
            << endl << getIndent()
            << "for ("
            << fixTypeName("vector", "", "")
            << "<"
            << fixTypeName(as->remotePart.typeName,
                           getNamespace(as->remotePart.typeName),
                           m_classInfo->packageName)
            << "*>::iterator it = obj->"
            << as->remotePart.name
            << "().begin();" << endl << getIndent()
            << "     it != obj->"
            << as->remotePart.name
            << "().end();" << endl << getIndent()
            << "     it++) {"  << endl;
  m_indent++;
  *m_stream << getIndent()
            << fixTypeName("set", "", "")
            << "<int>::iterator item;" << endl
            << getIndent() << "if ((item = "
            << as->remotePart.name
            << "List.find((*it)->id())) == "
            << as->remotePart.name
            << "List.end()) {" << endl;
  m_indent++;
  *m_stream << getIndent() << "toBeDeleted.push_back(*it);"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "} else {" << endl;
  m_indent++;
  *m_stream << getIndent() << as->remotePart.name
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
            << fixTypeName(as->remotePart.typeName,
                           getNamespace(as->remotePart.typeName),
                           m_classInfo->packageName)
            << "*>::iterator it = toBeDeleted.begin();"
            << endl << getIndent()
            << "     it != toBeDeleted.end();"
            << endl << getIndent()
            << "     it++) {"  << endl;
  m_indent++;
  *m_stream << getIndent() << "obj->remove"
            << capitalizeFirstLetter(as->remotePart.name)
            << "(*it);" << endl;
  if (as->localPart.name != "") {
    *m_stream << getIndent()
              << "(*it)->";
    if (as->type.multiLocal == MULT_ONE) {
      *m_stream << "set"
                << capitalizeFirstLetter(as->localPart.name)
                << "(0)";
    } else {
      *m_stream << "remove"
                << capitalizeFirstLetter(as->localPart.name)
                << "(obj)";
    }
    *m_stream << ";" << endl;
  }
  m_indent--;
  *m_stream << getIndent() << "}" << endl
            << getIndent()
            << "// Create new objects"
            << endl << getIndent() << "for ("
            << fixTypeName("set", "", "")
            << "<int>::iterator it = "
            << as->remotePart.name << "List.begin();"
            << endl << getIndent()
            << "     it != "
            << as->remotePart.name << "List.end();"
            << endl << getIndent()
            << "     it++) {"  << endl;
  m_indent++;
  *m_stream << getIndent() << "IObject* item"
            << " = cnvSvc()->getObjFromId(*it);"
            << endl << getIndent()
            << fixTypeName(as->remotePart.typeName,
                           getNamespace(as->remotePart.typeName),
                           m_classInfo->packageName)
            << "* remoteObj = " << endl << getIndent()
            << "  dynamic_cast<"
            << fixTypeName(as->remotePart.typeName,
                           getNamespace(as->remotePart.typeName),
                           m_classInfo->packageName)
            << "*>(item);" << endl << getIndent()
            << "obj->add"
            << capitalizeFirstLetter(as->remotePart.name)
            << "(remoteObj);" << endl;
  if (as->localPart.name != "") {
    // Update back link
    *m_stream << getIndent() << "remoteObj->";
    if (as->type.multiLocal == MULT_ONE) {
      *m_stream << "set";
    } else {
      *m_stream << "add";
    }
    *m_stream << capitalizeFirstLetter(as->localPart.name)
              << "(obj);" << endl;
  }
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
            << "if (0 == obj) return;" << endl
            << getIndent()
            << "if (0 != obj->id()) return;" << endl;
  // Start of try block for database statements
  *m_stream << getIndent() << "try {" << endl;
  m_indent++;
  // First check the statements
  *m_stream << getIndent()
            << "// Check whether the statements are ok"
            << endl << getIndent()
            << "if (0 == m_insertStatement) {" << endl;
  m_indent++;
  // Go through the members and assoc to find the number for id
  MemberList members = createMembersList();
  AssocList assocs = createAssocsList();
  int n = members.count(); // start from 0 but count id
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->type.multiRemote == MULT_ONE &&
        as->remotePart.name != "") {
      n++;
    }
  }
  *m_stream << getIndent()
            << "m_insertStatement = createStatement(s_insertStatementString);"
            << endl << getIndent()
            << "m_insertStatement->registerOutParam("
            << n
            << ", oracle::occi::OCCIDOUBLE);" << endl;
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
  *m_stream << getIndent() << "}" << endl;
  // Insert the object into the database
  *m_stream << getIndent()
            << "// Now Save the current object"
            << endl;
  // create a list of members to be saved
  n = 1;
  // Go through the members
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    if (mem->name != "id") {
      writeSingleSetIntoStatement("insert", *mem, n);
      n++;
    }
  }
  // Go through the associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (isEnum(as->remotePart.typeName)) {
      writeSingleSetIntoStatement("insert", as->remotePart, n, true);
      n++;
    } else if (as->type.multiRemote == MULT_ONE &&
               as->remotePart.name != "") {
      *m_stream << getIndent()
                << "m_insertStatement->setDouble("
                << n << ", ";
      if (!isEnum(as->remotePart.typeName)) {
        *m_stream << "(type == OBJ_"
                  << as->remotePart.typeName
                  << " && obj->" << as->remotePart.name
                  << "() != 0) ? obj->"
                  << as->remotePart.name
                  << "()->id() : ";
      }
      *m_stream << "0);" << endl;
      n++;
    }
  }
  *m_stream << getIndent()
            << "m_insertStatement->executeUpdate();"
            << endl << getIndent()
            << "obj->setId((u_signed64)m_insertStatement->getDouble("
            << n << "));" << endl << getIndent()
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
    if (mem->name == "id") {
      idMem = mem;
      continue;
    }
    writeSingleSetIntoStatement("update", *mem, n);
    n++;
  }
  // Go through dependant objects
  AssocList assocs = createAssocsList();
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (isEnum(as->remotePart.typeName)) {
      writeSingleSetIntoStatement("update", as->remotePart, n, true);
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
  AssocList emptyList;
  printSQLError("update", members, emptyList);
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
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (!isEnum(as->remotePart.typeName) &&
        as->type.kind == COMPOS_PARENT) {
      fixTypeName(as->remotePart.typeName,
                  getNamespace(as->remotePart.typeName),
                  m_classInfo->packageName);
      if (as->type.multiRemote == MULT_ONE) {
        // One to one association
        *m_stream << getIndent() << "if (obj->"
                  << as->remotePart.name
                  << "() != 0) {" << endl;
        m_indent++;
        fixTypeName("OraCnvSvc",
                    "castor::db::ora",
                    m_classInfo->packageName);
        *m_stream << getIndent()
                  << "cnvSvc()->deleteRep(0, obj->"
                  << as->remotePart.name << "(), false);"
                  << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl;
      } else if (as->type.multiRemote == MULT_N) {
        // One to n association, loop over the vector
        *m_stream << getIndent() << "for ("
                  << fixTypeName("vector", "", "")
                  << "<"
                  << fixTypeName(as->remotePart.typeName,
                                 getNamespace(as->remotePart.typeName),
                                 m_classInfo->packageName)
                  << "*>::iterator it = obj->"
                  << as->remotePart.name
                  << "().begin();" << endl << getIndent()
                  << "     it != obj->"
                  << as->remotePart.name
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
    for (Assoc* as = assocs.first();
         0 != as;
         as = assocs.next()) {
      if ((as->type.kind == COMPOS_PARENT ||
           as->type.kind == AGGREG_PARENT) &&
          as->type.multiRemote == MULT_N &&
          as->type.multiLocal == MULT_N) {
        *m_stream << getIndent()
                  << "// Delete "
                  << as->remotePart.name << " object"
                  << endl << getIndent()
                  << "if (0 != obj->"
                  << as->remotePart.name << "()) {" << endl;
        m_indent++;
        // N to N association
        // Here we will use a dedicated table for the association
        // Find out the parent and child in this table
        *m_stream << getIndent()
                  << "// Check whether the statement is ok"
                  << endl << getIndent()
                  << "if (0 == m_delete"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement) {" << endl;
        m_indent++;
        *m_stream << getIndent()
                  << "m_delete"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement = createStatement(s_delete"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "StatementString);"
                  << endl;
        m_indent--;
        *m_stream << getIndent() << "}" << endl
                  << getIndent()
                  << "// Delete links to objects"
                  << endl << getIndent()
                  << "m_delete"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement->setDouble(1, obj->"
                  << as->remotePart.name << "()->id());"
                  << endl << getIndent()
                  << "m_delete"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement->setDouble(2, obj->id());"
                  << endl << getIndent()
                  << "m_delete"
                  << capitalizeFirstLetter(as->remotePart.typeName)
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
  *m_stream << getIndent() << fixTypeName("BaseAddress",
                                          "castor",
                                          m_classInfo->packageName)
            << "* ad = " << endl
            << getIndent() << "  dynamic_cast<"
            << fixTypeName("BaseAddress",
                           "castor",
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
            << "m_selectStatement->setDouble(1, ad->target());"
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
            << " << ad->target();" << endl
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
    if (as->type.multiRemote == MULT_ONE) {
      bool isenum = isEnum(as->remotePart.typeName);
      if (isenum) {
        writeSingleGetFromSelect
          (as->remotePart, n, !isenum, isenum);
      }
      n++;
    }
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
    if (as->type.multiRemote == MULT_ONE) {
      // Enums are a simple case
      if (isEnum(as->remotePart.typeName)) {
        writeSingleGetFromSelect(as->remotePart, n, false, true);
      }
      n++;
    }
  }
  // Close request
  *m_stream << getIndent()
            << "m_selectStatement->closeResultSet(rset);"
            << endl;
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
                                                Member mem,
                                                int n,
                                                bool isEnum) {
  // deal with arrays of chars
  bool isArray = mem.typeName.find('[') > 0;
  if (isArray) {
    int i1 = mem.typeName.find('[');
    int i2 = mem.typeName.find(']');
    QString length = mem.typeName.mid(i1+1, i2-i1-1);
    *m_stream << getIndent() << fixTypeName("string", "", "")
              << " " << mem.name << "S((const char*)" << "obj->"
              << mem.name << "(), " << length << ");"
              << endl;
  }
  *m_stream << getIndent()
            << "m_" << statement << "Statement->set";
  if (isEnum) {
    *m_stream << "Int";
  } else {
    *m_stream << getOraType(mem.typeName);
  }
  *m_stream << "(" << n << ", ";
  if (isArray) {
    *m_stream << mem.name << "S";
  } else {
    if (isEnum) {
      *m_stream << "(int)";
    }
    *m_stream << "obj->"
              << mem.name
              << "()";
  }
  *m_stream << ");" << endl;
}

//=============================================================================
// writeSingleGetFromSelect
//=============================================================================
void CppCppOraCnvWriter::writeSingleGetFromSelect(Member mem,
                                                  int n,
                                                  bool isAssoc,
                                                  bool isEnum) {
  *m_stream << getIndent();
  // deal with arrays of chars
  bool isArray = mem.typeName.find('[') > 0;
  if (isAssoc) {
    *m_stream << "u_signed64 " << mem.name
              << "Id = ";
  } else {
    *m_stream << "object->set"
              << capitalizeFirstLetter(mem.name)
              << "(";
  }
  if (isArray) {
    *m_stream << "("
              << mem.typeName.left(mem.typeName.find('['))
              << "*)";
  }
  if (isEnum) {
    *m_stream << "(enum "
              << fixTypeName(mem.typeName,
                             getNamespace(mem.typeName),
                             m_classInfo->packageName)
              << ")";
  }
  if (isAssoc || mem.typeName == "u_signed64") {
    *m_stream << "(u_signed64)";
  }
  *m_stream << "rset->get";
  if (isEnum) {
    *m_stream << "Int";
  } else if (isAssoc) {
    *m_stream << "Double";
  } else {
    *m_stream << getOraType(mem.typeName);
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
    SQLType = "VARCHAR(2048)";
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
              << "                << \"and id was \" << ad->target() << std::endl;";
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
                << mem->name << " : \" << obj->"
                << mem->name << "() << std::endl";
    }
    // Go through the associations
    for (Assoc* as = assocs.first();
         0 != as;
         as = assocs.next()) {
      if (as->type.multiRemote == MULT_ONE &&
          as->remotePart.name != "") {
        *m_stream << endl << getIndent()
                  << "                << \"  "
                  << as->remotePart.name << " : \" << obj->"
                  << as->remotePart.name << "() << std::endl";
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
  UMLObject* obj = getClassifier(QString("Request"));
  const UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
  return m_classInfo->allSuperclasses.contains(concept);
}

// Include files
#include <qregexp.h>

// local
#include "cpphodbccnvwriter.h"

//-----------------------------------------------------------------------------
// Implementation file for class : CppHOdbcCnvWriter
//
// 2004-01-13 : Sebastien Ponce
//-----------------------------------------------------------------------------

//=============================================================================
// Standard constructor, initializes variables
//=============================================================================
CppHOdbcCnvWriter::CppHOdbcCnvWriter(UMLDoc *parent, const char *name) :
  CppHBaseCnvWriter(parent, name) {
  setPrefix("Odbc");
}

//=============================================================================
// Initialization
//=============================================================================
bool CppHOdbcCnvWriter::init(UMLClassifier* c, QString fileName) {
  // call upper class init
  this->CppBaseWriter::init(c, fileName);
  // fixes the namespace
  m_classInfo->packageName = "castor::db::odbc";
  // calls the postinit of this class
  postinit(c, fileName);
  return true;
}

//=============================================================================
// writeClass
//=============================================================================
void CppHOdbcCnvWriter::writeClass(UMLClassifier */*c*/) {
  // Generates class declaration
  int ClassIndentLevel = m_indent;
  writeClassDecl(QString("A converter for storing/retrieving ") +
                 m_classInfo->className + " into/from an Odbccle database");
  // Constructor and methods
  *m_stream << getIndent(INDENT*ClassIndentLevel)
            << scopeToCPPDecl(Uml::Public) << ":" << endl << endl;
  writeMethods();
  // Members
  *m_stream << getIndent(INDENT*ClassIndentLevel)
            << scopeToCPPDecl(Uml::Private) << ":" << endl << endl;
  writeMembers();
  // end of class header
  m_indent--;
  *m_stream << getIndent() << "}; // end of class Odbc"
            << m_classInfo->className << "Cnv" << endl << endl;
}

//=============================================================================
// writeMembers
//=============================================================================
void CppHOdbcCnvWriter::writeMembers() {
    // Dealing with object itself (insertion, deletion, selection, update)
  *m_stream << getIndent()
            << "/// SQL statement for request insertion"
            << endl << getIndent()
            << "static const std::string s_insertStatementString;"
            << endl << endl << getIndent()
            << "/// SQL statement object for request insertion"
            << endl << getIndent()
            << "HSTMT m_insertStatement;"
            << endl << endl << getIndent()
            << "/// SQL statement for request deletion"
            << endl << getIndent()
            << "static const std::string s_deleteStatementString;"
            << endl << endl << getIndent()
            << "/// SQL statement object for request deletion"
            << endl << getIndent()
            << "HSTMT m_deleteStatement;"
            << endl << endl << getIndent()
            << "/// SQL statement for request selection"
            << endl << getIndent()
            << "static const std::string s_selectStatementString;"
            << endl << endl << getIndent()
            << "/// SQL statement object for request selection"
            << endl << getIndent()
            << "HSTMT m_selectStatement;"
            << endl << endl << getIndent()
            << "/// SQL statement for request update"
            << endl << getIndent()
            << "static const std::string s_updateStatementString;"
            << endl << endl << getIndent()
            << "/// SQL statement object for request update"
            << endl << getIndent()
            << "HSTMT m_updateStatement;"
            << endl << endl;    
  // Dealing with object status if we have a request
  UMLObject* obj = getClassifier(QString("Request"));
  const UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
  if (m_classInfo->allSuperclasses.contains(concept)) {
    *m_stream << getIndent()
              << "/// SQL statement for request status insertion"
              << endl << getIndent()
              << "static const std::string s_insertStatusStatementString;"
              << endl << endl << getIndent()
              << "/// SQL statement object for request status insertion"
              << endl << getIndent()
              << "HSTMT m_insertStatusStatement;"
              << endl << endl << getIndent()
              << "/// SQL statement for status deletion"
              << endl << getIndent()
              << "static const std::string s_deleteStatusStatementString;"
              << endl << endl << getIndent()
              << "/// SQL statement object for request status deletion"
              << endl << getIndent()
              << "HSTMT m_deleteStatusStatement;"
              << endl << endl;
  }
  // Dealing with type identification (storage and deletion)
  *m_stream << getIndent()
            << "/// SQL statement for type storage "
            << endl << getIndent()
            << "static const std::string s_storeTypeStatementString;"
            << endl << endl << getIndent()
            << "/// SQL statement object for type storage"
            << endl << getIndent()
            << "HSTMT m_storeTypeStatement;"
            << endl << endl << getIndent()
            << "/// SQL statement for type deletion "
            << endl << getIndent()
            << "static const std::string s_deleteTypeStatementString;"
            << endl << endl << getIndent()
            << "/// SQL statement object for type deletion"
            << endl << getIndent()
            << "HSTMT m_deleteTypeStatement;"
            << endl << endl;
  // One to n associations dedicated statements
  AssocList assocs = createAssocsList();
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->type.multiRemote == MULT_N) {
      *m_stream << getIndent() 
                << "/// SQL insert statement for member "
                << as->remotePart.name
                << endl << getIndent()
                << "static const std::string s_insert"
                << m_classInfo->className << "2"
                << capitalizeFirstLetter(as->remotePart.name)
                << "StatementString;"
                << endl << endl << getIndent()
                << "/// SQL insert statement object for member "
                << as->remotePart.name
                << endl << getIndent()
                << "HSTMT m_insert"
                << m_classInfo->className << "2"
                << capitalizeFirstLetter(as->remotePart.name)
                << "Statement;"
                << endl << endl << getIndent() 
                << "/// SQL delete statement for member "
                << as->remotePart.name
                << endl << getIndent()
                << "static const std::string s_delete"
                << m_classInfo->className << "2"
                << capitalizeFirstLetter(as->remotePart.name)
                << "StatementString;"
                << endl << endl << getIndent()
                << "/// SQL delete statement object for member "
                << as->remotePart.name
                << endl << getIndent()
                << "HSTMT m_delete"
                << m_classInfo->className << "2"
                << capitalizeFirstLetter(as->remotePart.name)
                << "Statement;"
                << endl << endl << getIndent() 
                << "/// SQL select statement for member "
                << as->remotePart.name
                << endl << getIndent()
                << "static const std::string s_"
                << m_classInfo->className << "2"
                << capitalizeFirstLetter(as->remotePart.name)
                << "StatementString;"
                << endl << endl << getIndent()
                << "/// SQL select statement object for member "
                << as->remotePart.name
                << endl << getIndent()
                << "HSTMT m_"
                << m_classInfo->className << "2"
                << capitalizeFirstLetter(as->remotePart.name)
                << "Statement;"
                << endl << endl;
    }
  }
}

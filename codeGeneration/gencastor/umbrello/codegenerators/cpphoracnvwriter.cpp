// Include files
#include <qregexp.h>

// local
#include "cpphoracnvwriter.h"

//-----------------------------------------------------------------------------
// Implementation file for class : CppHOraCnvWriter
//
// 2004-01-13 : Sebastien Ponce
//-----------------------------------------------------------------------------

//=============================================================================
// Standard constructor, initializes variables
//=============================================================================
CppHOraCnvWriter::CppHOraCnvWriter(UMLDoc *parent, const char *name) :
  CppHBaseCnvWriter(parent, name) {
  setPrefix("Ora");
}

//=============================================================================
// Initialization
//=============================================================================
bool CppHOraCnvWriter::init(UMLClassifier* c, QString fileName) {
  // call upper class init
  this->CppBaseWriter::init(c, fileName);
  // fixes the namespace
  m_classInfo->packageName = "castor::db::ora";
  // calls the postinit of this class
  postinit(c, fileName);
  return true;
}

//=============================================================================
// writeClass
//=============================================================================
void CppHOraCnvWriter::writeClass(UMLClassifier *c) {
  // Generates class declaration
  int ClassIndentLevel = m_indent;
  writeClassDecl(QString("A converter for storing/retrieving ") +
                 m_classInfo->className + " into/from an Oracle database");
  // Constructor and methods
  *m_stream << getIndent(INDENT*ClassIndentLevel)
            << scopeToCPPDecl(Uml::Public) << ":" << endl << endl;
  writeMethods();
  // Add reset method
  writeReset(c);
  // Members
  *m_stream << getIndent(INDENT*ClassIndentLevel)
            << scopeToCPPDecl(Uml::Private) << ":" << endl << endl;
  writeMembers();
  // end of class header
  m_indent--;
  *m_stream << getIndent() << "}; // end of class Ora"
            << m_classInfo->className << "Cnv" << endl << endl;
}

//=============================================================================
// writeReset
//=============================================================================
void CppHOraCnvWriter::writeReset(UMLClassifier */*c*/) {
  writeDocumentation ("Reset the converter statements.",
                      "", QString(""), *m_stream);
  // Force inclusion of ObjectSet
  *m_stream << getIndent()
            << "void reset() throw ();"
            << endl << endl;
}

//=============================================================================
// writeMembers
//=============================================================================
void CppHOraCnvWriter::writeMembers() {
    // Dealing with object itself (insertion, deletion, selection, update)
  *m_stream << getIndent()
            << "/// SQL statement for request insertion"
            << endl << getIndent()
            << "static const std::string s_insertStatementString;"
            << endl << endl << getIndent()
            << "/// SQL statement object for request insertion"
            << endl << getIndent()
            << "oracle::occi::Statement *m_insertStatement;"
            << endl << endl << getIndent()
            << "/// SQL statement for request deletion"
            << endl << getIndent()
            << "static const std::string s_deleteStatementString;"
            << endl << endl << getIndent()
            << "/// SQL statement object for request deletion"
            << endl << getIndent()
            << "oracle::occi::Statement *m_deleteStatement;"
            << endl << endl << getIndent()
            << "/// SQL statement for request selection"
            << endl << getIndent()
            << "static const std::string s_selectStatementString;"
            << endl << endl << getIndent()
            << "/// SQL statement object for request selection"
            << endl << getIndent()
            << "oracle::occi::Statement *m_selectStatement;"
            << endl << endl << getIndent()
            << "/// SQL statement for request update"
            << endl << getIndent()
            << "static const std::string s_updateStatementString;"
            << endl << endl << getIndent()
            << "/// SQL statement object for request update"
            << endl << getIndent()
            << "oracle::occi::Statement *m_updateStatement;"
            << endl << endl;    
  // Dealing with object status if we have a request
  UMLObject* obj = m_doc->findUMLObject(QString("Request"),
                                        Uml::ot_Class);
  const UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
  if (m_classInfo->allSuperclasses.contains(concept)) {
    *m_stream << getIndent()
              << "/// SQL statement for request status insertion"
              << endl << getIndent()
              << "static const std::string s_insertStatusStatementString;"
              << endl << endl << getIndent()
              << "/// SQL statement object for request status insertion"
              << endl << getIndent()
              << "oracle::occi::Statement *m_insertStatusStatement;"
              << endl << endl << getIndent()
              << "/// SQL statement for status deletion"
              << endl << getIndent()
              << "static const std::string s_deleteStatusStatementString;"
              << endl << endl << getIndent()
              << "/// SQL statement object for request status deletion"
              << endl << getIndent()
              << "oracle::occi::Statement *m_deleteStatusStatement;"
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
            << "oracle::occi::Statement *m_storeTypeStatement;"
            << endl << endl << getIndent()
            << "/// SQL statement for type deletion "
            << endl << getIndent()
            << "static const std::string s_deleteTypeStatementString;"
            << endl << endl << getIndent()
            << "/// SQL statement object for type deletion"
            << endl << getIndent()
            << "oracle::occi::Statement *m_deleteTypeStatement;"
            << endl << endl;
  // Associations dedicated statements
  AssocList assocs = createAssocsList();
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_N) {
      *m_stream << getIndent() 
                << "/// SQL select statement for member "
                << as->second.first
                << endl << getIndent()
                << "static const std::string s_"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << "StatementString;"
                << endl << endl << getIndent()
                << "/// SQL select statement object for member "
                << as->second.first
                << endl << getIndent()
                << "oracle::occi::Statement *m_"
                << capitalizeFirstLetter(as->second.third)
                << "2"
                << capitalizeFirstLetter(as->second.second)
                << "Statement;"
                << endl << endl;
    } else if (as->first.second == COMPOS_CHILD ||
               as->first.second == AGGREG_CHILD) {
      *m_stream << getIndent() 
                << "/// SQL insert statement for member "
                << as->second.first
                << endl << getIndent()
                << "static const std::string s_insert"
                << capitalizeFirstLetter(as->second.second)
                << "2" << capitalizeFirstLetter(as->second.third)
                << "StatementString;"
                << endl << endl << getIndent()
                << "/// SQL insert statement object for member "
                << as->second.first
                << endl << getIndent()
                << "oracle::occi::Statement *m_insert"
                << capitalizeFirstLetter(as->second.second)
                << "2" << capitalizeFirstLetter(as->second.third)
                << "Statement;"
                << endl << endl << getIndent() 
                << "/// SQL delete statement for member "
                << as->second.first
                << endl << getIndent()
                << "static const std::string s_delete"
                << capitalizeFirstLetter(as->second.second)
                << "2" << capitalizeFirstLetter(as->second.third)
                << "StatementString;"
                << endl << endl << getIndent()
                << "/// SQL delete statement object for member "
                << as->second.first
                << endl << getIndent()
                << "oracle::occi::Statement *m_delete"
                << capitalizeFirstLetter(as->second.second)
                << "2" << capitalizeFirstLetter(as->second.third)
                << "Statement;"
                << endl << endl;      
    }
  }
}

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
// writeFillRep
//=============================================================================
void CppHOraCnvWriter::writeFillRep() {
  // First write the main function, dispatching the requests
  writeDocumentation
    (QString("Fill the foreign representation with some of the objects.") +
     "refered by a given C++ object.",
     "",
     QString("@param address the place where to find the foreign representation\n") +
     "@param object the original C++ object\n" +
     "@param type the type of the refered objects to store\n" +
     "@param autocommit whether the changes to the database\n" +
     "should be commited or not\n" +
     "@exception Exception throws an Exception in case of error",
     *m_stream);
  *m_stream << getIndent()
            << "virtual void fillRep("
            << fixTypeName("IAddress*",
                           "castor",
                           m_classInfo->packageName)
            << " address," << endl << getIndent()
            << "                     "
            << fixTypeName("IObject*",
                           "castor",
                           m_classInfo->packageName)
            << " object," << endl << getIndent()
            << "                     unsigned int type,"
            << endl << getIndent()
            << "                     bool autocommit)"
            << endl << getIndent() << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           m_classInfo->packageName)
            << ");"
            << endl << endl;

  // Now write the dedicated fillRep Methods
  AssocList assocs = createAssocsList();
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (!isEnum(as->remotePart.typeName)) {
      if (as->type.multiRemote == MULT_ONE ||
          as->type.multiRemote == MULT_N) {
        writeBasicFillRep(as);
      }
    }
  }
}

//=============================================================================
// writeFillObj
//=============================================================================
void CppHOraCnvWriter::writeFillObj() {
  // First write the main function, dispatching the requests
  writeDocumentation
    ("Retrieve from the database some of the objects refered by a given object.",
     "",
     QString("@param object the original object\n") +
     "@param type the type of the refered objects to retrieve\n" +
     "@exception Exception throws an Exception in case of error",
     *m_stream);
  *m_stream << getIndent()
            << "virtual void fillObj("
            << fixTypeName("IAddress*",
                           "castor",
                           m_classInfo->packageName)
            << " address," << endl << getIndent()
            << "                     "
            << fixTypeName("IObject*",
                           "castor",
                           m_classInfo->packageName)
            << " object," << endl << getIndent()
            << "                     unsigned int type)"
            << endl << getIndent() << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           m_classInfo->packageName)
            << ");"
            << endl << endl;

  // Now write the dedicated fillRep Methods
  AssocList assocs = createAssocsList();
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (!isEnum(as->remotePart.typeName)) {
      if (as->type.multiRemote == MULT_ONE ||
          as->type.multiRemote == MULT_N) {
        writeBasicFillObj(as);
      }
    }
  }

}

//=============================================================================
// writeBasicFillRep
//=============================================================================
void CppHOraCnvWriter::writeBasicFillRep(Assoc* as) {
  writeDocumentation
    (QString("Fill the database with objects of type ") +
     capitalizeFirstLetter(as->remotePart.typeName)
     + " refered by a given object.",
     "",
     QString("@param obj the original object\n") +
     "@exception Exception throws an Exception in case of error",
     *m_stream);
  *m_stream << getIndent() << "virtual void fillRep"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "("
            << fixTypeName(m_classInfo->className + "*",
                           getNamespace(m_classInfo->className),
                           m_classInfo->packageName)
            << " obj)"
            << endl << getIndent() << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           m_classInfo->packageName)
            << ");"
            << endl << endl;
}

//=============================================================================
// writeBasicFillObj
//=============================================================================
void CppHOraCnvWriter::writeBasicFillObj(Assoc* as) {
  writeDocumentation
    (QString("Retrieve from the database objects of type ") +
     capitalizeFirstLetter(as->remotePart.typeName)
     + " refered by a given object.",
     "",
     QString("@param obj the original object\n") +
     "@exception Exception throws an Exception in case of error",
     *m_stream);
  *m_stream << getIndent()
            << "virtual void fillObj"
            << capitalizeFirstLetter(as->remotePart.typeName)
            << "("
            << fixTypeName(m_classInfo->className + "*",
                           getNamespace(m_classInfo->className),
                           m_classInfo->packageName)
            << " obj)"
            << endl << getIndent() << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           m_classInfo->packageName)
            << ");"
            << endl << endl;
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
  // FillRep methods
  writeFillRep();
  // FillObj methods
  writeFillObj();
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
    if (isEnum(as->remotePart.typeName)) continue;
    if (as->type.multiRemote == MULT_N &&
        as->type.multiLocal == MULT_N) {
      // N to N association
      // Here we will use a dedicated table for the association
      // Find out the parent and child in this table
      *m_stream << getIndent()
                << "/// SQL insert statement for member "
                << as->remotePart.name
                << endl << getIndent()
                << "static const std::string s_insert"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "StatementString;"
                << endl << endl << getIndent()
                << "/// SQL insert statement object for member "
                << as->remotePart.name
                << endl << getIndent()
                << "oracle::occi::Statement *m_insert"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "Statement;"
                << endl << endl << getIndent()
                << "/// SQL delete statement for member "
                << as->remotePart.name
                << endl << getIndent()
                << "static const std::string s_delete"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "StatementString;"
                << endl << endl << getIndent()
                << "/// SQL delete statement object for member "
                << as->remotePart.name
                << endl << getIndent()
                << "oracle::occi::Statement *m_delete"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "Statement;"
                << endl << endl << getIndent()
                << "/// SQL select statement for member "
                << as->remotePart.name
                << endl << getIndent()
                << "static const std::string s_select"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "StatementString;"
                << endl << endl << getIndent()
                << "/// SQL select statement object for member "
                << as->remotePart.name
                << endl << getIndent()
                << "oracle::occi::Statement *m_select"
                << capitalizeFirstLetter(as->remotePart.typeName)
                << "Statement;"
                << endl << endl;
    } else {
      if (as->type.multiLocal == MULT_ONE) {
        // 1 to * association
        *m_stream << getIndent()
                  << "/// SQL select statement for member "
                  << as->remotePart.name
                  << endl << getIndent()
                  << "static const std::string s_select"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "StatementString;"
                  << endl << endl << getIndent()
                  << "/// SQL select statement object for member "
                  << as->remotePart.name
                  << endl << getIndent()
                  << "oracle::occi::Statement *m_select"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement;"
                  << endl << endl << getIndent()
                  << "/// SQL delete statement for member "
                  << as->remotePart.name
                  << endl << getIndent()
                  << "static const std::string s_delete"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "StatementString;"
                  << endl << endl << getIndent()
                  << "/// SQL delete statement object for member "
                  << as->remotePart.name
                  << endl << getIndent()
                  << "oracle::occi::Statement *m_delete"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement;"
                  << endl << endl;
      }
      if (as->type.multiRemote == MULT_ONE) {
        // * to 1
        *m_stream << getIndent()
                  << "/// SQL checkExist statement for member "
                  << as->remotePart.name
                  << endl << getIndent()
                  << "static const std::string s_check"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "ExistStatementString;"
                  << endl << endl << getIndent()
                  << "/// SQL checkExist statement object for member "
                  << as->remotePart.name
                  << endl << getIndent()
                  << "oracle::occi::Statement *m_check"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "ExistStatement;"
                  << endl << endl << getIndent()
                  << "/// SQL update statement for member "
                  << as->remotePart.name
                  << endl << getIndent()
                  << "static const std::string s_update"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "StatementString;"
                  << endl << endl << getIndent()
                  << "/// SQL update statement object for member "
                  << as->remotePart.name
                  << endl << getIndent()
                  << "oracle::occi::Statement *m_update"
                  << capitalizeFirstLetter(as->remotePart.typeName)
                  << "Statement;"
                  << endl << endl;
      }
    }
  }
}

// Include files
#include <qregexp.h>
#include <qmap.h>

// local
#include "cppcppbasecnvwriter.h"

//-----------------------------------------------------------------------------
// Implementation file for class : CppCppBaseCnvWriter
//
// 2004-01-13 : Sebastien Ponce
//-----------------------------------------------------------------------------

//=============================================================================
// Standard constructor, initializes variables
//=============================================================================
CppCppBaseCnvWriter::CppCppBaseCnvWriter(UMLDoc *parent, const char *name) :
  CppCppWriter(parent, name) {}

//=============================================================================
// writeFactory
//=============================================================================
void CppCppBaseCnvWriter::writeFactory() {
  writeWideHeaderComment("Instantiation of a static factory class",
                         getIndent(),
                         *m_stream);
  *m_stream << getIndent() << "static "
            << fixTypeName("CnvFactory",
                           "castor",
                           m_classInfo->packageName)
            << "<" << m_classInfo->fullPackageName << m_prefix
            << m_classInfo->className << "Cnv> s_factory"
            << m_prefix
            << m_classInfo->className << "Cnv;" << endl
            << getIndent() << "const "
            << fixTypeName("IFactory",
                           "castor",
                           m_classInfo->packageName)
            << "<" << fixTypeName("IConverter",
                                  "castor",
                                  m_classInfo->packageName)
            << ">& " << m_prefix << m_classInfo->className
            << "CnvFactory = " << endl << getIndent()
            << "  s_factory" << m_prefix << m_classInfo->className
            << "Cnv;" << endl << endl;
}


//=============================================================================
// writeObjType
//=============================================================================
void CppCppBaseCnvWriter::writeObjType() {
  // Static method
  writeWideHeaderComment("ObjType", getIndent(), *m_stream);
  *m_stream << getIndent() << "const unsigned int "
            << m_classInfo->fullPackageName << m_prefix
            << m_classInfo->className
            << "Cnv::ObjType() {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "return " << m_originalPackage
            << m_classInfo->className << "::TYPE();" << endl;
  m_indent--;
  *m_stream << "}" << endl << endl;
  writeWideHeaderComment("objType", getIndent(), *m_stream);
  *m_stream << getIndent() << "const unsigned int "
            << m_classInfo->fullPackageName << m_prefix
            << m_classInfo->className
            << "Cnv::objType() const {" << endl;
  m_indent++;
  *m_stream << getIndent()
            << "return ObjType();"
            << endl;
  m_indent--;
  *m_stream << "}" << endl << endl;
}

//=============================================================================
// writeCreateRep
//=============================================================================
void CppCppBaseCnvWriter::writeCreateRep() {
  // Header
  writeWideHeaderComment("createRep", getIndent(), *m_stream);
  QString str = QString("void ") + m_classInfo->packageName + "::" +
    m_prefix + m_classInfo->className + "Cnv::createRep(";
  *m_stream << getIndent() << str
            << fixTypeName ("IAddress*", "castor", m_classInfo->packageName)
            << " address," << endl;
  str.replace(QRegExp("."), " ");
  *m_stream << getIndent() << str
            << fixTypeName("IObject*", "castor", m_classInfo->packageName)
            << " object,"
            << endl << getIndent() << str
            << "bool autocommit)" << endl
            << getIndent() << "  throw ("
            << fixTypeName ("Exception",
                            "castor.exception",
                            m_classInfo->packageName)
            << ") {" << endl;
  m_indent++;
  // Get the precise object
  *m_stream << getIndent() << m_originalPackage
            << m_classInfo->className << "* obj = " << endl
            << getIndent() << "  dynamic_cast<"
            << m_originalPackage
            << m_classInfo->className << "*>(object);"
            << endl;
  // Write the content of the method
  writeCreateRepContent();
  // End of the method
  m_indent--;
  *m_stream << "}" << endl << endl;
}

//=============================================================================
// writeUpdateRep
//=============================================================================
void CppCppBaseCnvWriter::writeUpdateRep() {
  // Header
  writeWideHeaderComment("updateRep", getIndent(), *m_stream);
  QString str = QString("void ") + m_classInfo->packageName + "::" +
    m_prefix + m_classInfo->className + "Cnv::updateRep(";
  *m_stream << getIndent() << str
            << fixTypeName ("IAddress*", "castor", m_classInfo->packageName)
            << " address," << endl;
  str.replace(QRegExp("."), " ");
  *m_stream << getIndent() << str
            << fixTypeName("IObject*", "castor", m_classInfo->packageName)
            << " object,"
            << endl << getIndent() << str
            << "bool autocommit)" << endl
            << getIndent() << "  throw ("
            << fixTypeName ("Exception",
                            "castor.exception",
                            m_classInfo->packageName)
            << ") {" << endl;
  m_indent++;
  // Write the content of the method
  writeUpdateRepContent();
  // End of the method
  m_indent--;
  *m_stream << "}" << endl << endl;
}

//=============================================================================
// writeDeleteRep
//=============================================================================
void CppCppBaseCnvWriter::writeDeleteRep() {
  // Header
  writeWideHeaderComment("deleteRep", getIndent(), *m_stream);
  QString str = QString("void ") + m_classInfo->packageName + "::" +
    m_prefix + m_classInfo->className + "Cnv::deleteRep(";
  *m_stream << getIndent() << str
            << fixTypeName ("IAddress*", "castor", m_classInfo->packageName)
            << " address," << endl;
  str.replace(QRegExp("."), " ");
  *m_stream << getIndent() << str
            << fixTypeName("IObject*", "castor", m_classInfo->packageName)
            << " object,"
            << endl << getIndent() << str
            << "bool autocommit)" << endl
            << getIndent() << "  throw ("
            << fixTypeName ("Exception",
                            "castor.exception",
                            m_classInfo->packageName)
            << ") {" << endl;
  m_indent++;
  // Write the content of the method
  writeDeleteRepContent();
  // End of the method
  m_indent--;
  *m_stream << "}" << endl << endl;
}

//=============================================================================
// writeCreateObj
//=============================================================================
void CppCppBaseCnvWriter::writeCreateObj() {
  // Header
  writeWideHeaderComment("createObj", getIndent(), *m_stream);
  *m_stream << getIndent()
            << fixTypeName("IObject*", "castor", m_classInfo->packageName)
            << " " << m_classInfo->packageName
            << "::" << m_prefix
            << m_classInfo->className << "Cnv::createObj("
            << fixTypeName("IAddress*", "castor", m_classInfo->packageName)
            << " address)" << endl << getIndent()
            << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           m_classInfo->packageName)
            << ") {" << endl;
  m_indent++;
  // Write the content of the method
  writeCreateObjContent();
  // End of the method
  m_indent--;
  *m_stream << getIndent() << "}" << endl << endl;
}

//=============================================================================
// writeUpdateObj
//=============================================================================
void CppCppBaseCnvWriter::writeUpdateObj() {
  // Header
  writeWideHeaderComment("updateObj", getIndent(), *m_stream);
  
  *m_stream << getIndent() << QString("void ")
            << m_classInfo->packageName << "::"
            << m_prefix << m_classInfo->className
            << "Cnv::updateObj("
            << fixTypeName("IObject*", "castor", m_classInfo->packageName)
            << " obj)" << endl
            << getIndent() << "  throw ("
            << fixTypeName ("Exception",
                            "castor.exception",
                            m_classInfo->packageName)
            << ") {" << endl;
  m_indent++;
  // Write the content of the method
  writeUpdateObjContent();
  // End of the method
  m_indent--;
  *m_stream << "}" << endl << endl;
}


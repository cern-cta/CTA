// Include files 
#include <qregexp.h>

// local
#include "classifierinfo.h"
#include "cpphwriter.h"

//-----------------------------------------------------------------------------
// Implementation file for class : CppHWriter
//
// 2004-01-13 : Sebastien Ponce
//-----------------------------------------------------------------------------

//=============================================================================
// Constructor, initializes variables
//=============================================================================
CppHWriter::CppHWriter(UMLDoc *parent, const char *name) :
  CppBaseWriter(parent, name) {
    // Allow forward declarations
    m_allowForward = true;
}

//=============================================================================
// Initialization
//=============================================================================
bool CppHWriter::init(UMLClassifier* c, QString fileName) {
  // call upper class init
  this->CppBaseWriter::init(c, fileName);
  // calls the postinit of this class
  postinit(c, fileName);
  return true;
}

//=============================================================================
// Post Initialization
//=============================================================================
bool CppHWriter::postinit(UMLClassifier* /*c*/, QString fileName) {
  // Write Header
  QString str = getHeadingFile(".h");
  if(!str.isEmpty()) {
    str.replace(QRegExp("%filename%"), fileName);
    str.replace(QRegExp("%filepath%"), m_file.name());
    *m_mainStream << str << endl;
  }
  // Write the hash define stuff to prevent multiple parsing/inclusion of header
  QString hashDefine = m_classInfo->packageName.upper().replace("::",  "_");
  if (!hashDefine.isEmpty()) hashDefine.append('_');
  hashDefine.append(m_classInfo->className.upper().simplifyWhiteSpace().replace(QRegExp(" "),  "_"));
  *m_mainStream << "#ifndef "<< hashDefine + "_HPP" << endl;
  *m_mainStream << "#define "<< hashDefine + "_HPP" << endl << endl;

  // Compute the indentation due to namespaces
  if(!m_classInfo->packageName.isEmpty()) {
    QString ns = m_classInfo->packageName;
    int colonPos;
    while ((colonPos = ns.find("::")) > 0) {
      m_indent++;
      ns = ns.mid(colonPos+2);
    }
    m_indent++;
  }
  return true;
}

//=============================================================================
// Finalization
//=============================================================================
bool CppHWriter::finalize() {
  int innerIndent = m_indent;
  m_indent = 0;
  // Writes the includes files
  writeIncludesFromSet(*m_mainStream, m_includes);
  // If there was any include, put a new line after includes
  if (!m_firstInclude) *m_mainStream << endl;
  // Write the forward declarations and namespace declarations
  if (m_classInfo->isEnum) {
    *m_mainStream << "#ifdef __cplusplus" << endl;
  }
  writeNSForward(*m_mainStream, m_classInfo->packageName);
  if (m_classInfo->isEnum) {
    *m_mainStream << "#endif" << endl;
  }
  // and put the buffer content into the file
  *m_mainStream << m_buffer;
  // end of class namespace, if any
  m_indent = innerIndent;
  if(!m_classInfo->packageName.isEmpty()) {
    if (m_classInfo->isEnum) {
      *m_mainStream << "#ifdef __cplusplus" << endl;
    }
    QString ns = m_classInfo->packageName;
    int colonPos;
    while ((colonPos = ns.findRev("::")) > 0) {
      writeNSClose(*m_mainStream, ns.right(ns.length()-colonPos-2));
      ns = ns.mid(0, colonPos);
    }
    writeNSClose(*m_mainStream, ns);
    if (m_classInfo->isEnum) {
      *m_mainStream << "#endif" << endl;
    }
  }
  // Write anything needed after the closing of the last namespace
  writeAfterNSClose();
  // last thing : close our hashdefine
  QString hashDefine = m_classInfo->packageName.upper().replace("::",  "_");
  if (!hashDefine.isEmpty()) hashDefine.append('_');
  hashDefine.append(m_classInfo->className.upper().simplifyWhiteSpace().replace(QRegExp(" "),  "_"));
  *m_mainStream << "#endif // " << hashDefine << "_HPP" << endl;
  // call upperclass method
  this->CppBaseWriter::finalize();
  return true;
}

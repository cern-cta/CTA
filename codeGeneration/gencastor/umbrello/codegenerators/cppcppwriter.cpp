// Include files 
#include <qregexp.h>

// local
#include "classifierinfo.h"
#include "cppcppwriter.h"

//-----------------------------------------------------------------------------
// Implementation file for class : CppCppWriter
//
// 2004-01-13 : Sebastien Ponce
//-----------------------------------------------------------------------------

//=============================================================================
// Constructor, initializes variables
//=============================================================================
CppCppWriter::CppCppWriter(UMLDoc *parent, const char *name) :
  CppBaseWriter(parent, name) {
    // Allow forward declarations
    m_allowForward = false;
  }

//=============================================================================
// Initialization
//=============================================================================
bool CppCppWriter::init(UMLClassifier* c, QString fileName) {
  // call upper class init
  this->CppBaseWriter::init(c, fileName);
  // includes corresponding header file
  m_includes.insert(QString("\"") +
                    computeFileName(m_class,".h") + ".hpp\"");
  // calls the postinit of this class
  postinit(c, fileName);
  return true;
}

//=============================================================================
// Post Initialization
//=============================================================================
bool CppCppWriter::postinit(UMLClassifier* /*c*/, QString fileName) {
  // Write Header
  QString str;
  str = getHeadingFile(".cpp");
  if(!str.isEmpty()) {
    str.replace(QRegExp("%filename%"),fileName);
    str.replace(QRegExp("%filepath%"),m_file.name());
    *m_mainStream << str << endl;
  }
  return true;
}

//=============================================================================
// Finalization
//=============================================================================
bool CppCppWriter::finalize() {
  // Writes the includes files
  writeIncludesFromSet(*m_mainStream, m_includes);
  // and put the buffer content into the file
  *m_mainStream << endl << m_buffer;
  // call upperclass method
  this->CppBaseWriter::finalize();
  return true;
}

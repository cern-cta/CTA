// Include files
#include <qregexp.h>

// local
#include "cpphstreamcnvwriter.h"

//-----------------------------------------------------------------------------
// Implementation file for class : CppHStreamCnvWriter
//
// 2004-01-13 : Sebastien Ponce
//-----------------------------------------------------------------------------

//=============================================================================
// Standard constructor, initializes variables
//=============================================================================
CppHStreamCnvWriter::CppHStreamCnvWriter(UMLDoc *parent, const char *name) :
  CppHBaseCnvWriter(parent, name) {
  setPrefix("Stream");
}

//=============================================================================
// Initialization
//=============================================================================
bool CppHStreamCnvWriter::init(UMLClassifier* c, QString fileName) {
  // call upper class init
  this->CppBaseWriter::init(c, fileName);
  // fixes the namespace
  m_classInfo->packageName = "castor::io";
  // calls the postinit of this class
  postinit(c, fileName);
  return true;
}

//=============================================================================
// writeClass
//=============================================================================
void CppHStreamCnvWriter::writeClass(UMLClassifier */*c*/) {
  // Generates class declaration
  int ClassIndentLevel = m_indent;
  writeClassDecl(QString("A converter for marshalling/unmarshalling ") +
                 m_classInfo->className + " into/from stl streams");
  // Constructor and methods
  *m_stream << getIndent(INDENT*ClassIndentLevel)
            << scopeToCPPDecl(Uml::Public) << ":" << endl << endl;
  writeMethods();
  // end of class header
  m_indent--;
  *m_stream << getIndent() << "}; // end of class Stream"
            << m_classInfo->className << "Cnv" << endl << endl;
}


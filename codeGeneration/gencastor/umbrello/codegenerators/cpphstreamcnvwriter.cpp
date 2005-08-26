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
  m_classInfo->packageName = s_topNS + "::io";
  // calls the postinit of this class
  postinit(c, fileName);
  return true;
}

//=============================================================================
// writeClass
//=============================================================================
void CppHStreamCnvWriter::writeClass(UMLClassifier* /*c*/) {
  // Generates class declaration
  int ClassIndentLevel = m_indent;
  writeClassDecl(QString("A converter for marshalling/unmarshalling ") +
                 m_classInfo->className + " into/from stl streams");
  // Constructor and methods
  *m_stream << getIndent(INDENT*ClassIndentLevel)
            << scopeToCPPDecl(Uml::Public) << ":" << endl << endl;
  writeMethods(false);
  // marshal methods
  writeMarshal();
  // unmarshal methods
  writeUnmarshal();
  // end of class header
  m_indent--;
  *m_stream << getIndent() << "}; // end of class Stream"
            << m_classInfo->className << "Cnv" << endl << endl;
}

//=============================================================================
// marshal
//=============================================================================
void CppHStreamCnvWriter::writeMarshal() {
  writeDocumentation
    ("Marshals an object using a StreamAddress.",
     QString("If the object is in alreadyDone, just marshal its id.\n") +
     "Otherwise, call createRep and recursively marshal the\n" +
     "refered objects.",
     QString("@param object the object to marshal\n") +
     "@param address the address where to marshal\n" +
     "@param alreadyDone the list of objects already marshalled\n" +
     "@exception Exception throws an Exception in case of error",
     *m_stream);
  QString str = QString("virtual void marshalObject(");
  *m_stream << getIndent()
            << str
            << fixTypeName("IObject*",
                           "castor",
                           m_classInfo->packageName)
            << " object," << endl;
  str.replace(QRegExp("."), " ");
  *m_stream << getIndent() << str
            << fixTypeName("StreamAddress*",
                           "castor::io",
                           m_classInfo->packageName)
            << " address," << endl << getIndent() << str
            << fixTypeName("ObjectSet&",
                           "castor",
                           m_classInfo->packageName)
            << " alreadyDone)"
            << endl << getIndent() << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           m_classInfo->packageName)
            << ");"
            << endl << endl;
}

//=============================================================================
// unmarshal
//=============================================================================
void CppHStreamCnvWriter::writeUnmarshal() {
  writeDocumentation
    ("Unmarshals an object from a StreamAddress.",
     "",
     QString("@param stream the stream from which to unmarshal\n") +
     "@param newlyCreated a set of already created objects\n" +
     "that is used in case of circular dependencies\n" +
     "@return a pointer to the new object. If their was some\n" +
     "memory allocation (creation of a new object), the caller\n" +
     "is responsible for its deallocation\n" +
     "@exception Exception throws an Exception in case of error",
     *m_stream);
  QString str = QString("virtual ") +
    fixTypeName("IObject*",
                "castor",
                m_classInfo->packageName) +
    " unmarshalObject(";
  *m_stream << getIndent()
            << str
            << "castor::io::biniostream& stream," << endl;
  str.replace(QRegExp("."), " ");
  *m_stream << getIndent() << str
            << fixTypeName("ObjectCatalog&",
                           "castor",
                           m_classInfo->packageName)
            << " newlyCreated)"
            << endl << getIndent() << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           m_classInfo->packageName)
            << ");"
            << endl << endl;
}

// Include files
#include <qregexp.h>

// local
#include "cpphbasecnvwriter.h"

//-----------------------------------------------------------------------------
// Implementation file for class : CppHBaseCnvWriter
//
// 2004-01-13 : Sebastien Ponce
//-----------------------------------------------------------------------------

//=============================================================================
// Standard constructor, initializes variables
//=============================================================================
CppHBaseCnvWriter::CppHBaseCnvWriter(UMLDoc *parent, const char *name) :
  CppHWriter(parent, name) {}

//=============================================================================
// writeMethods
//=============================================================================
void CppHBaseCnvWriter::writeMethods (bool delUpMethods) {
  // Constructor and Destructor
  writeDocumentation("", "Constructor", "", *m_stream);
  *m_stream << getIndent() << m_prefix << m_classInfo->className
            << "Cnv("
            << fixTypeName("ICnvSvc*",
                           "castor",
                           m_classInfo->packageName)
            << " cnvSvc);" << endl << endl;
  writeDocumentation("", "Destructor", "", *m_stream);
  *m_stream << getIndent() << "virtual " << "~" << m_prefix
            << m_classInfo->className << "Cnv() throw();" << endl
            << endl;
  // Methods concerning object type
  writeDocumentation
    ("Gets the object type.",
     "That is the type of object this converter can convert",
     "", *m_stream);
  *m_stream << getIndent() << "static const unsigned int ObjType();"
            << endl << endl;
  writeDocumentation
    ("Gets the object type.",
     "That is the type of object this converter can convert",
     "", *m_stream);
  *m_stream << getIndent() << "virtual const unsigned int objType() const;"
            << endl << endl;
  // Conversion methods
  writeDocumentation
    ("Creates foreign representation from a C++ Object.",
     "",
     QString("@param address where to store the representation of\n") +
     "the object\n" +
     "@param object the object to deal with\n" +
     "@param autocommit whether the changes to the database\n" +
     "should be commited or not\n" +
     "@param type if not OBJ_INVALID, the ids representing\n" +
     "the links to objects of this type will not set to 0\n" +
     "as is the default.\n" +
     "@exception Exception throws an Exception in cas of error",
     *m_stream);
  addInclude("\"castor/Constants.hpp\"");
  *m_stream << getIndent()
            << "virtual void createRep("
            << fixTypeName("IAddress*",
                           "castor",
                           m_classInfo->packageName)
            << " address,"
            << endl
            << getIndent()
            << "                       "
            << fixTypeName("IObject*",
                           "castor",
                           m_classInfo->packageName)
            << " object,"
            << endl
            << getIndent()
            << "                       bool autocommit,"
            << endl
            << getIndent()
            << "                       unsigned int type = castor::OBJ_INVALID)"
            << endl
            << getIndent()
            << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           m_classInfo->packageName)
            << ");"
            << endl << endl;
  if (delUpMethods){
    writeDocumentation
      ("Updates foreign representation from a C++ Object.",
       "",
       QString("@param address where the representation of\n") +
       "the object is stored\n" +
       "@param object the object to deal with\n" +
       "@param autocommit whether the changes to the database\n" +
       "should be commited or not\n" +
       "@exception Exception throws an Exception in cas of error",
       *m_stream);
    *m_stream << getIndent()
              << "virtual void updateRep("
              << fixTypeName("IAddress*",
                             "castor",
                             m_classInfo->packageName)
              << " address,"
              << endl
              << getIndent()
              << "                       "
              << fixTypeName("IObject*",
                             "castor",
                             m_classInfo->packageName)
              << " object,"
              << endl
              << getIndent()
              << "                       bool autocommit)"
              << endl
              << getIndent()
              << "  throw ("
              << fixTypeName("Exception",
                             "castor.exception",
                             m_classInfo->packageName)
              << ");"
              << endl << endl;
    writeDocumentation
      ("Deletes foreign representation of a C++ Object.",
       "",
       QString("@param address where the representation of\n") +
       "the object is stored\n" +
       "@param object the object to deal with\n" +
       "@param autocommit whether the changes to the database\n" +
       "should be commited or not\n" +
       "@exception Exception throws an Exception in cas of error",
       *m_stream);
    *m_stream << getIndent()
              << "virtual void deleteRep("
              << fixTypeName("IAddress*",
                             "castor",
                             m_classInfo->packageName)
              << " address,"
              << endl
              << getIndent()
              << "                       "
              << fixTypeName("IObject*",
                             "castor",
                             m_classInfo->packageName)
              << " object,"
              << endl
              << getIndent()
              << "                       bool autocommit)"
              << endl
              << getIndent()
              << "  throw ("
              << fixTypeName("Exception",
                             "castor.exception",
                             m_classInfo->packageName)
              << ");"
              << endl << endl;
  }
  writeDocumentation
    ("Creates C++ object from foreign representation",
     "",
     QString("@param address the place where to find the foreign\n") +
     "representation\n" +
     "@return the C++ object created from its reprensentation\n" +
     "or 0 if unsuccessful. Note that the caller is responsible\n" +
     "for the deallocation of the newly created object\n" +
     "@exception Exception throws an Exception in cas of error",
     *m_stream);
  *m_stream << getIndent()
            << "virtual "
            << fixTypeName("IObject*",
                           "castor",
                           m_classInfo->packageName)
            << " createObj("
            << fixTypeName("IAddress",
                           "castor",
                           m_classInfo->packageName)
            << "* address)"
            << endl << getIndent() << "  throw ("
            << fixTypeName("Exception",
                           "castor.exception",
                           m_classInfo->packageName)
            << ");"
            << endl << endl;
  if (delUpMethods) {
    writeDocumentation
      ("Updates C++ object from its foreign representation.",
       "",
       QString("@param obj the object to deal with\n") +
       "@exception Exception throws an Exception in cas of error",
       *m_stream);
    *m_stream << getIndent()
              << "virtual void updateObj("
              << fixTypeName("IObject*",
                             "castor",
                             m_classInfo->packageName)
              << " obj)"
              << endl
              << getIndent()
              << "  throw ("
              << fixTypeName("Exception",
                             "castor.exception",
                             m_classInfo->packageName)
              << ");"
              << endl << endl;
  }
}

//=============================================================================
// writeClassDecl
//=============================================================================
void CppHBaseCnvWriter::writeClassDecl(const QString& doc) {
  // write documentation for class
  *m_stream << getIndent() << "/**"<<endl
            << getIndent() <<" * class " << m_prefix
            << m_classInfo->className << "Cnv" << endl
            << formatDoc (doc, getIndent() + " * ")
            << getIndent()<<" */" << endl
            << getIndent() << "class " << m_prefix << m_classInfo->className
            << "Cnv : public " << m_prefix
            << "BaseCnv {" << endl << endl;
  m_indent++;
  // Take care to include the base class declaration
  fixTypeName(m_prefix + "BaseCnv",
              m_classInfo->packageName,
              m_classInfo->packageName);
}


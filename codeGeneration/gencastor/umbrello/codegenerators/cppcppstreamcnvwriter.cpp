// Include files
#include <qmap.h>
#include <qregexp.h>

// local
#include "cppcppstreamcnvwriter.h"

//-----------------------------------------------------------------------------
// Implementation file for class : CppCppStreamCnvWriter
//
// 2004-01-13 : Sebastien Ponce
//-----------------------------------------------------------------------------

//=============================================================================
// Standard constructor, initializes variables
//=============================================================================
CppCppStreamCnvWriter::CppCppStreamCnvWriter(UMLDoc *parent, const char *name) :
  CppCppBaseCnvWriter(parent, name) {
  setPrefix("Stream");
}

//=============================================================================
// Initialization
//=============================================================================
bool CppCppStreamCnvWriter::init(UMLClassifier* c, QString fileName) {
  // call upper class init
  this->CppBaseWriter::init(c, fileName);
  // fixes the namespace
  m_originalPackage = m_classInfo->fullPackageName;
  m_classInfo->packageName = "castor::io";
  m_classInfo->fullPackageName = "castor::io::";
  // includes converter  header file and object header file
  m_includes.insert(QString("\"Stream") + m_classInfo->className + "Cnv.hpp\"");
  m_includes.insert(QString("\"") +
                    findFileName(m_class,".h") + ".hpp\"");
  // calls the postinit of this class
  postinit(c, fileName);
  return true;
}

//=============================================================================
// writeClass
//=============================================================================
void CppCppStreamCnvWriter::writeClass(UMLClassifier */*c*/) {
  // static factory declaration
  writeFactory();
  // constructor and destructor
  writeConstructors();
  // objtype methods
  writeObjType();
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
  // marshal methods
  writeMarshal();
  // unmarshal methods
  writeUnmarshal();
}

//=============================================================================
// writeConstructors
//=============================================================================
void CppCppStreamCnvWriter::writeConstructors() {
  // constructor
  writeWideHeaderComment("Constructor", getIndent(), *m_stream);
  *m_stream << getIndent() << m_classInfo->fullPackageName
            << "Stream" << m_classInfo->className << "Cnv::Stream"
            << m_classInfo->className << "Cnv() :" << endl
            << getIndent() << "  StreamBaseCnv() {}"
            << endl << endl;
  // Destructor
  writeWideHeaderComment("Destructor", getIndent(), *m_stream);
  *m_stream << getIndent() << m_classInfo->fullPackageName
            << "Stream" << m_classInfo->className << "Cnv::~Stream"
            << m_classInfo->className << "Cnv() throw() {" << endl;
  m_indent++;
  m_indent--;
  *m_stream << "}" << endl << endl;
}

//=============================================================================
// writeCreateRepContent
//=============================================================================
void CppCppStreamCnvWriter::writeCreateRepContent() {
  // Get the precise address
  *m_stream << getIndent() << fixTypeName("StreamAddress",
                                          "castor::io",
                                          m_classInfo->packageName)
            << "* ad = " << endl
            << getIndent() << "  dynamic_cast<"
            << fixTypeName("StreamAddress",
                           "castor::io",
                           m_classInfo->packageName)
            << "*>(address);"
            << endl;
  // Save the type
  *m_stream << getIndent()
            << "ad->stream() << obj->type();" << endl;
  // create a list of members to be saved
  MemberList members = createMembersList();
  // Go through the members
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    *m_stream << getIndent() << "ad->stream()";
    if (mem->second.find('[') > 0) {
      QString sl = mem->second;
      sl = sl.left(sl.find(']'));
      sl = sl.right(sl.length() - sl.find('[') - 1);
      int l = atoi(sl.ascii());
      for (int i = 0; i < l; i++) {
        *m_stream << " << obj->" << mem->first << "()[" << i << "]";
      }
    } else {
      *m_stream << " << obj->" << mem->first << "()";
    }
    *m_stream << ";" << endl;
  }
  // create a list of associations
  AssocList assocs = createAssocsList();
  // Go through the associations for the enums
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE && isEnum(as->second.second)) {
      *m_stream << getIndent() << "ad->stream() << obj->"
                << as->second.first << "();" << endl;
    }
  }
}

//=============================================================================
// writeUpdateRepContent
//=============================================================================
void CppCppStreamCnvWriter::writeUpdateRepContent() {
  // Get the precise address
  *m_stream << getIndent() << fixTypeName("Internal",
                                          "castor.exception",
                                          m_classInfo->packageName)
            << " ex;" << endl << getIndent()
            << "ex.getMessage() << "
            << "\"Cannot update representation in case of streaming.\""
            << endl << getIndent()
            << "                << std::endl;"
            << endl << getIndent()
            << "throw ex;" << endl;
}

//=============================================================================
// writeDeleteRepContent
//=============================================================================
void CppCppStreamCnvWriter::writeDeleteRepContent() {
  // Get the precise address
  *m_stream << getIndent() << fixTypeName("Internal",
                                          "castor.exception",
                                          m_classInfo->packageName)
            << " ex;" << endl << getIndent()
            << "ex.getMessage() << "
            << "\"Cannot delete representation in case of streaming.\""
            << endl << getIndent()
            << "                << std::endl;"
            << endl << getIndent()
            << "throw ex;" << endl;
}

//=============================================================================
// writeCreateObjContent
//=============================================================================
void CppCppStreamCnvWriter::writeCreateObjContent() {
  // Get the precise address
  *m_stream << getIndent() << fixTypeName("StreamAddress",
                                          "castor::io",
                                          m_classInfo->packageName)
            << "* ad = " << endl
            << getIndent() << "  dynamic_cast<"
            << fixTypeName("StreamAddress",
                           "castor::io",
                           m_classInfo->packageName)
            << "*>(address);"
            << endl;
  // Creates the new object
  *m_stream << getIndent()
            << "// create the new Object" << endl
            << getIndent() << m_originalPackage
            << m_classInfo->className << "* object = new "
            << m_originalPackage << m_classInfo->className
            << "();" << endl << getIndent()
            << "// Now retrieve and set members" << endl;
  // create a list of members to be saved
  MemberList members = createMembersList();
  // Go through the members
  for (Member* mem = members.first();
       0 != mem;
       mem = members.next()) {
    *m_stream << getIndent()
              << fixTypeName(mem->second,
                             getNamespace(mem->second),
                             m_classInfo->packageName)
              << " " << mem->first;
    if (isLastTypeArray()) *m_stream << arrayPart();
    *m_stream << ";" << endl
              << getIndent() << "ad->stream()";
    if (mem->second.find('[') > 0) {
      QString sl = mem->second;
      sl = sl.left(sl.find(']'));
      sl = sl.right(sl.length() - sl.find('[') - 1);
      int l = atoi(sl.ascii());
      for (int i = 0; i < l; i++) {
        *m_stream << " >> " << mem->first << "[" << i << "]";
      }
    } else {
      *m_stream << " >> " << mem->first;
    }
    *m_stream << ";" << endl
              << getIndent() << "object->set"
              << capitalizeFirstLetter(mem->first)
              << "(";
    if (isLastTypeArray()) {
      *m_stream << "("
                << fixTypeName(mem->second,
                               getNamespace(mem->second),
                               m_classInfo->packageName)
                << "*)";
    }
    *m_stream << mem->first << ");" << endl;
  }
  // create a list of associations
  AssocList assocs = createAssocsList();
  // Go through the one to one associations for enums
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE && isEnum(as->second.second)) {
      *m_stream << getIndent() << "int "
                << as->second.first << ";" << endl
                << getIndent() << "ad->stream() >> "
                << as->second.first << ";" << endl
                << getIndent() << "object->set"
                << capitalizeFirstLetter(as->second.first)
                << "((" << fixTypeName(as->second.second,
                                       getNamespace(as->second.second),
                                       m_classInfo->packageName)
                << ")" << as->second.first << ");" << endl;
    }
  }
  // Return result
  *m_stream << getIndent() << "return object;" << endl;
}

//=============================================================================
// writeUpdateObjContent
//=============================================================================
void CppCppStreamCnvWriter::writeUpdateObjContent() {
  // Get the precise address
  *m_stream << getIndent() << fixTypeName("Internal",
                                          "castor.exception",
                                          m_classInfo->packageName)
            << " ex;" << endl << getIndent()
            << "ex.getMessage() << "
            << "\"Cannot update object in case of streaming.\""
            << endl << getIndent()
            << "                << std::endl;"
            << endl << getIndent()
            << "throw ex;" << endl;
}

//=============================================================================
// marshal
//=============================================================================
void CppCppStreamCnvWriter::writeMarshal() {
  writeWideHeaderComment("marshalObject", getIndent(), *m_stream);
  QString str = QString("void ") +
    m_classInfo->packageName + "::Stream" +
    m_classInfo->className + "Cnv::marshalObject(";
  *m_stream << getIndent()
            << str
            << fixTypeName("IObject*", "castor", "")
            << " object," << endl;
  str.replace(QRegExp("."), " ");
  *m_stream << getIndent() << str
            << fixTypeName("StreamAddress*", "castor::io", "")
            << " address," << endl << getIndent() << str
            << fixTypeName("ObjectSet&", "castor", m_classInfo->packageName)
            << " alreadyDone)"
            << endl << getIndent() << "  throw ("
            << fixTypeName("Exception", "castor.exception", "")
            << ") {" << endl;
  m_indent++;
  // Get the precise object
  *m_stream << getIndent() << m_originalPackage
            << m_classInfo->className << "* obj = " << endl
            << getIndent() << "  dynamic_cast<"
            << m_originalPackage
            << m_classInfo->className << "*>(object);"
            << endl;
  *m_stream << getIndent()
            << "if (0 == obj) {" << endl;
  m_indent++;
    addInclude("\"castor/Constants.hpp\"");
  *m_stream << getIndent()
            << "// Case of a null pointer"
            << endl << getIndent()
            << "address->stream() << castor::OBJ_Ptr << 0;"
            << endl;
  m_indent--;
  *m_stream << getIndent()
            << "} else if (alreadyDone.find(obj) == alreadyDone.end()) {"
            << endl;
  m_indent++;
  fixTypeName("StreamCnvSvc", "castor::io", m_classInfo->packageName);
  *m_stream << getIndent()
            << "// Case of a pointer to a non streamed object"
            << endl << getIndent()
            << "cnvSvc()->createRep(address, obj, true);"
            << endl;
  // Mark object as done
  *m_stream << getIndent() << "// Mark object as done"
            << endl << getIndent()
            << "alreadyDone.insert(obj);"
            << endl;
  // create a list of associations
  AssocList assocs = createAssocsList();
  // Go through the associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE) {
      // don't consider enums
      if (isEnum(as->second.second)) continue;
      // One to one association
      fixTypeName(as->second.second,
                  getNamespace(as->second.second),
                  m_classInfo->packageName);
      fixTypeName("StreamCnvSvc", "castor::io", m_classInfo->packageName);
      *m_stream << getIndent()
                << "cnvSvc()->marshalObject(obj->"
                << as->second.first
                << "(), address, alreadyDone);"
                << endl;
    } else if (as->first.first == MULT_N) {
      // One to n association
      *m_stream << getIndent() << "address->stream() << obj->"
                << as->second.first << "().size();"
                << endl << getIndent() << "for ("
                << fixTypeName("vector", "", "")
                << "<"
                << fixTypeName(as->second.second,
                               getNamespace(as->second.second),
                               m_classInfo->packageName)
                << "*>::iterator it = obj->"
                << as->second.first
                << "().begin();" << endl << getIndent()
                << "     it != obj->"
                << as->second.first
                << "().end();" << endl << getIndent()
                << "     it++) {"  << endl;
      m_indent++;
      fixTypeName("StreamCnvSvc", "castor::io", m_classInfo->packageName);
      *m_stream << getIndent()
                << "cnvSvc()->marshalObject(*it, address, alreadyDone);"
                << endl;
      m_indent--;
      *m_stream << getIndent() << "}" << endl;
    }
  }
  m_indent--;
  *m_stream << getIndent()
            << "} else {" << endl;
  m_indent++;
  addInclude("\"castor/Constants.hpp\"");
  *m_stream << getIndent()
            << "// case of a pointer to an already streamed object"
            << endl << getIndent()
            << "address->stream() << castor::OBJ_Ptr << alreadyDone[obj];"
            << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl;
  m_indent--;
  *m_stream << getIndent() << "}" << endl << endl;
}

//=============================================================================
// unmarshal
//=============================================================================
void CppCppStreamCnvWriter::writeUnmarshal() {
  writeWideHeaderComment("unmarshalObject", getIndent(), *m_stream);
  QString str = fixTypeName("IObject*",
                            "castor",
                            m_classInfo->packageName) +
    " " + m_classInfo->packageName + "::Stream" +
    m_classInfo->className + "Cnv::unmarshalObject(";
  *m_stream << getIndent()
            << str
            << "castor::io::biniostream& stream," << endl;
  str.replace(QRegExp("."), " ");
  *m_stream << getIndent() << str
            << fixTypeName("ObjectCatalog&", "castor", "")
            << " newlyCreated)"
            << endl << getIndent() << "  throw ("
            << fixTypeName("Exception", "castor.exception", "")
            << ") {" << endl;
  m_indent++;
  fixTypeName("StreamCnvSvc", "castor::io", m_classInfo->packageName);
  *m_stream << getIndent()
            << fixTypeName("StreamAddress", "castor::io", "")
            << " ad(stream, \"StreamCnvSvc\", SVC_STREAMCNV);"
            << endl << getIndent()
            << fixTypeName("IObject*",
                           "castor",
                           m_classInfo->packageName)
            << " object = cnvSvc()->createObj(&ad);"
            << endl;
  // Add new object to the list of newly created objects
  *m_stream << getIndent() << "// Mark object as created"
            << endl << getIndent()
            << "newlyCreated.insert(object);"
            << endl;
  // create a list of associations
  AssocList assocs = createAssocsList();
  bool first = true;
  // Go through the associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (first &&
        ((as->first.first == MULT_ONE &&
          !isEnum(as->second.second)) ||
         as->first.first == MULT_N)) {
      // Get the precise object
      *m_stream << getIndent() << "// Fill object with associations"
                << endl << getIndent() << m_originalPackage
                << m_classInfo->className << "* obj = " << endl
                << getIndent() << "  dynamic_cast<"
                << m_originalPackage
                << m_classInfo->className << "*>(object);"
                << endl;
      first = false;
    }
    if (as->first.first == MULT_ONE &&
        !isEnum(as->second.second)) {
      fixTypeName("StreamCnvSvc", "castor::io", m_classInfo->packageName);
      *m_stream << getIndent()
                << "IObject* obj"
                << capitalizeFirstLetter(as->second.first)
                << " = cnvSvc()->unmarshalObject(ad, newlyCreated);"
                << endl << getIndent()
                << "obj->set"
                << capitalizeFirstLetter(as->second.first)
                << "(dynamic_cast<"
                << fixTypeName(as->second.second,
                               getNamespace(as->second.second),
                               m_classInfo->packageName)
                << "*>(obj"
                << capitalizeFirstLetter(as->second.first)
                << "));" << endl;
    } else if (as->first.first == MULT_N) {
      *m_stream << getIndent() << "unsigned int "
                << as->second.first << "Nb;" << endl
                << getIndent() << "ad.stream() >> "
                << as->second.first << "Nb;" << endl
                << getIndent()
                << "for (unsigned int i = 0; i < "
                << as->second.first << "Nb; i++) {" << endl;
      m_indent++;
      fixTypeName("StreamCnvSvc", "castor::io", m_classInfo->packageName);
      *m_stream << getIndent()
                << "IObject* obj"
                << capitalizeFirstLetter(as->second.first)
                << " = cnvSvc()->unmarshalObject(ad, newlyCreated);"
                << endl << getIndent()
                << "obj->add"
                << capitalizeFirstLetter(as->second.first)
                << "(dynamic_cast<"
                << fixTypeName(as->second.second,
                               getNamespace(as->second.second),
                               m_classInfo->packageName)
                << "*>(obj"
                << capitalizeFirstLetter(as->second.first)
                << "));" << endl;
      m_indent--;
      *m_stream << getIndent() << "}" << endl;
    }
  }
  m_indent--;
  *m_stream << getIndent() << "}" << endl << endl;
}

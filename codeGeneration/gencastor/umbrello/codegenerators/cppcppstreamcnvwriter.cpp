// Include files
#include <qmap.h>

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
  // extract the blobs and their lengths
  QMap<QString,QString> blobs = extractBlobsFromMembers(members);
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
  // Mark object as done
  *m_stream << getIndent() << "// Mark object as done"
            << endl << getIndent()
            << "alreadyDone.insert(obj);"
            << endl;
  // Go through the associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    fixTypeName("StreamCnvSvc", "castor::io", m_classInfo->packageName);
    addInclude("\"castor/Constants.hpp\"");
    if (as->first.first == MULT_ONE) {
      // don't consider enums
      if (isEnum(as->second.second)) continue;
      // One to one association
      fixTypeName(as->second.second,
                  getNamespace(as->second.second),
                  m_classInfo->packageName);
      *m_stream << getIndent()
                << "marshalObject(obj->"
                << as->second.first
                << "(), ad, alreadyDone);"
                << endl;
    } else if (as->first.first == MULT_N) {
      // One to n association
      *m_stream << getIndent() << "ad->stream() << obj->"
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
      *m_stream << getIndent()
                << "marshalObject(*it, ad, alreadyDone);"
                << endl;
      m_indent--;
      *m_stream << getIndent() << "}" << endl;
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
  // extract the blobs and their lengths
  QMap<QString,QString> blobs = extractBlobsFromMembers(members);
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
		<< "(("	<< fixTypeName(as->second.second,
				       getNamespace(as->second.second),
				       m_classInfo->packageName)
		<< ")" << as->second.first << ");" << endl;
    }
  }
  // Add new object to the list of newly created objects
  *m_stream << getIndent()
            << "newlyCreated.insert(object);"
            << endl;
  // Go through the associations
  for (Assoc* as = assocs.first();
       0 != as;
       as = assocs.next()) {
    if (as->first.first == MULT_ONE) {
      if (!isEnum(as->second.second)) {    
	*m_stream << getIndent()
		  << "IObject* obj"
		  << capitalizeFirstLetter(as->second.first)
		  << " = unmarshalObject(ad->stream(), newlyCreated);"
		  << endl << getIndent()
		  << "object->set"
		  << capitalizeFirstLetter(as->second.first)
		  << "(dynamic_cast<"
		  << fixTypeName(as->second.second,
				 getNamespace(as->second.second),
				 m_classInfo->packageName)
		  << "*>(obj"
		  << capitalizeFirstLetter(as->second.first)
		  << "));" << endl;
      }
    } else if (as->first.first == MULT_N) {
      *m_stream << getIndent() << "unsigned int "
                << as->second.first << "Nb;" << endl
                << getIndent() << "ad->stream() >> "
                << as->second.first << "Nb;" << endl
                << getIndent()
                << "for (unsigned int i = 0; i < "
                << as->second.first << "Nb; i++) {" << endl;
      m_indent++;
      *m_stream << getIndent()
                << "IObject* obj"
                << " = unmarshalObject(ad->stream(), newlyCreated);"
                << endl << getIndent()
                << "object->add"
                << capitalizeFirstLetter(as->second.first)
                << "(dynamic_cast<"
                << fixTypeName(as->second.second,
                               getNamespace(as->second.second),
                               m_classInfo->packageName)
                << "*>(obj));" << endl;
      m_indent--;
      *m_stream << getIndent() << "}" << endl;
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


// Include files
#include <qstring.h>
#include <qvaluelist.h>
#include <../class.h>
#include <../interface.h>

// local
#include "cppcppclasswriter.h"

//-----------------------------------------------------------------------------
// Implementation file for class : CppCppClassWriter
//
// 2004-01-13 : Sebastien Ponce
//-----------------------------------------------------------------------------

//=============================================================================
// Standard constructor, initializes variables
//=============================================================================
CppCppClassWriter::CppCppClassWriter(UMLDoc *parent, const char *name) :
  CppCppWriter(parent, name) {
  m_methodImplementations[std::pair<QString, int>(QString("id const"),0)] =
    &CppCppClassWriter::writeId;
  m_methodImplementations[std::pair<QString, int>(QString("setId"),1)] =
    &CppCppClassWriter::writeSetId;
  m_methodImplementations[std::pair<QString, int>(QString("type const"),0)] =
    &CppCppClassWriter::writeType;
  m_methodImplementations[std::pair<QString, int>(QString("TYPE"),0)] =
    &CppCppClassWriter::writeTYPE;
  m_methodImplementations[std::pair<QString, int>(QString("clone"),0)] =
    &CppCppClassWriter::writeClone;
  m_methodImplementations[std::pair<QString, int>(QString("print const"),3)] =
    &CppCppClassWriter::writeFullPrint;
  m_methodImplementations[std::pair<QString, int>(QString("print const"),0)] =
    &CppCppClassWriter::writePrint;
  m_castorPrintImplementations["Cuuid_t"] =
    &CppCppClassWriter::writeCuuidPrint;
  m_castorPrintImplementations["StringResponse"] =
    &CppCppClassWriter::writeStringResponsePrint;
}

//=============================================================================
// writeClass
//=============================================================================
void CppCppClassWriter::writeClass(UMLClassifier *c) {
  if (m_classInfo->isEnum) {
    // Deal With enum
    UMLClass* k = dynamic_cast<UMLClass*>(c);
    QPtrList<UMLAttribute>* atl = k->getFilteredAttributeList();
    writeWideHeaderComment
      (m_classInfo->className + "Strings", getIndent(), *m_stream);
    *m_stream << getIndent() << "const char* "
              << m_classInfo->fullPackageName
              << m_classInfo->className << "Strings["
              << atl->count() << "] = {" << endl;
    m_indent++;
    for (UMLAttribute *at=atl->first(); at ; ) {
      QString attrName = at->getName();
      *m_stream << getIndent() << "\"" << attrName << "\"";
      UMLAttribute *next = atl->next();
      bool isLast = next == 0;
      if (!isLast) *m_stream << "," << endl;
      at = next;
    }
    *m_stream << endl;
    m_indent--;
    *m_stream << getIndent() << "};" << endl << endl;
    return;
  }
  // Deal with actual class, starting with Constructors
  if(!m_classInfo->isInterface) {
    writeConstructorMethods(*m_stream);
  }
  // Operation methods
  QValueList<std::pair<QString, int> > alreadyGenerated;
  writeOperations(c,false,Uml::Public,*m_stream, alreadyGenerated);
  writeOperations(c,false,Uml::Protected,*m_stream, alreadyGenerated);
  writeOperations(c,false,Uml::Private,*m_stream, alreadyGenerated);
}

//=============================================================================
// writeConstructorMethods
//=============================================================================
void CppCppClassWriter::writeConstructorMethods(QTextStream &stream) {
  // write constructor
  writeWideHeaderComment("Constructor", getIndent(), stream);
  stream << getIndent()<< m_classInfo->fullPackageName
         << m_classInfo->className << "::"
         << m_classInfo->className << "() throw()";
  bool first = true;
  // Initializes the base classes
  for (UMLClassifier *superClass = m_classInfo->superclasses.first();
       0 != superClass;
       superClass = m_classInfo->superclasses.next()) {
    writeInitInConstructor(superClass->getName(), "void", first);
  }
  // Try to put sensible default values for the members
  // First public, then protected and finally private ones
  for(UMLAttribute *at = m_classInfo->atpub.first();
      0 != at;
      at = m_classInfo->atpub.next()) {
    writeInitInConstructor(QString("m_") + at->getName(),
                           at->getTypeName(), first);
  }
  for(UMLAttribute *at = m_classInfo->atprot.first();
      0 != at;
      at = m_classInfo->atprot.next()) {
    writeInitInConstructor(QString("m_") + at->getName(),
                           at->getTypeName(), first);
  }
  for(UMLAttribute *at = m_classInfo->atpriv.first();
      0 != at;
      at = m_classInfo->atpriv.next()) {
    writeInitInConstructor(QString("m_") + at->getName(),
                           at->getTypeName(), first);
  }
  // Put default values for associations. Always 0 since there are pointers
  for(UMLAssociation *a = m_classInfo->plainAssociations.first();
      0 != a;
      a = m_classInfo->plainAssociations.next()) {
    writeAssocInitInConstructor(a, first);
  }
  for(UMLAssociation *a = m_classInfo->aggregations.first();
      0 != a;
      a = m_classInfo->aggregations.next()) {
    writeAssocInitInConstructor(a, first);
  }
  for(UMLAssociation *a = m_classInfo->compositions.first();
      0 != a;
      a = m_classInfo->compositions.next()) {
    writeAssocInitInConstructor(a, first);
  }
  stream << " {" << endl;
  m_indent++;
  // For arrays, call memset
  for(UMLAttribute *at = m_classInfo->getAttList()->first();
      0 != at;
      at = m_classInfo->getAttList()->next()) {
    if (!at->getStatic()) {
      QString type = fixTypeName (at->getTypeName(),"","");
      if (isLastTypeArray()) {
        QString length = arrayLength();
        stream << getIndent() << "memset("
               << "m_" << at->getName()
               << ", 0, " << length << " * sizeof("
               << type << "));" << endl;
      }
    }
  }
  m_indent--;
  stream << "};" << endl << endl;
  // write destructor
  writeWideHeaderComment("Destructor", getIndent(), stream);
  stream << getIndent() << m_classInfo->fullPackageName
         << m_classInfo->className << "::~"
         << m_classInfo->className << "() throw() {";
  // there are some associations to delete
  stream << endl;
  m_indent++;
  for(UMLAssociation *a = m_classInfo->plainAssociations.first();
      0 != a;
      a = m_classInfo->plainAssociations.next()) {
    writeAssocDeleteInDestructor(a);
  }
  for(UMLAssociation *a = m_classInfo->aggregations.first();
      0 != a;
      a = m_classInfo->aggregations.next()) {
    writeAssocDeleteInDestructor(a);
  }
  for(UMLAssociation *a = m_classInfo->compositions.first();
      0 != a;
      a = m_classInfo->compositions.next()) {
    writeAssocDeleteInDestructor(a);
  }
  m_indent--;
  stream << "};" << endl << endl;
}


//=============================================================================
// writeInitInConstructor
//=============================================================================
void CppCppClassWriter::writeInitInConstructor(QString name,
                                               QString type,
                                               bool& first) {
  QString content;
  type = type.replace("unsigned", "");
  type = type.stripWhiteSpace();
  if (type.contains("*") || (type == "int") ||
      (type == "long") || (type == "short")) {
    content = "0";
  } else if ((type == "float") || (type == "double")) {
    content = "0.0";
  } else if (type == "void") {
  } else if (type[type.length()-1] == ']') {
    // Nothing for arrays
  } else if (type == "bool") {
    content = "false";
  } else if (type == "string") {
    content = "\"\"";
  } else if (m_castorTypes.find(type) != m_castorTypes.end()) {
  } else if (m_castorTypes.find(type.left(type.length()-2)) !=
             m_castorTypes.end()) {
    // The _t has may have to be removed for looking in castor type !
  } else if (isEnum(type)) {
    content = type + "(0)";
  } else {
    content = QString("AIE : ") + type;
  }
  if (first) {
    *m_stream << " :";
    first = false;
  } else {
    *m_stream << ",";
  }
  *m_stream << endl << getIndent() << "  " << name
            << "(" << content << ")";
}

//=============================================================================
// writeAssocInitInConstructor
//=============================================================================
void CppCppClassWriter::writeAssocInitInConstructor (UMLAssociation *a,
                                                     bool& first) {
  if (m_classInfo->id() == a->getRoleId(A) ||
      m_classInfo->allSuperclassIds.contains(a->getRoleId(A))) {
    if (parseMulti(a->getMulti(B)) == MULT_ONE) {
      QString className = a->getObject(B)->getName();
      if (!isEnum(className)) className.append("*");
      writeInitInConstructor (QString("m_") + a->getRoleName(B),
                              className,
                              first);
    }
  } else {
    if (parseMulti(a->getMulti(A)) == MULT_ONE) {
      QString className = a->getObject(A)->getName();
      if (!isEnum(className)) className.append("*");
      writeInitInConstructor (QString("m_") + a->getRoleName(A),
                              className,
                              first);
    }
  }
}

//=============================================================================
// writeDeleteInDestructor
//=============================================================================
void CppCppClassWriter::writeDeleteInDestructor(QString name,
                                                QString backName,
                                                Multiplicity multi,
                                                Multiplicity backMulti,
                                                bool fulldelete) {
  switch (multi) {
  case MULT_N :
    *m_stream << getIndent()
              << "for (unsigned int i = 0; i < m_"
              << name << "Vector.size(); i++) {" << endl;
    m_indent++;
    switch (backMulti) {
    case MULT_N :
      *m_stream << getIndent() << "m_" << name
                << "Vector[i]->remove"
                << capitalizeFirstLetter(backName)
                << "(this);" << endl;
      break;
    case MULT_ONE :
      *m_stream << getIndent() << "m_" << name
                << "Vector[i]->set"
                << capitalizeFirstLetter(backName)
                << "(0);" << endl;
      if (fulldelete) {
        *m_stream << getIndent()
                  << "delete m_" << name
                  << "Vector[i];" << endl;
      }
      break;
    case MULT_UNKNOWN :
      break;
    }
    m_indent--;
    *m_stream << getIndent() << "}" << endl << getIndent()
              << "m_" << name << "Vector.clear();" << endl;
    break;
  case MULT_ONE :
    *m_stream << getIndent() << "if (0 != m_" << name
              << ") {" << endl;
    m_indent++;
    switch (backMulti) {
    case MULT_N :
      *m_stream << getIndent() << "m_" << name
                << "->remove"
                << capitalizeFirstLetter(backName)
                << "(this);" << endl;
      break;
    case MULT_ONE :
      *m_stream << getIndent() << "m_" << name << "->set"
                << capitalizeFirstLetter(backName)
                << "(0);" << endl;
      break;
    case MULT_UNKNOWN :
      break;
    }
    if (fulldelete) {
      *m_stream << getIndent() << "delete m_" << name << ";"
                << endl << getIndent() << "m_" << name
                << " = 0;" << endl;
    }
    m_indent--;
    *m_stream << getIndent() << "}" << endl;
    break;
  case MULT_UNKNOWN :
    break;
  }
}

//=============================================================================
// writeAssocDeleteInDestructor
//=============================================================================
void CppCppClassWriter::writeAssocDeleteInDestructor(UMLAssociation *a) {
  Multiplicity multA = parseMulti(a->getMulti(A));
  Multiplicity multB = parseMulti(a->getMulti(B));
  if (a->getRoleName(B) != "" && a->getRoleName(A) != "") {
    if (m_classInfo->id() == a->getRoleId(A) ||
        m_classInfo->allSuperclassIds.contains(a->getRoleId(A))) {
      UMLClass* cl = dynamic_cast<UMLClass*>(a->getObject(B));
      if (0 == cl || !cl->isEnumeration()) {
        fixTypeName(a->getObject(B)->getName(),
                    getNamespace(a->getObject(B)->getName()),
                    m_classInfo->packageName);
        writeDeleteInDestructor
          (a->getRoleName(B), a->getRoleName(A),
           multB, multA,
           a->getAssocType() == Uml::at_Composition);
      }
    } else {
      UMLClass* cl = dynamic_cast<UMLClass*>(a->getObject(A));
      if (0 == cl || !cl->isEnumeration()) {
        fixTypeName(a->getObject(A)->getName(),
                    getNamespace(a->getObject(A)->getName()),
                    m_classInfo->packageName);
        writeDeleteInDestructor
          (a->getRoleName(A), a->getRoleName(B),
           multA, multB, false);
      }
    }
  }
}

//=============================================================================
// writeId
//=============================================================================
void CppCppClassWriter::writeId(CppBaseWriter* obj,
                                QTextStream &stream) {
  stream << obj->getIndent() << "return m_id;" << endl;
}

//=============================================================================
// writeSetId
//=============================================================================
void CppCppClassWriter::writeSetId(CppBaseWriter* obj,
                                   QTextStream &stream) {
  stream << obj->getIndent() << "m_id = id;" << endl;
}

//=============================================================================
// writeType
//=============================================================================
void CppCppClassWriter::writeType(CppBaseWriter* obj,
                                  QTextStream &stream) {
  stream << obj->getIndent() << "return TYPE();" << endl;
}

//=============================================================================
// writeClone
//=============================================================================
void CppCppClassWriter::writeClone(CppBaseWriter* obj,
                                   QTextStream &stream) {
  stream << obj->getIndent() << "return this;" << endl;
}

//=============================================================================
// writeTYPE
//=============================================================================
void CppCppClassWriter::writeTYPE(CppBaseWriter* obj,
                                  QTextStream &stream) {
  obj->addInclude("\"castor/Constants.hpp\"");
  stream << obj->getIndent() << "return OBJ_"
         << obj->classInfo()->className << ";" << endl;
}

//=============================================================================
// writePrint
//=============================================================================
void CppCppClassWriter::writePrint(CppBaseWriter* obj,
                                   QTextStream &stream) {
  stream << obj->getIndent()
         << obj->fixTypeName("ObjectSet",
                             obj->getNamespace("ObjectSet"),
                             obj->classInfo()->packageName)
         << " alreadyPrinted;" << endl  << obj->getIndent()
         << "print(std::cout, \"\", alreadyPrinted);"
         << endl;
  obj->addInclude("<iostream>");
}

//=============================================================================
// writeFullPrint
//=============================================================================
void CppCppClassWriter::writeFullPrint(CppBaseWriter* obj,
                                       QTextStream &stream) {
  // Look whether something is defined
  if (obj->m_castorPrintImplementations.find(obj->classInfo()->className) !=
      obj->m_castorPrintImplementations.end()) {
    (*(obj->m_castorPrintImplementations[obj->classInfo()->className]))
      (obj, stream);
    return;
  }
  // First the object type
  stream << obj->getIndent() << "stream << indent << \"[# "
         << obj->classInfo()->className
         << " #]\" << std::endl;" << endl;
  // Deal with circular dependencies
  stream << obj->getIndent()
         << "if (alreadyPrinted.find(this) != alreadyPrinted.end()) {"
         << endl;
  obj->incIndent();
  stream << obj->getIndent()
         << "// Circular dependency, this object was already printed"
         << endl << obj->getIndent()
         << "stream << indent << \"Back pointer, see above\" << std::endl;"
         << endl << obj->getIndent() << "return;" << endl;
  obj->decIndent();
  stream << obj->getIndent() << "}" << endl;
  // Calls the parent's method
  UMLClassifier *superClass =
    obj->classInfo()->superclasses.first();
  if (superClass != 0) {
    stream << obj->getIndent()
           << "// Call print on the parent class(es)" << endl;
  }
  while (superClass != 0) {
    stream << obj->getIndent() << "this->"
           << obj->fixTypeName(superClass->getName(),
                               obj->getNamespace(superClass->getName()),
                               obj->classInfo()->packageName)
           << "::print(stream, indent, alreadyPrinted);" << endl;
    superClass = obj->classInfo()->superclasses.next();
  }
  // Outputs the members
  QPtrList<UMLAttribute>* members = obj->classInfo()->getAttList();
  if (members->count() > 0) {
    stream << obj->getIndent()
           << "// Output of all members" << endl;
  }
  for(UMLAttribute *at = members->first();
      0 != at;
      at = members->next()) {
    if (at->getName() == "content") {
      // This probably means we deal with a castor type
      // Then try to get the dedicated
      if (obj->m_castorPrintImplementations.find(at->getTypeName()) !=
          obj->m_castorPrintImplementations.end()) {
        (*(obj->m_castorPrintImplementations[at->getTypeName()]))(obj, stream);
        continue;
      }
    }
    writeSimplePrint(obj->getIndent(), at->getName(), stream);
  }
  // Mark object as already printed
  stream << obj->getIndent()
         << "alreadyPrinted.insert(this);" << endl;
  // Outputs associations
  for(UMLAssociation *a = obj->classInfo()->plainAssociations.first();
      0 != a;
      a = obj->classInfo()->plainAssociations.next()) {
    writeAssocPrint(a, obj, stream);
  }
  for(UMLAssociation *a = obj->classInfo()->aggregations.first();
      0 != a;
      a = obj->classInfo()->aggregations.next()) {
    writeAssocPrint(a, obj, stream);
  }
  for(UMLAssociation *a = obj->classInfo()->compositions.first();
      0 != a;
      a = obj->classInfo()->compositions.next()) {
    writeAssocPrint(a, obj, stream);
  }
}

//=============================================================================
// writeAssocPrint
//=============================================================================
void CppCppClassWriter::writeAssocPrint(UMLAssociation* a,
                                        CppBaseWriter* obj,
                                        QTextStream &stream) {
  if (obj->classInfo()->id() == a->getRoleId(A) ||
      obj->classInfo()->allSuperclassIds.contains(a->getRoleId(A))) {
    if (a->getRoleName(B) != "") {
      Multiplicity multiB = obj->parseMulti(a->getMulti(B));
      switch (multiB) {
      case MULT_ONE:
        {
          UMLInterface* in = dynamic_cast<UMLInterface*>(a->getObject(B));
          UMLClass* cl = dynamic_cast<UMLClass*>(a->getObject(B));
          if (in != 0 || (cl != 0 && ! cl->isEnumeration())) {
            writeAssoc1Print (a->getRoleName(B),
                              a->getObject(B)->getName(),
                              obj,
                              stream);
          } else if (cl != 0 && cl->isEnumeration()) {
            writeEnumPrint (obj->getIndent(), a->getRoleName(B),
                            a->getObject(B)->getName(), stream);
          } else {
            writeSimplePrint (obj->getIndent(), a->getRoleName(B), stream);
          }
        }
        break;
      case MULT_N:
        writeAssocNPrint (a->getRoleName(B),
                          a->getObject(B)->getName() + "*",
                          obj,
                          stream);
        break;
      default:
        break;
      }
    }
  } else {
    if (a->getRoleName(A) != "") {
      Multiplicity multiA = obj->parseMulti(a->getMulti(A));
      switch (multiA) {
      case MULT_ONE:
        {
          UMLInterface* in = dynamic_cast<UMLInterface*>(a->getObject(A));
          UMLClass* cl = dynamic_cast<UMLClass*>(a->getObject(A));
          if (in != 0 || (cl != 0 && ! cl->isEnumeration())) {
            writeAssoc1Print (a->getRoleName(A),
                              a->getObject(A)->getName(),
                              obj,
                              stream);
          } else if (cl != 0 && cl->isEnumeration()) {
            writeEnumPrint (obj->getIndent(), a->getRoleName(A),
                            a->getObject(A)->getName(), stream);
          } else {
            writeSimplePrint (obj->getIndent(), a->getRoleName(A), stream);
          }
        }
        break;
      case MULT_N:
        writeAssocNPrint (a->getRoleName(A),
                          a->getObject(A)->getName() + "*",
                          obj,
                          stream);
        break;
      default:
        break;
      }
    }
  }
}

//=============================================================================
// writeSimplePrint
//=============================================================================
void CppCppClassWriter::writeSimplePrint(QString indent,
                                         QString name,
                                         QTextStream &stream) {
  stream << indent << "stream << indent << \"" << name
         << " : \" << m_" << name
         << " << std::endl;" << endl;
}

//=============================================================================
// writeEnumPrint
//=============================================================================
void CppCppClassWriter::writeEnumPrint(QString indent,
                                       QString name,
                                       QString type,
                                       QTextStream &stream) {
  stream << indent << "stream << indent << \"" << name
         << " : \" << " << type << "Strings[m_" << name
         << "] << std::endl;" << endl;
}

//=============================================================================
// writeAssoc1Print
//=============================================================================
void CppCppClassWriter::writeAssoc1Print(QString name,
                                         QString type,
                                         CppBaseWriter* obj,
                                         QTextStream &stream) {
  stream << obj->getIndent() << "stream << indent << \""
         << capitalizeFirstLetter(name)
         << " : \" << std::endl;" << endl
         << obj->getIndent() << "if (0 != m_" << name
         << ") {" << endl;
  obj->incIndent();
  obj->fixTypeName(type,
                   obj->getNamespace(type),
                   obj->classInfo()->packageName);
  stream << obj->getIndent() << "m_" << name
         << "->print(stream, indent + \"  \", alreadyPrinted);" << endl;
  obj->decIndent();
  stream << obj->getIndent() << "} else {" << endl;
  obj->incIndent();
  stream << obj->getIndent() << "stream << indent << \"  null\" << std::endl;"
         << endl;
  obj->decIndent();
  stream << obj->getIndent() << "}" << endl;
}

//=============================================================================
// writeAssocNPrint
//=============================================================================
void CppCppClassWriter::writeAssocNPrint(QString name,
                                         QString type,
                                         CppBaseWriter* obj,
                                         QTextStream &stream) {
  stream << obj->getIndent() << "{" << endl;
  obj->incIndent();
  stream << obj->getIndent() << "stream << indent << \""
         << capitalizeFirstLetter(name)
         << " : \" << std::endl;" << endl
         << obj->getIndent() << "int i;" << endl
         << obj->getIndent()
         << obj->fixTypeName("vector", "", "")
         << "<" << obj->fixTypeName(type,
                                    obj->getNamespace(type),
                                    obj->classInfo()->packageName)
         << ">::const_iterator it;" << endl
         << obj->getIndent() << "for ("
         << "it = m_" << name
         << "Vector.begin(), i = 0;" << endl
         << obj->getIndent() << "     it != m_"
         << name << "Vector.end();" << endl
         << obj->getIndent() << "     it++, i++) {" << endl;
  obj->incIndent();
  stream << obj->getIndent()
         << "stream << indent << \"  \" << i << \" :\" << std::endl;"
         << endl << obj->getIndent()
         << "(*it)->print(stream, indent + \"    \", alreadyPrinted);"
         << endl;
  obj->decIndent();
  stream << obj->getIndent() << "}" << endl;
  obj->decIndent();
  stream << obj->getIndent() << "}" << endl;
}

//=============================================================================
// writeCuuidPrint
//=============================================================================
void CppCppClassWriter::writeCuuidPrint(CppBaseWriter* obj,
                                        QTextStream &stream) {
  stream << obj->getIndent()
         << "stream << indent << \"content : \" << \"(\""
         << endl << obj->getIndent()
         << "       << m_content.time_low << \", \""
         << endl << obj->getIndent()
         << "       << m_content.time_mid << \", \""
         << endl << obj->getIndent()
         << "       << m_content.time_hi_and_version << \", \""
         << endl << obj->getIndent()
         << "       << m_content.clock_seq_hi_and_reserved << \", \""
         << endl << obj->getIndent()
         << "       << m_content.clock_seq_low << \", (\""
         << endl << obj->getIndent()
         << "       << m_content.node[0] << \",\""
         << endl << obj->getIndent()
         << "       << m_content.node[1] << \",\""
         << endl << obj->getIndent()
         << "       << m_content.node[2] << \",\""
         << endl << obj->getIndent()
         << "       << m_content.node[3] << \",\""
         << endl << obj->getIndent()
         << "       << m_content.node[4] << \",\""
         << endl << obj->getIndent()
         << "       << m_content.node[5]"
         << endl << obj->getIndent()
         << "       << \"))\" << std::endl;"
         << endl;
}

//=============================================================================
// writeStringResponsePrint
//=============================================================================
void CppCppClassWriter::writeStringResponsePrint(CppBaseWriter* obj,
                                                 QTextStream &stream) {
  stream << obj->getIndent()
         << "stream << m_content;"
         << endl;
}

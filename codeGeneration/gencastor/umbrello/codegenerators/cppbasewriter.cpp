// Includes
#include <list>
#include <limits>
#include <iostream>
#include <qobject.h>
#include <qregexp.h>

// local includes
#include "cppbasewriter.h"
#include "../attribute.h"
#include "../classifier.h"
#include "../class.h"

//-----------------------------------------------------------------------------
// Implementation file for class : cppbasewriter
//
// 2004-01-13 : Sebastien Ponce
//-----------------------------------------------------------------------------

//=============================================================================
// Standard constructor, initializes variables
//=============================================================================
CppBaseWriter::CppBaseWriter(UMLDoc *parent,
                             const char *name) :
  CppCastorWriter(parent, name), m_firstInclude (true),
  m_indent(0), m_allowForward(false) {
  // All used standard library classes
  m_stdMembers[QString("istringstream")] = QString("<sstream>");
  m_stdMembers[QString("ostringstream")] = QString("<sstream>");
  m_stdMembers[QString("istream")] = QString("<istream>");
  m_stdMembers[QString("ostream")] = QString("<iostream>");
  m_stdMembers[QString("string")] = QString("<string>");
  m_stdMembers[QString("list")] = QString("<list>");
  m_stdMembers[QString("vector")] = QString("<vector>");
  m_stdMembers[QString("set")] = QString("<set>");
}

//=============================================================================
// addInclude
//=============================================================================
void CppBaseWriter::addInclude(QString file) {
  m_includes.insert(file);
}

//=============================================================================
// writeIncludesFromSet
//=============================================================================
void CppBaseWriter::writeIncludesFromSet(QTextStream &stream,
                                         std::set<QString> &set) {
  for (std::set<QString>::const_iterator it = set.begin();
       it != set.end();
       it++) {
    writeInclude(stream, *it);
  }
}

//=============================================================================
// writeInclude
//=============================================================================
void CppBaseWriter::writeInclude(QTextStream &stream,
                                 const QString &file) {
  checkFirst(stream);
  stream << "#include " << file << endl;
}

//=============================================================================
// writeInclude
//=============================================================================
void CppBaseWriter::checkFirst(QTextStream &stream) {
  if (m_firstInclude) {
    stream << "// Include Files" << endl;
    m_firstInclude = false;
  }
}

//=============================================================================
// writeNSForward
//=============================================================================
void CppBaseWriter::writeNSForward(QTextStream &stream,
                                   QString localNS) {
  writeEasyForward(stream, m_forward);
  int index = localNS.find("::");
  QString ns;
  if (index < 0) {
    ns = localNS;
    localNS = "";
  } else {
    ns = localNS.left(index);
    localNS = localNS.right(localNS.length()-index-2);
  }
  std::set<QString> fwds;
  for (std::set<QString>::const_iterator it = m_forward.begin();
       it != m_forward.end();
       it++){
    if (!it->startsWith(ns)) {
      fwds.insert(*it);
    }
  }
  for (std::set<QString>::const_iterator it = fwds.begin();
       it != fwds.end();
       it++) {
    m_forward.erase(*it);
  }
  if (fwds.begin() != fwds.end()) {
    stream << getIndent() << "// Forward declarations" << endl;
    writeForward(stream, fwds);
  }
  writeNSOpen(stream, ns);
  fwds = m_forward;
  m_forward.clear();
  for (std::set<QString>::const_iterator it = fwds.begin();
       it != fwds.end();
       it++) {
    m_forward.insert(it->right(it->length()-ns.length()-2));
  }
  if (!localNS.isEmpty())
    writeNSForward(stream, localNS);
  else
    writeForward(stream, m_forward);
  m_indent--;
}

//=============================================================================
// writeForward
//=============================================================================
void CppBaseWriter::writeForward(QTextStream &stream,
                                 std::set<QString> fwds) {
  writeEasyForward(stream, fwds);
  // Now go to sub namespaces
  while (fwds.begin() != fwds.end()) {
    std::set<QString> newfwds;
    QString decl = *(fwds.begin());
    fwds.erase(decl);
    int index = decl.find("::");
    QString ns = decl.left(index);
    decl = decl.right(decl.length()-index-2);
    newfwds.insert(decl);
    for (std::set<QString>::const_iterator it = fwds.begin();
         it != fwds.end();
         it++) {
      if (it->startsWith(ns)) {
        newfwds.insert(it->right(it->length()-index-2));
      }
    }
    for (std::set<QString>::const_iterator it = newfwds.begin();
         it != newfwds.end();
         it++) {
      fwds.erase(ns + "::" + *it);
    }
    writeNSOpen(stream, ns);
    writeForward(stream, newfwds);
    writeNSClose(stream,ns);
  }
}

//=============================================================================
// writeEasyForward
//=============================================================================
void CppBaseWriter::writeEasyForward(QTextStream &stream,
                                     std::set<QString>& fwds) {
  if (fwds.begin() == fwds.end()) return;
  std::list<QString> nons;
  for (std::set<QString>::const_iterator it = fwds.begin();
       it != fwds.end();
       it++){
    if (it->find("::") < 0) nons.push_front(*it);
  }
  if (nons.begin() != nons.end()) {
    stream << getIndent() << "// Forward declarations" << endl;
    for (std::list<QString>::const_iterator it = nons.begin();
         it != nons.end();
         it++){
      stream << getIndent() << "class " << *it << ";" << endl;
      fwds.erase(*it);
    }
    stream << endl;
  }
}

//=============================================================================
// writeNSOpen
//=============================================================================
void CppBaseWriter::writeNSOpen(QTextStream &stream,
                                QString ns) {
  stream << getIndent() << "namespace " << ns << " {" << endl << endl;
  m_indent++;
}

//=============================================================================
// writeNSClose
//=============================================================================
void CppBaseWriter::writeNSClose(QTextStream &stream,
                                 QString ns) {
  m_indent--;
  stream << getIndent() << "}; // end of namespace " << ns << endl << endl;
}

//=============================================================================
// getIndent
//=============================================================================
QString CppBaseWriter::getIndent(int l) {
  QString myIndent = "";
  for (int i = 0 ; i < l ; i++)
    myIndent.append(" ");
  return myIndent;
}

//=============================================================================
// getIndent
//=============================================================================
QString CppBaseWriter::getIndent () {
  return getIndent(INDENT*m_indent);
}

//=============================================================================
// writeComment
//=============================================================================
void CppBaseWriter::writeComment(QString comment,
                                 QString myIndent,
                                 QTextStream &stream) {
  // in the case we have several line comment..
  // NOTE: this part of the method has the problem of adopting UNIX newline,
  // need to resolve for using with MAC/WinDoze eventually I assume
  if (comment.contains(QRegExp("\n"))) {
    stream << myIndent << "/*" << endl;
    QStringList lines = QStringList::split( "\n", comment);
    for(uint i= 0; i < lines.count(); i++)
    {
      stream << myIndent << " * " << lines[i] << endl;
    }
    stream << myIndent << "*/" << endl;
  } else {
    // this should be more fancy in the future, breaking it up into 80 char
    // lines so that it doesnt look too bad
    stream << myIndent << "/// " << comment << endl;
  }
}

//=============================================================================
// writeHeaderComment
//=============================================================================
void CppBaseWriter::writeHeaderComment(QString comment,
                                       QString myIndent,
                                       QTextStream &stream) {
  unsigned int l = comment.length();
  QString topline = "/**";
  for (unsigned int i = 0; i < l; i++) topline.append('*');
  topline.append("**/");
  stream << myIndent << topline << endl;
  stream << myIndent << "/* " << comment << " */" << endl;
  stream << myIndent << topline << endl;
}

//=============================================================================
// writeWideHeaderComment
//=============================================================================
void CppBaseWriter::writeWideHeaderComment(QString comment,
                                           QString myIndent,
                                           QTextStream &stream) {
  unsigned int lindent = getIndent().length();
  QString topline = "//";
  for (unsigned int i = 0; i < 80 - lindent - 2; i++) topline.append('-');
  stream << myIndent << topline << endl;
  stream << myIndent << "// " << comment << endl;
  stream << myIndent << topline << endl;
}

//=============================================================================
// writeDocumentation
//=============================================================================
void CppBaseWriter::writeDocumentation(QString header,
                                       QString body,
                                       QString end,
                                       QTextStream &cpp) {
  QString indent = getIndent();
  cpp << indent << "/**" << endl;
  if (!header.isEmpty())
    cpp << formatDoc(header, indent+" * ");
  if (!body.isEmpty())
    cpp << formatDoc(body, indent+" * ");
  if (!end.isEmpty()) {
    QStringList lines = QStringList::split( "\n", end);
    for(uint i= 0; i < lines.count(); i++)
      cpp << formatDoc(lines[i], indent+" * ");
  }
  cpp << indent << " */" << endl;
}

//=============================================================================
// fixTypeName
//=============================================================================
QString CppBaseWriter::fixTypeName(QString string,
                                   QString typePackage,
                                   QString currentPackage,
                                   bool forceNamespace) {
  QString nsString = string;
  // First deal with namespaces
  typePackage = typePackage.replace(".","::");
  currentPackage = currentPackage.replace(".","::");
  if (typePackage != "" && nsString.find("::") < 0) {
    int pos = nsString.find(QRegExp("[a-zA-Z]"));
    if (nsString.find(QString("const"), pos) == pos) {
      pos = nsString.find(QRegExp("[a-zA-Z]"), pos+6);
    }
    nsString.insert(pos, typePackage + "::");
    if (forceNamespace || typePackage != currentPackage) {
      string = nsString;
    }
  }
  // Then deal with include files and forward declarations
  QString type = getSimpleType(nsString);
  if (typePackage != "" &&
      type != currentPackage + "::"+ m_classInfo->className) {
    QString inc = type;
    inc.replace("::", "/");
    inc = "\"" + inc + ".hpp\"";
    if (m_allowForward) {
      if (string.find('*') > -1 || string.find('&') > -1) {
        if (m_includes.find(inc) == m_includes.end()) {
          m_forward.insert(type);
        }
      } else {
        m_forward.erase(type);
        m_includes.insert(inc);
      }
    } else {
      m_includes.insert(inc);
    }
  }
  // Then deal with the standard library
  int curPos = 0;
  int tempPos = 0;
  while (tempPos < (int)string.length()) {
    tempPos = std::numeric_limits<int>::max();
    std::map<QString, QString>::const_iterator winner;
    for (std::map<QString, QString>::const_iterator it = m_stdMembers.begin();
         it != m_stdMembers.end();
         it++) {
      int pos = string.find(it->first, curPos);
      if (pos >= 0 && pos < tempPos) {
        tempPos = pos;
        winner = it;
      }
    }
    if (tempPos < (int)string.length()) {
      curPos = tempPos;
      string.insert(tempPos, "std::");
      m_includes.insert(winner->second);
      curPos = curPos + winner->first.length() + 5;
    }
  }
  // Deal with castor predefined types
  for (std::map<QString, QString>::const_iterator it = m_castorTypes.begin();
       it != m_castorTypes.end();
       it++) {
    int pos = string.find(it->first);
    if (pos >= 0) {
      m_includes.insert(it->second);
    }
  }
  // Last point : deal with arrays and templates (only simple ones)
  m_isLastTypeArray = false;
  if (string.find(']') > -1) {
    int start = string.find('[');
    int end = string.findRev(']');
    m_arrayPart = string.mid(start, end-start+1);
    string.remove(start, string.length()-start);
    m_isLastTypeArray = true;
  } else if (string.find('<') > -1) {
    int start = string.find('<') + 1;
    int end = string.findRev('>');
    QString itype = string.mid(start, end - start);
    itype = fixTypeName(itype,
                        getNamespace(itype),
                        currentPackage,
                        forceNamespace);
    string.replace(start, end - start, itype);
  }
  return string;
}

//=============================================================================
// Initialization
//=============================================================================
bool CppBaseWriter::init(UMLClassifier* c, QString fileName) {
  m_class = c;  
  // open the file
  std::cout << "Generating file " << fileName.ascii() << std::endl;
  if (!openFile(m_file, fileName)) {
    std::cerr << "Unable to open file" << std::endl;
    return false;
  }
  // preparations
  m_classInfo = new ClassifierInfo(m_class, m_doc);
  m_classInfo->className = m_class->getName();
  m_classInfo->packageName = m_class->getPackage().replace(".","::");
  // Open a stream on the file handle
  m_mainStream = new QTextStream(&m_file);
  // creates another stream because we need to write the
  // includes and forward statements afterwards
  m_stream = new QTextStream(&m_buffer, IO_WriteOnly);
  return true;
}

//=============================================================================
// Finalization
//=============================================================================
bool CppBaseWriter::finalize() {
  // Delete class Info
  delete (m_classInfo);
  m_classInfo = 0;
  // Closes the file
  m_file.close();
  // Delete the streams
  delete (m_mainStream);
  m_mainStream = 0;
  delete (m_stream);
  m_stream = 0;
  // No more class
  m_class = 0;
  // reset some members
  m_buffer = "";
  m_arrayPart = "";
  m_firstInclude = true;
  m_indent = 0;
  m_includes.clear();
  m_forward.clear();
  return true;
}

//=============================================================================
// writeOperations
//=============================================================================
void CppBaseWriter::writeOperations
(UMLClassifier *c,
 bool isHeaderMethod,
 Scope permitScope,
 QTextStream &stream,
 QValueList<std::pair<QString, int> >& alreadyGenerated) {
  // First this object methods
  QPtrList<UMLOperation> *opl;
  opl = c->getFilteredOperationsList(permitScope);
  writeOperations(*opl,isHeaderMethod, false, stream, alreadyGenerated);

  // Then the implemented interfaces methods
  if (!c->getAbstract()) {
    for (UMLClassifier *interface = m_classInfo->implementedAbstracts.first();
         interface !=0;
         interface = m_classInfo->implementedAbstracts.next()) { 
      opl = interface->getFilteredOperationsList(permitScope, true);
      if(opl->count()) {
        QString com = " of " + interface->getName();
        if (m_classInfo->isInterface)
          com += " interface";
        else
          com += " abstract class";
        if (isHeaderMethod)
          writeHeaderComment(QString("Implementation") + com,
                                     getIndent(), stream);
        writeOperations(*opl,isHeaderMethod, true, stream, alreadyGenerated);
        if (isHeaderMethod)
          writeHeaderComment(QString("End") + com,
                                     getIndent(), stream);
      }
    }
  }
}

//=============================================================================
// writeOperations
//=============================================================================
void CppBaseWriter::writeOperations
(QPtrList<UMLOperation> &oplist,
 bool isHeaderMethod,
 bool isInterfaceImpl,
 QTextStream &stream,
 QValueList<std::pair<QString, int> >& alreadyGenerated) {
  QString className = m_classInfo->fullPackageName;
  className.append(m_classInfo->className);
  // generate method decl for each operation given
  for (UMLOperation *op = oplist.first();
       0 != op;
       op = oplist.next()) {
    // If no implementation and we are in the .cpp, skip
    if (!isHeaderMethod && op->getAbstract() && !isInterfaceImpl) continue;
    // Deal with the const in the name
    QString name = op->getName();
    bool constOp = false;
    if (name.endsWith(" const")) {
      name = name.left(name.length()-6);
      constOp = true;
    }
    // Check we did not already generate this method
    std::pair<QString, int> p(name, op->getParmList()->count());
    if (alreadyGenerated.find(p) != alreadyGenerated.end()) {
      // already done, skip
      continue;
    } else {
      // mark as done for the future
      alreadyGenerated.append(p);
    }
    // First write the documentation of the method
    if (isHeaderMethod) {
      // Build the documentation from the member list
      QString returnStr = "";
      for(UMLAttribute *at = op->getParmList()->first();
          0 != at;
          at = op->getParmList()->next()) {
        returnStr += "@param " + at->getName() +
          " " + at->getDoc() + "\n";
      }
      writeDocumentation("", op->getDoc(), returnStr, stream);
    } else {
      writeWideHeaderComment(name, getIndent(), stream);
    }
    // Now write the method itself
    int paramIndent = INDENT*m_indent;
    QString methodReturnType = fixTypeName(op->getReturnType(),
                                           getNamespace(op->getReturnType()),
                                           m_classInfo->packageName,
                                           !isHeaderMethod);
    bool forceVirtual = false;
    if (methodReturnType.left(8) == "virtual ") {
      methodReturnType = methodReturnType.remove(0,8);
      forceVirtual = true;
    }
    stream << getIndent();
    if (isHeaderMethod &&
        (isInterfaceImpl || op->getAbstract() || forceVirtual)) {
      // declare abstract method as 'virtual'
      stream << "virtual ";
      paramIndent += 8;
    }
    // static declaration for header file
    if (op->getStatic() && isHeaderMethod) {
      stream << "static ";
      paramIndent += 7;
    }
    // return type of method
    stream << methodReturnType << " ";
    paramIndent += methodReturnType.length() + 1;
    if (!isHeaderMethod) {
      stream << className << "::";
      paramIndent += className.length() + 2;
    }
    stream << name << "(";
    paramIndent += name.length() + 1;
    // method parameters
    QPtrList<UMLAttribute>* pl = op->getParmList();
    for (unsigned int i = 0; i < pl->count(); i++) {
      UMLAttribute* at = pl->at(i);
      if (at) {
        if (at != op->getParmList()->first()) {
          stream << "," << endl << getIndent(paramIndent);
        }
        stream << fixTypeName(at->getTypeName(),
                              getNamespace(at->getTypeName()),
                              m_classInfo->packageName)
               << " " << at->getName();
        if (!(at->getInitialValue().isEmpty())) {
          stream << " = " << at->getInitialValue();
        }
      }
    }
    stream << ")";
    if (constOp) stream << " const";
    // method body : only gets IF its not in a header
    if (isHeaderMethod) {
      if (op->getAbstract() && !isInterfaceImpl) {
        stream << " = 0";
      };
      stream << ";\n";
    } else {
      stream << getIndent() << " {" << endl;
      // See whether we have an implementation for this method
      std::map<std::pair<QString, int>,
        void(*)(CppBaseWriter* obj,
                QTextStream &stream)>::iterator
        it = m_methodImplementations.find
        (std::pair<QString, int>
         (op->getName(), op->getParmList()->count()));
      if (it != m_methodImplementations.end()) {
        // we've got an implemantation
        m_indent++;
        (*(it->second))(this, stream);
        m_indent--;
      }        
      stream << getIndent() << "}" << endl;
    }
    // write it out
    stream << endl;
  }
}

//=============================================================================
// scopeToCPPDecl
//=============================================================================
QString CppBaseWriter::scopeToCPPDecl(Uml::Scope scope) {
  switch(scope) {
  case Uml::Public:
    return QString("public");
  case Uml::Protected:
    return QString("protected");
  case Uml::Private:
    return QString("private");
  default:
    break;
  }
  return QString("UnknownScope");
}

//=============================================================================
// getSimpleType
//=============================================================================
QString CppBaseWriter::getSimpleType(QString type) {
  type.replace("*", "");
  type.replace("&", "");
  type.replace("const", "");
  type.replace("unsigned", "");
  type = type.stripWhiteSpace();
  if (type.startsWith("::"))
    type = type.right(type.length()-2);
  return type;
}
  
//=============================================================================
// getClassifier
//=============================================================================
UMLClassifier* CppBaseWriter::getClassifier(QString type) {
  return m_doc->findUMLClassifier(getSimpleType(type));
}

//=============================================================================
// getNamespace
//=============================================================================
QString CppBaseWriter::getNamespace(QString type) {
  // first try to find out a namespace in the type name
  int pos = type.findRev("::");
  if (pos > -1) return type.left(pos);
  // if the type name does not contain any namespace,
  // Try to see whether the type is know but the UML modeler
  UMLClassifier* c = getClassifier(type);
  if (c != 0) {
    return c->getPackage();
  } else {
    return QString("");
  }
}

//=============================================================================
// createAssocsList
//=============================================================================
CppBaseWriter::AssocList CppBaseWriter::createAssocsList() {
  AssocList result;
  // Parent classes
  for (UMLClassifier *superClass = m_classInfo->allSuperclasses.first();
       0 != superClass;
       superClass = m_classInfo->allSuperclasses.next()) {
    ClassifierInfo ci(superClass, m_doc);
    for(UMLAssociation *a = ci.plainAssociations.first();
        0 != a;
        a = ci.plainAssociations.next()) {
      singleAssocToPairList(a, result, ci);
    }
    for(UMLAssociation *a = ci.aggregations.first();
        0 != a;
        a = ci.aggregations.next()) {
      singleAssocToPairList(a, result, ci);
    }
    for(UMLAssociation *a = ci.compositions.first();
        0 != a;
        a = ci.compositions.next()) {
      singleAssocToPairList(a, result, ci);
    }
  }
  // Current class
  for(UMLAssociation *a = m_classInfo->plainAssociations.first();
      0 != a;
      a = m_classInfo->plainAssociations.next()) {
    singleAssocToPairList(a, result, *m_classInfo);
  }
  for(UMLAssociation *a = m_classInfo->aggregations.first();
      0 != a;
      a = m_classInfo->aggregations.next()) {
    singleAssocToPairList(a, result, *m_classInfo);
  }
  for(UMLAssociation *a = m_classInfo->compositions.first();
      0 != a;
      a = m_classInfo->compositions.next()) {
    singleAssocToPairList(a, result, *m_classInfo);
  }
  return result;
}

//=============================================================================
// createMembersList
//=============================================================================
CppBaseWriter::MemberList CppBaseWriter::createMembersList() {
  MemberList result;
  // Parent classes
  for (UMLClassifier *superClass = m_classInfo->allSuperclasses.first();
       0 != superClass;
       superClass = m_classInfo->allSuperclasses.next()) {
    ClassifierInfo ci(superClass, m_doc);
    for (UMLAttribute *at = ci.getAttList()->first();
         0 != at;
         at = ci.getAttList()->next()) {
      result.append(new Member(at->getName(), at->getTypeName()));
    }
  }
  // Current class
  for (UMLAttribute *at = m_classInfo->getAttList()->first();
       0 != at;
       at = m_classInfo->getAttList()->next()) {
    result.append(new Member(at->getName(), at->getTypeName()));
  }
  return result;
}

//=============================================================================
// singleAssocToPairList
//=============================================================================
void CppBaseWriter::singleAssocToPairList (UMLAssociation *a,
                                           AssocList &list,
                                           ClassifierInfo &ci) {
  Assoc* as;
  if (ci.id() == a->getRoleAId() ||
      ci.allSuperclassIds.contains(a->getRoleAId())) {
    as = new Assoc(AssocType(parseMulti(a->getMultiB()),
                             parseMulti(a->getMultiA()),
                             parseAssocKind(a->getAssocType(), true)),
                   Member(a->getRoleNameB(),
                          a->getObjectB()->getName()),
                   Member(a->getRoleNameA(),
                          a->getObjectA()->getName()));
  } else {
    as = new Assoc(AssocType(parseMulti(a->getMultiA()),
                             parseMulti(a->getMultiB()),
                             parseAssocKind(a->getAssocType(), false)),
                   Member(a->getRoleNameA(),
                          a->getObjectA()->getName()),
                   Member(a->getRoleNameB(),
                          a->getObjectB()->getName()));
  }
  list.append(as);
}

//=============================================================================
// parseMulti
//=============================================================================
CppBaseWriter::Multiplicity CppBaseWriter::parseMulti(QString multi) {
  Multiplicity res = MULT_UNKNOWN;
  if (multi == "1") {
    res = MULT_ONE;
  } else if (multi == "0..n") {
    res = MULT_N;
  }
  return res;
}

//=============================================================================
// parseAssocKind
//=============================================================================
CppBaseWriter::AssocKind CppBaseWriter::parseAssocKind(Uml::Association_Type t,
                                                       bool isParent) {
  switch (t) {
  case Uml::at_Generalization:
  case Uml::at_Implementation:
  case Uml::at_Realization:
    return isParent ? IMPL_PARENT : IMPL_CHILD;
  case Uml::at_Aggregation:
    return isParent ? AGGREG_PARENT : AGGREG_CHILD;
  case Uml::at_Association:
    return ASSOC;
  case Uml::at_Composition:
    return isParent ? COMPOS_PARENT : COMPOS_CHILD;
  case Uml::at_UniAssociation:
    return isParent ? UNI_PARENT : UNI_CHILD;
  default:
    return ASSOC_UNKNOWN;
  }
}

//=============================================================================
// isEnum
//=============================================================================
bool CppBaseWriter::isEnum(QString name) {
  UMLObject* obj = m_doc->findUMLObject(name, Uml::ot_Class);
  if (0 == obj) return false;
  UMLClass *concept = dynamic_cast<UMLClass*>(obj);
  if (0 == concept) return false;
  return concept->isEnumeration();
}

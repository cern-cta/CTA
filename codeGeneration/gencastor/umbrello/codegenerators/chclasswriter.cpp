/******************************************************************************
 *                      chclasswriter.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: chclasswriter.cpp,v $ $Revision: 1.24 $ $Release$ $Date: 2008/07/14 13:52:38 $ $Author: sponcec3 $
 *
 * This generator creates a .h file containing the C interface
 * to the corresponding C++ class
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include files
#include "chclasswriter.h"
#include <qregexp.h>
#include "umlrole.h"

//=============================================================================
// Standard constructor, initializes variables
//=============================================================================
CHClassWriter::CHClassWriter(UMLDoc *parent, const char *name) :
  CppBaseWriter(parent, name) {
  m_typeConvertions["string"] = "const char*";
  m_typeConvertions["longstring"] = "const char*";
  m_typeConvertions["bool"] = "int";
  m_typeConvertions["double"] = "double";
}

//=============================================================================
// writeClass
//=============================================================================
void CHClassWriter::writeClass(UMLClassifier *c) {
  // compute prefix for the functions of this class
  m_prefix = convertType(m_classInfo->className,
                         m_classInfo->packageName);
  if (isEnum(c)) {
    m_prefix = m_prefix.right(m_prefix.length() - 5);
  } else {
    m_prefix = m_prefix.right(m_prefix.length() - 7);
  }
  m_prefix = m_prefix.left(m_prefix.length() - 2) + "_";
  // Generates file header
  QString comment = "This defines a C interface to the following ";
  if (isEnum(c)) {
    comment.append("enum");
  } else {
    comment.append("class");
  }
  comment = comment + "\n" + getIndent() + "// ";
  if (isEnum(c)) {
    comment.append("enum");
  } else {
    comment.append("class");
  }
  comment = comment + " " + m_classInfo->className
    + "\n" + formatDoc(c->getDoc(),getIndent()+"// ");
  comment = comment.left(comment.length() - 1);
  writeCWideHeaderComment(comment, getIndent(), *m_stream);
  *m_stream << endl;
  // Deal with enumerations
  if (isEnum(c)) {
    *m_stream << "#define " << m_prefix
              << "t " << m_classInfo->className
              << endl << endl << "#include \""
              << m_classInfo->className
              << ".hpp\"" << endl << endl;
  } else {
    // PUBLIC constructor/methods/accessors
    writeConstructorDecls(*m_stream);
    writeCastsDecls(*m_stream);
    QValueList<QString> alreadyGeneratedMethods;
    writeOperations(c, *m_stream, alreadyGeneratedMethods);
    writeHeaderAccessorMethodDecl(c, m_classInfo, *m_stream);
  }
}

//=============================================================================
// Initialization
//=============================================================================
bool CHClassWriter::init(UMLClassifier* c, QString fileName) {
  // call upper class init
  this->CppBaseWriter::init(c, fileName);
  // Write Header
  QString str = getHeadingFile(".h");
  if(!str.isEmpty()) {
    str.replace(QRegExp("%filename%"), fileName);
    str.replace(QRegExp("%filepath%"), m_file.name());
    *m_mainStream<<str<<endl;
  }
  // Write the hash define stuff to prevent multiple parsing/inclusion of header
  QString hashDefine = m_classInfo->packageName.upper().replace("::",  "_");
  if (!hashDefine.isEmpty() && !hashDefine.endsWith("_")) hashDefine.append('_');
  hashDefine.append(m_classInfo->className.upper().simplifyWhiteSpace().replace(QRegExp(" "),  "_"));
  *m_mainStream << "#ifndef "<< hashDefine + "_H" << endl;
  *m_mainStream << "#define "<< hashDefine + "_H" << endl << endl;
  return true;
}

//=============================================================================
// Finalization
//=============================================================================
bool CHClassWriter::finalize(UMLClassifier* c) {
  m_indent = 0;
  if (!isEnum(c)) {
    // Writes the includes files
    writeCForwardFromSet(*m_mainStream, m_includes);
  }
  // If there was any include, put a new line after includes
  if (!m_firstInclude) *m_mainStream << endl;
  // and put the buffer content into the file
  *m_mainStream << m_buffer;
  // last thing : close our hashdefine
  QString hashDefine = m_classInfo->packageName.upper().replace("::",  "_");
  if (!hashDefine.isEmpty() && !hashDefine.endsWith("_")) hashDefine.append('_');
  hashDefine.append(m_classInfo->className.upper().simplifyWhiteSpace().replace(QRegExp(" "),  "_"));
  *m_mainStream << "#endif /* " << hashDefine << "_H */" << endl;
  // call upperclass method
  this->CppBaseWriter::finalize(c);
  return true;
}

//=============================================================================
// writeCForwardFromSet
//=============================================================================
void CHClassWriter::writeCForwardFromSet(QTextStream &stream,
                                         std::set<QString> &set) {
  stream << "/* Include Files and Forward declarations for the C world */"
         << endl;
  for (std::set<QString>::const_iterator it = set.begin();
       it != set.end();
       it++) {
    writeCForward(stream, *it);
  }
  stream << endl;
}

//=============================================================================
// writeInclude
//=============================================================================
void CHClassWriter::writeCForward(QTextStream &stream,
                                  const QString &type) {
  stream << type << endl;
}

//=============================================================================
// writeConstructorDecls
//=============================================================================
void CHClassWriter::writeConstructorDecls(QTextStream &stream) {
  if (!m_classInfo->isInterface) {
    writeDocumentation("", "Empty Constructor", "", stream);
    stream << getIndent() << "int " << m_prefix
           << "create("
           << convertType (m_classInfo->className,
                           m_classInfo->packageName)
           << "** obj);" << endl << endl;
  }
  writeDocumentation("", "Empty Destructor", "", stream);
  stream << getIndent() << "int " << m_prefix
         << "delete("
         << convertType (m_classInfo->className,
                         m_classInfo->packageName)
         << "* obj);" << endl << endl;
}

//=============================================================================
// writeCastsDecls
//=============================================================================
void CHClassWriter::writeCastsDecls(QTextStream &stream) {
  for (UMLClassifier *superClass = m_classInfo->allSuperclasses.first();
       0 != superClass;
       superClass = m_classInfo->allSuperclasses.next()) {
    if (m_ignoreClasses.find(superClass->getName()) ==
        m_ignoreClasses.end()) {
      writeCastDecl(stream, superClass->getName());
    }
  }
}

//=============================================================================
// writeCastDecl
//=============================================================================
void CHClassWriter::writeCastDecl(QTextStream &stream,
                                  QString name) {
  writeDocumentation("", "Cast into " + name,
                     "", stream);
  stream << getIndent()
         << convertType(name,
                        getNamespace(name))
         << "* " << m_prefix << "get"
         << name
         << "("
         << convertType (m_classInfo->className,
                         m_classInfo->packageName)
         << "* obj);" << endl << endl;
  writeDocumentation("", "Dynamic cast from " + name,
                     "", stream);
  stream << getIndent()
         << convertType (m_classInfo->className,
                         m_classInfo->packageName)
         << "* " << m_prefix << "from"
         << name
         << "("
         << convertType(name,
                        getNamespace(name))
         << "* obj);" << endl << endl;
}

//=============================================================================
// writeHeaderAccessorMethodDecl
//=============================================================================
void CHClassWriter::writeHeaderAccessorMethodDecl(UMLClassifier *c,
                                                  ClassifierInfo *classInfo,
                                                  QTextStream &stream) {
  // static public attributes
  writeAttributeMethods(classInfo->static_atpub, stream);
  writeAttributeMethods(classInfo->atpub, stream);
  // associations
  writeAssociationMethods
    (classInfo->plainAssociations, c->getID(), stream);
  writeAssociationMethods
    (classInfo->aggregations, c->getID(), stream);
  writeAssociationMethods
    (classInfo->compositions, c->getID(), stream);
}

//=============================================================================
// writeAttributeMethods
//=============================================================================
void CHClassWriter::writeAttributeMethods(QPtrList <UMLAttribute>& attribs,
                                          QTextStream &stream) {
  for(UMLAttribute *at = attribs.first();
      at != 0;
      at=attribs.next()) {
    QString methodBaseName = at->getName();
    methodBaseName.stripWhiteSpace();
    if (at->getStereotype() != SQLONLY) {
      writeSingleAttributeAccessorMethods(at->getTypeName(),
                                          methodBaseName,
                                          at->getDoc(),
                                          Uml::chg_Changeable,
                                          at->getStatic(),
                                          stream);
    }
  }
}

//=============================================================================
// writeAssociationMethods
//=============================================================================
void CHClassWriter::writeAssociationMethods (QPtrList<UMLAssociation> associations,
                                             Uml::IDType myID,
                                             QTextStream &stream) {
  if (forceSections() || !associations.isEmpty()) {
    for(UMLAssociation *a = associations.first(); a; a = associations.next()) {
      // insert the methods to access the role of the other
      // class in the code of this one
      if (a->getUMLRole(Uml::A)->getObject()->getID() == myID && a->getVisibility(Uml::A) == Uml::Visibility::Public) {
        // only write out IF there is a rolename given
        if(!a->getRoleName(Uml::B).isEmpty()) {
          QString name = a->getObject(Uml::B)->getName();
          if (a->getRoleDoc(Uml::B) != SQLONLY) {
            if (!isEnum(name)) name.append("*");
            writeAssociationRoleMethod
              (name,
               a->getRoleName(Uml::B),
               parseMulti(a->getMulti(Uml::B)),
               a->getRoleDoc(Uml::B),
               a->getChangeability(Uml::B),
               stream);
          }
        }
      }
      if (a->getUMLRole(Uml::B)->getObject()->getID() == myID && a->getVisibility(Uml::B) == Uml::Visibility::Public) {
        // only write out IF there is a rolename given
        if(!a->getRoleName(Uml::A).isEmpty()) {
          QString name = a->getObject(Uml::A)->getName();
          if (a->getRoleDoc(Uml::A) != SQLONLY) {
            if (!isEnum(name)) name.append("*");
            writeAssociationRoleMethod
              (name,
               a->getRoleName(Uml::A),
               parseMulti(a->getMulti(Uml::A)),
               a->getRoleDoc(Uml::A),
               a->getChangeability(Uml::A),
               stream);
          }
        }
      }
    }
  }
}

//=============================================================================
// writeAssociationRoleMethod
//=============================================================================
void CHClassWriter::writeAssociationRoleMethod (QString fieldClassName,
                                                QString roleName,
                                                Multiplicity multi,
                                                QString description,
                                                Uml::Changeability_Type change,
                                                QTextStream &stream) {
  switch (multi) {
  case MULT_ONE:
    {
      writeSingleAttributeAccessorMethods(fieldClassName,
                                          roleName,
                                          description,
                                          change,
                                          false,
                                          stream);
    }
    break;
  case MULT_N:
    {
      writeVectorAttributeAccessorMethods (fieldClassName,
                                           roleName,
                                           description,
                                           change,
                                           stream);
    }
    break;
  case MULT_UNKNOWN:
    stream << "Unknown Multiplicity : "
           << fieldClassName << " " << roleName << endl;
    break;
  }
}

//=============================================================================
// writeSingleAttributeAccessorMethods
//=============================================================================
void CHClassWriter::writeSingleAttributeAccessorMethods(QString fieldClassName,
                                                        QString fieldName,
                                                        QString description,
                                                        Uml::Changeability_Type change,
                                                        bool isStatic,
                                                        QTextStream &stream) {
  QString fieldNamespace = getNamespace(fieldClassName);
  if (m_castorTypes.find(getSimpleType(fieldClassName)) != m_castorTypes.end() &&
      m_castorIsComplexType[getSimpleType(fieldClassName)]) {
    fieldClassName.replace("*","");
    fieldClassName.append("_t");
  } else {
    fieldClassName = convertType(fieldClassName,
                                 fieldNamespace);
  }
  // get method
  writeDocumentation ("Get the value of " + fieldName,
                      description, "", stream);
  stream << getIndent() << "int " << m_prefix << fieldName
         << "("
         << convertType (m_classInfo->className,
                         m_classInfo->packageName)
         << "* instance, ";
  if (isLastTypeArray()) stream << "const ";
  stream << fieldClassName;
  if (isLastTypeArray()) stream << "*";
  stream << "* var);" << endl << endl;
  // set method
  if ((!fieldClassName.contains("const ") ||
       fieldClassName.contains("const char*"))&&
      change == Uml::chg_Changeable && !isStatic) {
    writeDocumentation("Set the value of " + fieldName,
                       description, "", stream);
    stream << getIndent() << "int " << m_prefix
           << "set" << capitalizeFirstLetter(fieldName)
           << "("
           << convertType (m_classInfo->className,
                           m_classInfo->packageName)
           << "* instance, ";
    if (isLastTypeArray()) stream << "const ";
    stream << fieldClassName;
    if (isLastTypeArray()) stream << "*";
    stream << " new_var);" << endl << endl;
  }
}

//=============================================================================
// writeVectorAttributeAccessorMethods
//=============================================================================
void CHClassWriter::writeVectorAttributeAccessorMethods (QString fieldClassName,
                                                         QString fieldName,
                                                         QString description,
                                                         Uml::Changeability_Type changeType,
                                                         QTextStream &stream) {
  QString fieldNamespace = getNamespace(fieldClassName);
  fieldClassName = convertType(fieldClassName,
                               fieldNamespace);
  // ONLY IF changeability is NOT Frozen
  if (changeType != Uml::chg_Frozen) {
    writeDocumentation
      ("Add a " + fieldClassName + " object to the " + fieldName + " list",
       description,
       "",
       stream);
    stream << getIndent() << "int " << m_prefix << "add"
           << capitalizeFirstLetter(fieldName)
           << "(" << convertType (m_classInfo->className,
                                  m_classInfo->packageName)
           << "* instance, " << fieldClassName << " obj);"
           << endl << endl;
  }
  // ONLY IF changeability is Changeable
  if (changeType == Uml::chg_Changeable) {
    writeDocumentation
      ("Remove a " + fieldClassName + " object from " +
       fieldName,
       "",
       "",
       stream);
    stream << getIndent() << "int " << m_prefix << "remove"
           << capitalizeFirstLetter(fieldName)
           << "(" << convertType (m_classInfo->className,
                                  m_classInfo->packageName)
           << "* instance, " << fieldClassName << " obj);"
           << endl << endl;
  }
  // always allow getting the list of stuff
  writeDocumentation
    ("Get the list of " + fieldClassName + " objects held by " +
     fieldName + ". Note that the caller is responsible " +
     "for the deletion of the returned vector.",
     "", "", stream);
  stream << getIndent() << "int " << m_prefix << fieldName
         << "(" << convertType (m_classInfo->className,
                                m_classInfo->packageName)
         << "* instance, " << fieldClassName
         << "** var, int* len);"
         << endl << endl;
}

//=============================================================================
// writeOperations
//=============================================================================
void CHClassWriter::writeOperations(UMLClassifier *c,
                                    QTextStream &stream,
                                    QValueList<QString>& alreadyGenerated) {
  // First this object methods
  QPtrList<UMLOperation> *opl;
  opl = ClassifierInfo::getFilteredOperationsList(c, Uml::Visibility::Public);
  writeOperations(*opl, stream, alreadyGenerated);
  delete opl;
  opl = 0;

  // Then the implemented interfaces methods
  for (UMLClassifier *interface = m_classInfo->allSuperclasses.first();
       interface != 0;
       interface = m_classInfo->allSuperclasses.next()) {
    bool noTitle = true;
    QString com = " of " + interface->getName();
    if (interface->isInterface())
      com += " interface";
    else if (interface->getAbstract())
      com += " abstract class";
    else
      com += " class";
    if (!c->getAbstract()) {
      opl = ClassifierInfo::getFilteredOperationsList(interface, Uml::Visibility::Public, true);
      if (opl->count()) {
        writeHeaderComment(QString("Implementation") + com,
                           getIndent(), stream);
        stream << endl;
        noTitle = false;
        writeOperations(*opl, stream, alreadyGenerated);
      }
      delete opl;
    }
    ClassifierInfo aci(interface, m_doc);
    if ((aci.static_atpub.count() ||
         aci.atpub.count() ||
         aci.plainAssociations.count() ||
         aci.aggregations.count() ||
         aci.compositions.count()) &&
        noTitle) {
      writeHeaderComment(QString("Implementation") + com,
                         getIndent(), stream);
      stream << endl;    
    }
    writeHeaderAccessorMethodDecl(interface, &aci, *m_stream);
  }
}

//=============================================================================
// writeOperations
//=============================================================================
void CHClassWriter::writeOperations(QPtrList<UMLOperation> &oplist,
                                    QTextStream &stream,
                                    QValueList<QString>& alreadyGenerated) {
  QString className = m_classInfo->fullPackageName;
  className.append(m_classInfo->className);
  // create a list of members
  UMLAttributeList *members = m_classInfo->getAttList();
  // generate method decl for each operation given
  for (UMLOperation *op = oplist.first();
       0 != op;
       op = oplist.next()) {
    if (!isOpPrintable(op)) continue;
    // Deal with the const in the name
    QString name = op->getName();
    bool constOp = false;
    if (name.endsWith(" const")) {
      name = name.left(name.length()-6);
      constOp = true;
    }
    // Check we will not generate this method as an accessor
    bool skip = false;
    for (UMLAttribute* mem = members->first();
         0 != mem;
         mem = members->next()) {
      QString n = mem->getName();
      if (0 == name.compare(n) ||
          0 == name.compare(QString("set") +
                            n.left(1).upper() +
                            n.right(n.length() - 1))) {
        skip = true;
        break;
      }
    }
    if (skip) continue;
    // Check we did not already generate this method
    if (alreadyGenerated.find(name) != alreadyGenerated.end()) {
      // already done, skip
      continue;
    } else {
      // mark as done for the future
      alreadyGenerated.append(name);
    }
    // First write the documentation of the method
    writeDocumentation("", op->getDoc(), "", stream);
    // Now write the method itself
    int paramIndent = INDENT*m_indent;
    stream << getIndent() << "int " << m_prefix << name
           << "(";
    paramIndent += 5 + m_prefix.length() + name.length();
    // take care of static declarations
    if (!op->getStatic()) {
      stream << convertType(m_classInfo->className,
                            m_classInfo->packageName)
             << "* instance";
    }
    // method parameters
    QPtrList<UMLAttribute> pl = op->getParmList();
    for (unsigned int i = 0; i < pl.count(); i++) {
      UMLAttribute* at = pl.at(i);
      if (at) {
        if (at != op->getParmList().first() || !op->getStatic()) {
          stream << "," << endl << getIndent(paramIndent);
        }
        stream << convertType(at->getTypeName(),
                              getNamespace(at->getTypeName()))
               << " " << at->getName();
        if (!(at->getInitialValue().isEmpty())) {
          stream << " = " << at->getInitialValue();
        }
      }
    }
    // return type of method
    QString methodReturnType = op->getTypeName();
    if (methodReturnType.left(8) == "virtual ") {
      methodReturnType = methodReturnType.remove(0,8);
    }
    methodReturnType = convertType(methodReturnType,
                                   getNamespace(methodReturnType));
    if (methodReturnType != "void" && methodReturnType != "") {
      if (pl.count() > 0 || !op->getStatic()) {
        stream << "," << endl << getIndent(paramIndent);
      }
      stream << methodReturnType;
      if (methodReturnType.stripWhiteSpace().right(1) != "*" ||
          isLastTypeVector() ||
          0 == methodReturnType.stripWhiteSpace().compare("const char*")) {
        stream << "*";
      }
      stream << " ret";
      if (isLastTypeVector()) {
        stream << ", int retlen";
      }
    }
    stream << ");" << endl << endl;
  }
}

//=============================================================================
// convertType
//=============================================================================
QString CHClassWriter::convertType(QString type,
                                   QString ns) {
  // special case of vectors first
  if (type.left(6) == "vector") {
    int start = type.find("<") + 1;
    int end = type.find(">");
    type = type.mid(start, end - start);
    m_isLastTypeVector = true;
  } else {
    m_isLastTypeVector = false;    
  }
  if (m_typeConvertions.find(type) != m_typeConvertions.end()) {
    if (m_castorTypes.find(getSimpleType(type)) != m_castorTypes.end()) {
      addInclude(QString("#include ") + m_castorTypes[getSimpleType(type)]);
	}
    return m_typeConvertions[type];
  }
  if (0 == getClassifier(type)) {
    // deal with arrays (simple ones)
    if (type[type.length()-1] == ']') {
      int pos = type.find('[');
      m_arrayPart = type.mid(pos, type.length()-pos);
      type.remove(pos, type.length()-pos);
      m_isLastTypeArray = true;
    } else {
      m_isLastTypeArray = false;
    }
    if (m_castorTypes.find(getSimpleType(type)) != m_castorTypes.end()) {
      addInclude(QString("#include ") + m_castorTypes[getSimpleType(type)]);
      type.replace("*", "_t");
    }
    return type;
  }
  QString result = QString("C");
  ns.replace(".", "::");
  if (ns.endsWith("::")) ns.truncate(ns.length()-2);
  if (ns.find("::") > -1) {
    result += ns.right(ns.length() - ns.find("::") - 2);
    result.replace("::", "_");
  }
  result = result + "_" + type;
  QString simpleResult = getSimpleType(result);
  int index = result.find(simpleResult) + simpleResult.length();
  result.insert(index, "_t");
  QString prefix = QString("struct ");
  if (isEnum(getSimpleType(type))) {
    ns.replace("::", "/");
    addInclude("#include \"" + ns + "/" + type + ".h\"");
    prefix = "enum ";
  } else { 
    addInclude(prefix + getSimpleType(result) + ";");
  }
  return prefix + result;
}

//=============================================================================
// isOpPrintable
//=============================================================================
bool CHClassWriter::isOpPrintable(UMLOperation* op) {
  // the full print function
  if (op->getName() == "print const" &&
      3 == op->getParmList().count()) {
    return false;
  }
  return true;
}

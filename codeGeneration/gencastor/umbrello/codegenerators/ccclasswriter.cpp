/******************************************************************************
 *                      ccclasswriter.cpp
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
 * @(#)$RCSfile: ccclasswriter.cpp,v $ $Revision: 1.1.1.1 $ $Release$ $Date: 2004/09/28 14:45:44 $ $Author: sponcec3 $
 *
 * This generator creates a .h file containing the C interface
 * to the corresponding C++ class
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include files
#include "ccclasswriter.h"
#include <qregexp.h>

//=============================================================================
// Standard constructor, initializes variables
//=============================================================================
CCClassWriter::CCClassWriter(UMLDoc *parent, const char *name) :
  CppBaseWriter(parent, name) {
  m_typeConvertions["string"] = "const char*";
}

//=============================================================================
// writeClass
//=============================================================================
void CCClassWriter::writeClass(UMLClassifier *c) {
  // compute prefix for the functions of this class
  m_prefix = getPrefix(m_classInfo->className,
                       m_classInfo->packageName) + "_";
  // PUBLIC constructor/methods/accessors
  *m_stream << "extern \"C\" {" << endl << endl;
  m_indent++;
  writeConstructors(*m_stream, c);
  writeCasts(*m_stream, c);
  QValueList<QString> alreadyGeneratedMethods;
  writeOperations(c, *m_stream, alreadyGeneratedMethods);
  writeHeaderAccessorMethod(c, *m_stream);
  m_indent--;
  *m_stream << "} // End of extern \"C\"" << endl;
}

//=============================================================================
// Initialization
//=============================================================================
bool CCClassWriter::init(UMLClassifier* c, QString fileName) {
  // call upper class init
  this->CppBaseWriter::init(c, fileName);
  // Write Header
  QString str = getHeadingFile(".cpp");
  if(!str.isEmpty()) {
    str.replace(QRegExp("%filename%"), fileName);
    str.replace(QRegExp("%filepath%"), m_file.name());
    *m_mainStream << str << endl;
  }
  // define type of the current class in C
  m_typedefs.insert
    (std::pair<QString,QString>
     (m_classInfo->packageName.replace(".", "::"),
      m_classInfo->className));
  return true;
}

//=============================================================================
// Finalization
//=============================================================================
bool CCClassWriter::finalize() {
  m_indent = 0;
  // includes corresponding header file
  writeIncludesFromSet(*m_mainStream, m_includes);
  *m_mainStream << endl;
  // and put the buffer content into the file
  *m_mainStream << m_buffer;
  // call upperclass method
  this->CppBaseWriter::finalize();
  return true;
}

//=============================================================================
// writeConstructor
//=============================================================================
void CCClassWriter::writeConstructors(QTextStream &stream,
                                      UMLClassifier* c) {
  QString className = m_classInfo->fullPackageName;
  className.append(m_classInfo->className);
  if (!c->getAbstract()) {
    writeWideHeaderComment(m_prefix + "create",
                           getIndent(), stream);
    stream << getIndent() << "int " << m_prefix
           << "create("
           << convertType (m_classInfo->className,
                           m_classInfo->packageName)
           << "** obj) {" << endl;
    m_indent++;
    stream << getIndent() << "*obj = new "
           << className << "();" << endl
           << getIndent() << "return 0;" << endl;
    m_indent--;
    stream << getIndent() << "}" << endl;
  }
  writeWideHeaderComment(m_prefix + "delete",
                         getIndent(), stream);
  stream << getIndent() << "int " << m_prefix
         << "delete("
         << convertType (m_classInfo->className,
                         m_classInfo->packageName)
         << "* obj) {" << endl;
  m_indent++;
  stream << getIndent() << "delete obj;" << endl
         << getIndent() << "return 0;" << endl;
  m_indent--;
  stream << getIndent() << "}" << endl << endl;
}

//=============================================================================
// writeCasts
//=============================================================================
void CCClassWriter::writeCasts(QTextStream &stream,
                               UMLClassifier* /*c*/) {
  for (UMLClassifier *superClass = m_classInfo->superclasses.first();
       0 != superClass;
       superClass = m_classInfo->superclasses.next()) {
    writeCast(stream, superClass->getName());
  }
  for (UMLClassifier *superInterface = m_classInfo->superInterfaces.first();
       0 != superInterface;
       superInterface = m_classInfo->superInterfaces.next()) {
    if (m_ignoreClasses.find(superInterface->getName()) !=
        m_ignoreClasses.end()) {
      continue;
    }
    writeCast(stream, superInterface->getName());
  }
}

//=============================================================================
// writeCast
//=============================================================================
void CCClassWriter::writeCast(QTextStream &stream,
                              QString name) {
  writeWideHeaderComment(m_prefix + "get" + name,
                         getIndent(), stream);
  stream << getIndent()
         << convertType (name,
                         getNamespace(name))
         << "* " << m_prefix << "get"
         << name << "("
         << convertType (m_classInfo->className,
                         m_classInfo->packageName)
         << "* obj) {" << endl;
  m_indent++;
  stream << getIndent() << "return obj;" << endl;
  m_indent--;
  stream << getIndent() << "}" << endl << endl;
  writeWideHeaderComment(m_prefix + "from" + name,
                         getIndent(), stream);
  stream << getIndent()
         << convertType (m_classInfo->className,
                         m_classInfo->packageName)
         << "* " << m_prefix << "from"
         << name << "("
         << convertType (name,
                         getNamespace(name))
         << "* obj) {" << endl;
  m_indent++;
  stream << getIndent() << "return dynamic_cast<"
         << convertType (m_classInfo->className,
                         m_classInfo->packageName)
         << "*>(obj);" << endl;
  m_indent--;
  stream << getIndent() << "}" << endl << endl;
}

//=============================================================================
// writeHeaderAccessorMethodDecl
//=============================================================================
void CCClassWriter::writeHeaderAccessorMethod(UMLClassifier *c,
                                              QTextStream &stream) {
  // static public attributes
  writeAttributeMethods(m_classInfo->static_atpub, stream);
  writeAttributeMethods(m_classInfo->atpub, stream);
  // associations
  writeAssociationMethods
    (m_classInfo->plainAssociations, c->getID(), stream);
  writeAssociationMethods
    (m_classInfo->aggregations, c->getID(), stream);
  writeAssociationMethods
    (m_classInfo->compositions, c->getID(), stream);
}

//=============================================================================
// writeAssociationRoleMethod
//=============================================================================
void CCClassWriter::writeAttributeMethods(QPtrList <UMLAttribute>& attribs,
                                          QTextStream &stream) {
  for(UMLAttribute *at = attribs.first();
      at != 0;
      at=attribs.next()) {
    QString methodBaseName = at->getName();
    methodBaseName.stripWhiteSpace();
    writeSingleAttributeAccessorMethods(at->getTypeName(),
                                        methodBaseName,
                                        chg_Changeable,
                                        at->getStatic(),
                                        stream);
  }
}

//=============================================================================
// writeAssociationMethods
//=============================================================================
void CCClassWriter::writeAssociationMethods (QPtrList<UMLAssociation> associations,
                                             int myID,
                                             QTextStream &stream) {
  if (forceSections() || !associations.isEmpty()) {
    for(UMLAssociation *a = associations.first(); a; a = associations.next()) {
      // insert the methods to access the role of the other
      // class in the code of this one
      if (a->getRoleAId() == myID && a->getVisibilityB() == Uml::Public) {
        // only write out IF there is a rolename given
        if(!a->getRoleNameA().isEmpty()) {
          QString className = a->getObjectB()->getName();
          if (!isEnum(className)) className.append("*");
          writeAssociationRoleMethod
            (className,
             a->getRoleNameA(),
             parseMulti(a->getMultiA()),
             a->getChangeabilityB(),
             stream);
        }
      }
      if (a->getRoleBId() == myID && a->getVisibilityA() == Uml::Public) {
        // only write out IF there is a rolename given
        if(!a->getRoleNameB().isEmpty()) {
          QString className = a->getObjectA()->getName();
          if (!isEnum(className)) className.append("*");
          writeAssociationRoleMethod
            (className,
             a->getRoleNameB(),
             parseMulti(a->getMultiB()),
             a->getChangeabilityA(),
             stream);
        }
      }
    }
  }
}

//=============================================================================
// writeAssociationRoleMethod
//=============================================================================
void CCClassWriter::writeAssociationRoleMethod (QString fieldClassName,
                                                QString roleName,
                                                Multiplicity multi,
                                                Changeability_Type change,
                                                QTextStream &stream) {
  switch (multi) {
  case MULT_ONE:
    {
      QString fieldVarName = "m_" + roleName;
      writeSingleAttributeAccessorMethods(fieldClassName,
                                          roleName,
                                          change,
                                          false,
                                          stream);
    }
    break;
  case MULT_N:
    {
      QString fieldVarName = "m_" + roleName + "Vector";
      writeVectorAttributeAccessorMethods (fieldClassName,
                                           roleName,
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
void CCClassWriter::writeSingleAttributeAccessorMethods(QString fieldClassName,
                                                        QString fieldName,
                                                        Changeability_Type change,
                                                        bool isStatic,
                                                        QTextStream &stream) {
  QString fieldCppName;
  bool isCastorType = false;
  if (m_castorTypes.find(getSimpleType(fieldClassName)) != m_castorTypes.end() &&
      m_castorIsComplexType[getSimpleType(fieldClassName)]) {
    fieldClassName.replace("*","");
    fieldCppName = convertType(fieldClassName,
                               getNamespace(fieldClassName));
    fieldClassName.append("_t");
    isCastorType = true;
  } else {
    fieldClassName = convertType(fieldClassName,
                                 getNamespace(fieldClassName));
  }
  bool isArray = isLastTypeArray();
  // get method
  writeWideHeaderComment(m_prefix + fieldName,
                         getIndent(), stream);
  stream << getIndent() << "int " << m_prefix << fieldName
         << "("
         << convertType (m_classInfo->className,
                         m_classInfo->packageName)
         << "* instance, ";
  if (isArray) stream << "const ";
  stream << fieldClassName;
  if (isArray) stream << "*";
  stream << "* var) {" << endl;
  m_indent++;
  if (isCastorType) {
    addInclude(m_castorTypes[fieldClassName.left(fieldClassName.length()-2)]);
    stream << getIndent() << "if (0 != instance->"
           << fieldName << "()) {" << endl;
    m_indent++;
  }
  stream << getIndent() << "*var = instance->"
         << fieldName << "()";
  if (fieldClassName == "const char*") {
    stream << ".c_str()";
  }
  if (isCastorType) {
    stream << "->content()";
  }
  stream << ";" << endl;
  if (isCastorType) {
    m_indent--;
    stream << getIndent() << "}" << endl;
  }
  stream << getIndent() << "return 0;" << endl;
  m_indent--;
  stream << getIndent() << "}" << endl << endl;
  // set method
  if ((!fieldClassName.contains("const ") ||
       fieldClassName.contains("const char*"))&&
      change == chg_Changeable && !isStatic) {
    writeWideHeaderComment
      (m_prefix + "set" + capitalizeFirstLetter(fieldName),
       getIndent(), stream);
    stream << getIndent() << "int " << m_prefix
           << "set" << capitalizeFirstLetter(fieldName)
           << "("
           << convertType (m_classInfo->className,
                           m_classInfo->packageName)
           << "* instance, ";
    if (isArray) stream << "const ";
    stream << fieldClassName;
    if (isArray) stream << "*";
    stream << " new_var) {" << endl;
    m_indent++;
    if (fieldClassName == "const char*") {
      stream << getIndent()
             << "std::string snew_var(new_var, strlen(new_var));"
             << endl;
    }
    if (isCastorType) {
      addInclude(m_castorTypes[fieldClassName.left(fieldClassName.length()-2)]);
      stream << getIndent() << fieldCppName << "* new_varcpp = new "
             << fieldCppName << "();" << endl
             << getIndent() << "new_varcpp->setContent(new_var);"
             << endl;
    }
    stream << getIndent() << "instance->set"
           << capitalizeFirstLetter(fieldName) << "(";
    if (fieldClassName == "const char*") {
      stream << "snew_var";
    } else if (isCastorType) {
      stream << "new_varcpp";
    } else {
      stream << "new_var";
    }
    stream << ");" << endl
           << getIndent() << "return 0;" << endl;
    m_indent--;
    stream << getIndent() << "}" << endl << endl;
  }
}

//=============================================================================
// writeVectorAttributeAccessorMethods
//=============================================================================
void CCClassWriter::writeVectorAttributeAccessorMethods (QString fieldClassName,
                                                         QString fieldName,
                                                         Changeability_Type changeType,
                                                         QTextStream &stream) {
  QString fieldNamespace = getNamespace(fieldClassName);
  fieldClassName = convertType(fieldClassName,
                               fieldNamespace);
  // ONLY IF changeability is NOT Frozen
  if (changeType != chg_Frozen) {
    writeWideHeaderComment
      (m_prefix + "add" + capitalizeFirstLetter(fieldName),
       getIndent(), stream);
    stream << getIndent() << "int " << m_prefix << "add"
           << capitalizeFirstLetter(fieldName)
           << "("
           << convertType (m_classInfo->className,
                           m_classInfo->packageName)
           << "* instance, " << fieldClassName
           << " obj) {" << endl;
    m_indent++;
    stream << getIndent() << "instance->add"
           << capitalizeFirstLetter(fieldName)
           << "(obj);" << endl
           << getIndent() << "return 0;" << endl;
    m_indent--;
    stream << getIndent() << "}" << endl << endl;
  }
  // ONLY IF changeability is Changeable
  if (changeType == chg_Changeable) {
    writeWideHeaderComment
      (m_prefix + "remove" + capitalizeFirstLetter(fieldName),
       getIndent(), stream);
    stream << getIndent() << "int " << m_prefix << "remove"
           << capitalizeFirstLetter(fieldName)
           << "("
           << convertType (m_classInfo->className,
                           m_classInfo->packageName)
           << "* instance, " << fieldClassName
           << " obj) {" << endl;
    m_indent++;
    stream << getIndent() << "instance->remove"
           << capitalizeFirstLetter(fieldName)
           << "(obj);" << endl
           << getIndent() << "return 0;" << endl;
    m_indent--;
    stream << getIndent() << "}" << endl << endl;
  }
  // always allow getting the list of stuff
  writeWideHeaderComment (m_prefix + fieldName,
                          getIndent(), stream);
  stream << getIndent() << "int " << m_prefix << fieldName
         << "("
         << convertType (m_classInfo->className,
                         m_classInfo->packageName)
         << "* instance, " << fieldClassName
         << "** var, int* len) {" << endl;
  m_indent++;
  stream << getIndent() << fixTypeName("vector", "", "")
         << "<" << fieldClassName
         << "> result = instance->" << fieldName
         << "();" << endl << getIndent()
         << "*len = result.size();" << endl
         << getIndent() << "*var = ("
         << fieldClassName << "*) malloc((*len) * sizeof("
         << fieldClassName << "));" << endl
         << getIndent() << "for (int i = 0; i < *len; i++) {"
         << endl;
  m_indent++;
  stream << getIndent() << "(*var)[i] = result[i];"
         << endl;
  m_indent--;
  stream << getIndent() << "}" << endl
         << getIndent() << "return 0;" << endl;
  m_indent--;
  stream << getIndent() << "}"
         << endl << endl;
}

//=============================================================================
// writeOperations
//=============================================================================
void CCClassWriter::writeOperations(UMLClassifier *c,
                                    QTextStream &stream,
                                    QValueList<QString>& alreadyGenerated) {
  // First this object methods
  QPtrList<UMLOperation> *opl;
  opl = c->getFilteredOperationsList(Uml::Public);
  writeOperations(*opl, stream, alreadyGenerated);

  // Then the implemented interfaces methods
  if (!c->getAbstract()) {
    for (UMLClassifier *interface = m_classInfo->implementedAbstracts.first();
         interface !=0;
         interface = m_classInfo->implementedAbstracts.next()) {
      opl = interface->getFilteredOperationsList(Uml::Public, true);
      writeOperations(*opl, stream, alreadyGenerated);
    }
  }
}

//=============================================================================
// writeOperations
//=============================================================================
void CCClassWriter::writeOperations(QPtrList<UMLOperation> &oplist,
                                    QTextStream &stream,
                                    QValueList<QString>& alreadyGenerated) {
  QString className = m_classInfo->fullPackageName;
  className.append(m_classInfo->className);
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
    // Check we did not already generate this method
    if (alreadyGenerated.find(name) != alreadyGenerated.end()) {
      // already done, skip
      continue;
    } else {
      // mark as done for the future
      alreadyGenerated.append(name);
    }
    // First write the documentation of the method
    writeWideHeaderComment (m_prefix + name,
                            getIndent(), stream);
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
    QPtrList<UMLAttribute>* pl = op->getParmList();
    for (unsigned int i = 0; i < pl->count(); i++) {
      UMLAttribute* at = pl->at(i);
      if (at) {
        if (at != op->getParmList()->first() || !op->getStatic()) {
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
    QString methodReturnTypecpp = op->getReturnType();
    if (methodReturnTypecpp.left(8) == "virtual ") {
      methodReturnTypecpp = methodReturnTypecpp.remove(0,8);
    }
    QString methodReturnType = convertType(methodReturnTypecpp,
                                           getNamespace(methodReturnTypecpp));
    if (methodReturnType != "void") {
      if (pl->count() > 0 || !op->getStatic()) {
        stream << "," << endl << getIndent(paramIndent);
      }
      stream << methodReturnType;
      if (methodReturnType.stripWhiteSpace().right(1) != "*" ||
          isLastTypeVector()) {
        stream << "*";
      }
      stream << " ret";
      if (isLastTypeVector()) {
        stream << ", int retlen";
      }
    }
    stream << ") {" << endl;
    m_indent++;
    stream << getIndent();
    if (methodReturnType != "void") {
      if (isLastTypeVector()) {
        stream << fixTypeName(methodReturnTypecpp, "", "")
               << " vecret = ";
      } else {
        stream << "*ret = ";
      }
    }
    if (op->getStatic()) {
      stream << className << "::";
    } else {
      stream << "instance->";
    }
    stream << name << "(";
    for (unsigned int i = 0; i < pl->count(); i++) {
      UMLAttribute* at = pl->at(i);
      if (at) {
        if (at != op->getParmList()->first()) {
          stream << ", ";
        }
        stream << at->getName();
      }
    }
    stream << ")";
    if (methodReturnType == "const char*") {
      stream << ".c_str()";
    }
    stream << ";" << endl;
    if (isLastTypeVector()) {
      stream << getIndent()
             << "*retlen = vecret.size();" << endl
             << getIndent()
             << "*ret = (" << methodReturnType
             << ")malloc((*retlen)*sizeof("
             << methodReturnType << "));" << endl
             << getIndent()
             << "for (int i = 0; i < *retlen; i++) {"
             << endl;
      m_indent++;
      stream << getIndent()
             << "(*ret)[i] = vecret[i];"
             << endl;
      m_indent--;
      stream << getIndent() << "}" << endl;
    }
    stream << getIndent() << "return 0;" << endl;
    m_indent--;
    stream << getIndent() << "}" << endl << endl;
  }
}

//=============================================================================
// getPrefix
//=============================================================================
QString CCClassWriter::getPrefix(QString type,
                                 QString ns) {
  if (m_typeConvertions.find(type) != m_typeConvertions.end()) {
    return m_typeConvertions[type];
  }
  if (0 == m_doc->findUMLClassifier(getSimpleType(type))) {
    return type;
  }
  QString result = QString("C");
  ns.replace(".", "::");
  if (ns.find("::") > -1) {
    result += ns.right(ns.length() - ns.find("::") - 2);
    result.replace("::", "_");
  }
  result = result + "_" + type;
  return result;
}

//=============================================================================
// convertType
//=============================================================================
QString CCClassWriter::convertType(QString type,
                                   QString ns) {
  // special case of vectors first
  if (type.left(6) == "vector") {
    int start = type.find("<") + 1;
    int end = type.find(">");
    type = type.mid(start, end - start);
    ns = getNamespace(type);
    m_isLastTypeVector = true;
  } else {
    m_isLastTypeVector = false;    
  }
  if (m_typeConvertions.find(type) != m_typeConvertions.end()) {
    return m_typeConvertions[type];
  }
  return fixTypeName(type, ns, "");
}

//=============================================================================
// isOpPrintable
//=============================================================================
bool CCClassWriter::isOpPrintable(UMLOperation* op) {
  // the full print function
  if (op->getName() == "print const" &&
      3 == op->getParmList()->count()) {
    return false;
  }
  return true;
}

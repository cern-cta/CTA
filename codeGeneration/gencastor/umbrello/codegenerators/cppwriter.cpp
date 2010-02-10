/***************************************************************************
     cppwriter.cpp  -  description
        -------------------
    copyright     : (C) 2003 Brian Thomas brian.thomas@gsfc.nasa.gov
***************************************************************************/

/***************************************************************************
 *          *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.       *
 *          *
 ***************************************************************************/

// Local includes
#include "cppwriter.h"
#include "codegenerationpolicy.h"
#include "chclasswriter.h"
#include "ccclasswriter.h"
#include "cpphclasswriter.h"
#include "cppcppclasswriter.h"
//#include "cpphoracnvwriter.h"
//#include "cppcpporacnvwriter.h"
#include "cpphdbcnvwriter.h"
#include "cppcppdbcnvwriter.h"
#include "cpphstreamcnvwriter.h"
#include "cppcppstreamcnvwriter.h"

#include "../umldoc.h"
#include <iostream>
#include "model_utils.h"
#include "uml.h"
#include "uniqueid.h"
#include <qregexp.h>
#include "object_factory.h"

CppWriter::CppWriter( ) :
  CppCastorWriter(UMLApp::app()->getDocument(), "Cpp"), firstGeneration(true) {
  // Create all needed generators
  hppw = new CppHClassWriter(m_doc, ".hpp file generator");
  hw = new CHClassWriter(m_doc, ".h file generator for C interfaces");
  cw = new CCClassWriter(m_doc, ".cpp file generator for C interfaces");
  cppw = new CppCppClassWriter(m_doc, ".cpp file generator");
  dbhw = new CppHDbCnvWriter(m_doc, "Database converter generator");
  dbcppw = new CppCppDbCnvWriter(m_doc, "Database converter generator");
  streamhw = new CppHStreamCnvWriter(m_doc, "Stream converter generator");
  streamcppw = new CppCppStreamCnvWriter(m_doc, "Stream converter generator");
  m_noDBCnvs.insert(QString("DiskCopyForRecall"));
  m_noDBCnvs.insert(QString("TapeCopyForMigration"));
}

CppWriter::~CppWriter() {
  // delete Generators
  delete hppw;
  delete hw;
  delete cw;
  delete cppw;
  delete dbhw;
  delete dbcppw;
  delete streamhw;
  delete streamcppw;
}

void CppWriter::configGenerator(CppBaseWriter *cg) {
  cg->setTopNS(s_topNS);
}

void CppWriter::runGenerator(CppBaseWriter *cg,
                             QString fileName,
                             UMLClassifier *c) {
  if (cg->init(c, fileName)) {
    cg->writeClass(c);
    cg->finalize(c);
  }
}

UMLObject* CppWriter::getOrCreateObject(QString type) {
  UMLObject* res = m_doc->findUMLObject(type);
  if (res == NULL) {
    if (type.contains( QRegExp("[\\*\\&]") ))
      res = Object_Factory::createUMLObject(Uml::ot_Datatype, type);
    else
      res = Object_Factory::createUMLObject(Uml::ot_Class, type);
  }
  return res;
}

void CppWriter::writeClass(UMLClassifier *c) {
    
  if (firstGeneration) {
    configGenerator(hppw);
    configGenerator(hw);
    configGenerator(cw);
    configGenerator(cppw);
    configGenerator(dbhw);
    configGenerator(dbcppw);
    configGenerator(streamhw);
    configGenerator(streamcppw);
    firstGeneration = false;
  }

  // Ignore the classes to be ignored
  if (m_ignoreClasses.find(c->getName()) != m_ignoreClasses.end()) {
    return;
  }

  // build file name
  QString fileName = computeFileName(c,".cpp");
  if (!fileName) {
    emit codeGenerated(c, false);
    return;
  }

  // build information on the calss
  ClassifierInfo classInfo(c, m_doc);

  // If not in our namespace, ignore the class
  if (classInfo.packageName.left(s_topNS.length()) != s_topNS) {
    return;
  }

  // Check whether some methods or members should be added
  UMLObject* ostreamType = getOrCreateObject("ostream&");
  UMLObject* stringType = getOrCreateObject("string");
  UMLObject* objectSetType = getOrCreateObject("castor::ObjectSet&");
  {
    // first the print method for IObject descendants
    UMLObject* obj = m_doc->findUMLObject(QString("castor::IObject"),
                                          Uml::ot_Interface);
    const UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
    if (classInfo.allImplementedAbstracts.contains(concept) ||
        classInfo.className == "IObject") {
      Model_Utils::NameAndType_List params;
      UMLOperation* printOp =
        c->createOperation("print const", NULL, &params);
      printOp->setTypeName("virtual void");
      printOp->setID(UniqueID::get());
      printOp->setDoc("Outputs this object in a human readable format");
      if (classInfo.isInterface) {
        printOp->setAbstract(true);
      }
      UMLAttribute* streamParam =
        new UMLAttribute(printOp, "stream",
                         "1", Uml::Visibility::Public, ostreamType);
      streamParam->setDoc("The stream where to print this object");
      printOp->addParm(streamParam);
      UMLAttribute* indentParam =
        new UMLAttribute(printOp, "indent",
                         "2", Uml::Visibility::Public,
                         stringType);
      indentParam->setDoc("The indentation to use");
      printOp->addParm(indentParam);
      UMLAttribute* alreadyPrintedParam =
        new UMLAttribute(printOp, "alreadyPrinted",
                         "3", Uml::Visibility::Public,
                         objectSetType);
      alreadyPrintedParam->setDoc
        (QString("The set of objects already printed.\n") +
         "This is to avoid looping when printing circular dependencies");
      printOp->addParm(alreadyPrintedParam);
      c->addOperation(printOp);
      // Second print method, with no arguments
      printOp = c->createOperation("print const", NULL, &params);
      printOp->setTypeName("virtual void");
      printOp->setID(UniqueID::get());
      printOp->setDoc("Outputs this object in a human readable format");
      if (classInfo.isInterface) {
        printOp->setAbstract(true);
      }
      c->addOperation(printOp);
      // For first level of concrete implementations
      if (!c->getAbstract()) {
        // TYPE method
        UMLOperation* typeOp = c->createOperation("TYPE", NULL, &params);
        typeOp->setTypeName("int");
        typeOp->setID(UniqueID::get());
        typeOp->setDoc("Gets the type of this kind of objects");
        typeOp->setStatic(true);
        c->addOperation(typeOp);
        // check whether some ancester is non abstract
        // If yes, don't redefine id
        if (classInfo.allSuperclasses.count() ==
            classInfo.implementedAbstracts.count()) {
          // ID attribute
          if (!c->isInterface()) {
            UMLAttribute* idAt =
              c->addAttribute
              ("id", getDatatype("u_signed64"), Uml::Visibility::Public);
            idAt->setDoc("The id of this object");
          }
        }
      }
    }
  }

  // write Header file
  runGenerator(hppw, fileName + ".hpp", c);
  if (m_castorTypes.find(c->getName()) == m_castorTypes.end() &&
      m_cWrappedTypes.find(c->getName()) != m_cWrappedTypes.end()) {
    // Write C implementation only when needed
    // These C implementatiosn are supposed to disappear as soon as possible
    // so we hardcoded the list of objects still needing it
    runGenerator(hw, fileName + ".h", c);
    if (!CppBaseWriter::isEnum(c)) {
      QString fileNameC = fileName;
      runGenerator(cw, fileNameC + "CInt.cpp", c);
    }
  }

  // Determine whether the implementation file is required.
  // (It is not required if the class is an interface
  // or an enumeration.)
  if (!c->isInterface()) {
    runGenerator(cppw, fileName + ".cpp", c);

    // Persistency and streaming for castor type is done by hand
    if (m_castorTypes.find(c->getName()) == m_castorTypes.end()) {
      // Determine whether persistency has to be implemented
      // It is implemented for concrete classes implementing
      // the IPersistent abstract interface
      if (!c->getAbstract()) {
        ClassifierInfo* classInfo = new ClassifierInfo(c, m_doc);
        if (m_noDBCnvs.find(c->getName()) == m_noDBCnvs.end()) {
          UMLObject* obj = m_doc->findUMLObject(QString("castor::IPersistent"),
                                                Uml::ot_Interface);
          const UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
          if (classInfo->allImplementedAbstracts.contains(concept)) {
            // check existence of the directory
            QDir outputDirectory = UMLApp::app()->getCommonPolicy()->getOutputDirectory();
            QDir packageDir(UMLApp::app()->getCommonPolicy()->getOutputDirectory().absPath() + "/" + s_topNS + "/db");
            if (! (packageDir.exists() || mkpath(packageDir.absPath()) ) ) {
              std::cerr << "Cannot create the package folder "
                        << packageDir.absPath().ascii()
                        << "\nPlease check the access rights" << std::endl;
              return;
            }
            QDir packageDirDb(UMLApp::app()->getCommonPolicy()->getOutputDirectory().absPath() + "/" + s_topNS + "/db/cnv");
            if (! (packageDirDb.exists() || mkpath(packageDirDb.absPath()) ) ) {
              std::cerr << "Cannot create the package folder "
                        << packageDirDb.absPath().ascii()
                        << "\nPlease check the access rights" << std::endl;
              return;
            }
            
            // run generation
            int i = fileName.findRev('/') + 1;
            QString file = fileName.right(fileName.length()-i);
            runGenerator(dbhw, s_topNS + "/db/cnv/Db" + file + "Cnv.hpp", c);
            runGenerator(dbcppw, s_topNS + "/db/cnv/Db" + file + "Cnv.cpp", c);
          }
        }
        UMLObject* obj = m_doc->findUMLObject(QString("castor::IStreamable"),
                                              Uml::ot_Interface);
        const UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
        if (classInfo->allImplementedAbstracts.contains(concept)) {
          // check existence of the directory
          QDir packageDir(UMLApp::app()->getCommonPolicy()->getOutputDirectory().absPath() + "/" + s_topNS + "/io");
          if (! (packageDir.exists() || mkpath(packageDir.absPath()) ) ) {
            std::cerr << "Cannot create the package folder "
                      << packageDir.absPath().ascii()
                      << "\nPlease check the access rights" << std::endl;
            return;
          }
          // run generation
          int i = fileName.findRev('/') + 1;
          QString file = fileName.right(fileName.length()-i);
          runGenerator(streamhw, s_topNS + "/io/Stream" + file + "Cnv.hpp", c);
          runGenerator(streamcppw, s_topNS + "/io/Stream" + file + "Cnv.cpp", c);
        }
      }
    }
  }

  emit codeGenerated(c, true);
}


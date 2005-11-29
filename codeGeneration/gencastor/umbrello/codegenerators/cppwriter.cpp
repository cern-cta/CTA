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
#include "cpphoracnvwriter.h"
#include "cppcpporacnvwriter.h"
#include "cpphdbcnvwriter.h"
#include "cppcppdbcnvwriter.h"
#include "cpphstreamcnvwriter.h"
#include "cppcppstreamcnvwriter.h"

#include "../umldoc.h"
#include "../class.h"
#include "../interface.h"
#include <iostream>
#include <model_utils.h>
#include "uml.h"

CppWriter::CppWriter( UMLDoc *parent, const char *name ) :
  CppCastorWriter(parent, name), firstGeneration(true) {
  std::cout << "Generation started !" << std::endl;
  // Create all needed generators
  hppw = new CppHClassWriter(m_doc, ".hpp file generator");
  hw = new CHClassWriter(m_doc, ".h file generator for C interfaces");
  cw = new CCClassWriter(m_doc, ".cpp file generator for C interfaces");
  cppw = new CppCppClassWriter(m_doc, ".cpp file generator");
  orahw = new CppHOraCnvWriter(m_doc, "Oracle converter generator");
  oracppw = new CppCppOraCnvWriter(m_doc, "Oracle converter generator");
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
  delete orahw;
  delete oracppw;
  delete dbhw;
  delete dbcppw;
  delete streamhw;
  delete streamcppw;
}

void CppWriter::configGenerator(CppBaseWriter *cg) {
  CodeGenerationPolicy *parentPolicy = getPolicy();
  CodeGenerationPolicy *childPolicy = cg->getPolicy();
  childPolicy->setHeadingFileDir(parentPolicy->getHeadingFileDir());
  childPolicy->setOutputDirectory(parentPolicy->getOutputDirectory());
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

void CppWriter::writeClass(UMLClassifier *c) {
    
  if (firstGeneration) {
    configGenerator(hppw);
    configGenerator(hw);
    configGenerator(cw);
    configGenerator(cppw);
    configGenerator(dbhw);
    configGenerator(dbcppw);
    configGenerator(orahw);
    configGenerator(oracppw);
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
  {
    // first the print method for IObject descendants
    UMLObject* obj = m_doc->findUMLObject(QString("castor::IObject"),
                                          Uml::ot_Interface);
    const UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
    if (classInfo.allImplementedAbstracts.contains(concept) ||
        classInfo.className == "IObject") {
      Umbrello::NameAndType_List params;
      UMLOperation* printOp =
        c->createOperation("print const", NULL, &params);
      printOp->setTypeName("virtual void");
      printOp->setID(m_doc->getUniqueID());
      printOp->setDoc("Outputs this object in a human readable format");
      if (classInfo.isInterface) {
        printOp->setAbstract(true);
      }
      UMLAttribute* streamParam =
        new UMLAttribute(printOp, "stream",
                         "1", Uml::Public, "ostream&");
      streamParam->setDoc("The stream where to print this object");
      printOp->getParmList()->append(streamParam);
      UMLAttribute* indentParam =
        new UMLAttribute(printOp, "indent",
                         "2", Uml::Public,
                         "string");
      indentParam->setDoc("The indentation to use");
      printOp->getParmList()->append(indentParam);
      UMLAttribute* alreadyPrintedParam =
        new UMLAttribute(printOp, "alreadyPrinted",
                         "3", Uml::Public,
                         "castor::ObjectSet&");
      alreadyPrintedParam->setDoc
        (QString("The set of objects already printed.\n") +
         "This is to avoid looping when printing circular dependencies");
      printOp->getParmList()->append(alreadyPrintedParam);
      c->getOpList().append(printOp);
      // Second print method, with no arguments
      printOp = c->createOperation("print const", NULL, &params);
      printOp->setTypeName("virtual void");
      printOp->setID(m_doc->getUniqueID());
      printOp->setDoc("Outputs this object in a human readable format");
      if (classInfo.isInterface) {
        printOp->setAbstract(true);
      }
      c->getOpList().append(printOp);
      // For first level of concrete implementations
      if (!c->getAbstract()) {
        // TYPE method
        UMLOperation* typeOp = c->createOperation("TYPE", NULL, &params);
        typeOp->setTypeName("int");
        typeOp->setID(m_doc->getUniqueID());
        typeOp->setDoc("Gets the type of this kind of objects");
        typeOp->setStatic(true);
        c->getOpList().append(typeOp);
        // check whether some ancester is non abstract
        // If yes, don't redefine id
        if (classInfo.allSuperclasses.count() ==
            classInfo.implementedAbstracts.count()) {
          // ID attribute
          UMLClass* cl = dynamic_cast <UMLClass*>(c);
          if (0 != cl) {
            UMLAttribute* idAt =
              cl->addAttribute
              ("id", getDatatype("u_signed64"), Uml::Public);
            idAt->setDoc("The id of this object");
          }
        }
      }
    }
  }

  // write Header file
  runGenerator(hppw, fileName + ".hpp", c);
  if (m_castorTypes.find(c->getName()) == m_castorTypes.end()) {
    runGenerator(hw, fileName + ".h", c);
    // Write C implementation.
    // Needed even when the CPP one does not exist, except for enums
    if (!CppBaseWriter::isEnum(c)) {
      QString fileNameC = fileName;
      runGenerator(cw, fileNameC + "CInt.cpp", c);
    }
  }

  // Determine whether the implementation file is required.
  // (It is not required if the class is an interface
  // or an enumeration.)
  if ((dynamic_cast<UMLInterface*>(c)) == 0) {
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
            QDir outputDirectory = getPolicy()->getOutputDirectory();
            QDir packageDir(getPolicy()->getOutputDirectory().absPath() + "/" + s_topNS + "/db");
            if (! (packageDir.exists() || packageDir.mkdir(packageDir.absPath()) ) ) {
              std::cerr << "Cannot create the package folder "
                        << packageDir.absPath().ascii()
                        << "\nPlease check the access rights" << std::endl;
              return;
            }
            QDir packageDirOra(getPolicy()->getOutputDirectory().absPath() + "/" + s_topNS + "/db/ora");
            if (! (packageDirOra.exists() || packageDirOra.mkdir(packageDirOra.absPath()) ) ) {
              std::cerr << "Cannot create the package folder "
                        << packageDirOra.absPath().ascii()
                        << "\nPlease check the access rights" << std::endl;
              return;
            }
            QDir packageDirDb(getPolicy()->getOutputDirectory().absPath() + "/" + s_topNS + "/db/cnv");
            if (! (packageDirDb.exists() || packageDirDb.mkdir(packageDirDb.absPath()) ) ) {
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
            runGenerator(orahw, s_topNS + "/db/ora/Ora" + file + "Cnv.hpp", c);
            runGenerator(oracppw, s_topNS + "/db/ora/Ora" + file + "Cnv.cpp", c);
          }
        }
        UMLObject* obj = m_doc->findUMLObject(QString("castor::IStreamable"),
                                              Uml::ot_Interface);
        const UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
        if (classInfo->allImplementedAbstracts.contains(concept)) {
          // check existence of the directory
          QDir packageDir(getPolicy()->getOutputDirectory().absPath() + "/" + s_topNS + "/io");
          if (! (packageDir.exists() || packageDir.mkdir(packageDir.absPath()) ) ) {
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


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
#include "chclasswriter.h"
#include "ccclasswriter.h"
#include "cpphclasswriter.h"
#include "cppcppclasswriter.h"
#include "cpphoracnvwriter.h"
#include "cppcpporacnvwriter.h"
//#include "cpphodbccnvwriter.h"
//#include "cppcppodbccnvwriter.h"
#include "cpphstreamcnvwriter.h"
#include "cppcppstreamcnvwriter.h"

#include "../umldoc.h"
#include "../class.h"
#include "../interface.h"
#include <iostream>

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
  //odbchw = new CppHOdbcCnvWriter(m_doc, "Odbc converter generator");
  //odbccppw = new CppCppOdbcCnvWriter(m_doc, "Odbc converter generator");
  streamhw = new CppHStreamCnvWriter(m_doc, "Stream converter generator");
  streamcppw = new CppCppStreamCnvWriter(m_doc, "Stream converter generator");
  m_noDBCnvs.insert(QString("DiskCopyForRecall"));
  m_noDBCnvs.insert(QString("TapeCopyForMigration"));
}

CppWriter::~CppWriter() {
  // delete Generators
  // Create all needed generators
  delete hppw;
  delete hw;
  delete cw;
  delete cppw;
  delete orahw;
  delete oracppw;
  //delete odbchw;
  //delete odbccppw;
  delete streamhw;
  delete streamcppw;
}

void CppWriter::configGenerator(CppBaseWriter *cg) {
  cg->setForceDoc(forceDoc());
  cg->setForceSections(forceSections());
  cg->setIncludeHeadings(includeHeadings());
  cg->setHeadingFileDir(headingFileDir());
  cg->setModifyNamePolicy(modifyNamePolicy());
  cg->setOutputDirectory(outputDirectory());
  cg->setOverwritePolicy(CodeGenerationPolicy::Ok);
}

void CppWriter::runGenerator(CppBaseWriter *cg,
                             QString fileName,
                             UMLClassifier *c) {
  if (cg->init(c, fileName)) {
    cg->writeClass(c);
    cg->finalize();
  }
}

void CppWriter::writeClass(UMLClassifier *c) {
    
  if (firstGeneration) {
    configGenerator(hppw);
    configGenerator(hw);
    configGenerator(cw);
    configGenerator(cppw);
    configGenerator(orahw);
    configGenerator(oracppw);
    //configGenerator(odbchw);
    //configGenerator(odbccppw);
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

  // If not in namespace castor, ignore the class
  if (classInfo.packageName.left(6) != "castor") {
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
      UMLAttributeList param;
      UMLOperation* printOp =
        getDocument()->createOperation(c,"print const", &param);
      printOp->setReturnType("virtual void");
      printOp->setID(m_doc->getUniqueID());
      printOp->setDoc("Outputs this object in a human readable format");
      if (classInfo.isInterface) {
        printOp->setAbstract(true);
      }
      UMLAttribute* streamParam =
        new UMLAttribute(printOp, "stream",
                         1, "ostream&");
      streamParam->setDoc("The stream where to print this object");
      printOp->getParmList()->append(streamParam);
      UMLAttribute* indentParam =
        new UMLAttribute(printOp, "indent",
                         2,
                         "string");
      indentParam->setDoc("The indentation to use");
      printOp->getParmList()->append(indentParam);
      UMLAttribute* alreadyPrintedParam =
        new UMLAttribute(printOp, "alreadyPrinted",
                         3,
                         "castor::ObjectSet&");
      alreadyPrintedParam->setDoc
        (QString("The set of objects already printed.\n") +
         "This is to avoid looping when printing circular dependencies");
      printOp->getParmList()->append(alreadyPrintedParam);
      c->getOpList().append(printOp);
      // Second print method, with no arguments
      printOp = getDocument()->createOperation(c,"print const", &param);
      printOp->setReturnType("virtual void");
      printOp->setID(m_doc->getUniqueID());
      printOp->setDoc("Outputs this object in a human readable format");
      if (classInfo.isInterface) {
        printOp->setAbstract(true);
      }
      c->getOpList().append(printOp);
      // For first level of concrete implementations
      if (!c->getAbstract()) {
        // TYPE method
        UMLOperation* typeOp = getDocument()->createOperation(c, "TYPE", &param);
        typeOp->setReturnType("int");
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
            UMLAttribute* idAt = new UMLAttribute
              (c, "id", m_doc->getUniqueID(), "u_signed64", Uml::Public);
            idAt->setDoc("The id of this object");
            cl->getAttList()->append(idAt);
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
    if (!classInfo.isEnum) {
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
            QDir packageDir(m_outputDirectory.absPath() + "/castor/db");
            if (! (packageDir.exists() || packageDir.mkdir(packageDir.absPath()) ) ) {
              std::cerr << "Cannot create the package folder "
                        << packageDir.absPath().ascii()
                        << "\nPlease check the access rights" << std::endl;
              return;
            }
            QDir packageDirOra(m_outputDirectory.absPath() + "/castor/db/ora");
            if (! (packageDirOra.exists() || packageDirOra.mkdir(packageDirOra.absPath()) ) ) {
              std::cerr << "Cannot create the package folder "
                        << packageDirOra.absPath().ascii()
                        << "\nPlease check the access rights" << std::endl;
              return;
            }
            //           QDir packageDirOdbc(m_outputDirectory.absPath() + "/castor/db/odbc");
            //           if (! (packageDirOdbc.exists() || packageDirOdbc.mkdir(packageDirOdbc.absPath()) ) ) {
            //             std::cerr << "Cannot create the package folder "
            //                       << packageDirOdbc.absPath().ascii()
            //                       << "\nPlease check the access rights" << std::endl;
            //             return;
            //           }
            // run generation
            int i = fileName.findRev('/') + 1;
            QString file = fileName.right(fileName.length()-i);
            runGenerator(orahw, "castor/db/ora/Ora" + file + "Cnv.hpp", c);
            runGenerator(oracppw, "castor/db/ora/Ora" + file + "Cnv.cpp", c);
            //           runGenerator(odbchw, "castor/db/odbc/Odbc" + file + "Cnv.hpp", c);
            //           runGenerator(odbccppw, "castor/db/odbc/Odbc" + file + "Cnv.cpp", c);
          }
        }
        UMLObject* obj = m_doc->findUMLObject(QString("castor::IStreamable"),
                                              Uml::ot_Interface);
        const UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
        if (classInfo->allImplementedAbstracts.contains(concept)) {
          // check existence of the directory
          QDir packageDir(m_outputDirectory.absPath() + "/castor/io");
          if (! (packageDir.exists() || packageDir.mkdir(packageDir.absPath()) ) ) {
            std::cerr << "Cannot create the package folder "
                      << packageDir.absPath().ascii()
                      << "\nPlease check the access rights" << std::endl;
            return;
          }
          // run generation
          int i = fileName.findRev('/') + 1;
          QString file = fileName.right(fileName.length()-i);
          runGenerator(streamhw, "castor/io/Stream" + file + "Cnv.hpp", c);
          runGenerator(streamcppw, "castor/io/Stream" + file + "Cnv.cpp", c);
        }
      }
    }
  }

  emit codeGenerated(c, true);
  
}


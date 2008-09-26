// Includes
#include <list>
#include <limits>
#include <iostream>
#include <qobject.h>
#include <qregexp.h>
#include <kdebug.h>
#include <klocale.h>
#include <kmessagebox.h>
#include <uml.h>
#include <sys/stat.h>
#include <sys/types.h>

// local includes
#include "folder.h"
#include "cppcastorwriter.h"
#include "../attribute.h"
#include "../classifier.h"

//-----------------------------------------------------------------------------
// Implementation file for class : cppcastorwriter
//
// 2004-01-13 : Sebastien Ponce
//-----------------------------------------------------------------------------

//=============================================================================
// initialization of static variables
//=============================================================================
QString CppCastorWriter::s_topNS = "castor";

//=============================================================================
// Standard constructor, initializes variables
//=============================================================================
CppCastorWriter::CppCastorWriter(UMLDoc* parent, const char *name) :
  SimpleCodeGenerator(parent, name) {
  // All used castor types
  m_castorTypes[QString("Cuuid")] = QString("\"Cuuid.h\"");
  m_castorIsComplexType[QString("Cuuid")] = true;
  m_castorTypes[QString("u_signed64")] = QString("\"osdep.h\"");
  m_castorIsComplexType[QString("u_signed64")] = false;
  m_castorTypes[QString("signed64")] = QString("\"osdep.h\"");
  m_castorIsComplexType[QString("signed64")] = false;
  // Types to ignore
  m_ignoreClasses.insert(QString("IPersistent"));
  m_ignoreClasses.insert(QString("IStreamable"));
  // Associations and Members to ignore, except for dB stuff
  m_ignoreButForDB.insert(QString("lastFileSystemUsed"));
  m_ignoreButForDB.insert(QString("lastButOneFileSystemUsed"));
  m_ignoreButForDB.insert(QString("lastFileSystemChange"));
  m_ignoreButForDB.insert(QString("diskCopySize"));
  m_ignoreButForDB.insert(QString("nbCopyAccesses"));
  m_ignoreButForDB.insert(QString("gcType"));
  m_ignoreButForDB.insert(QString("migrSelectPolicy"));
  m_ignoreButForDB.insert(QString("owneruid"));
  m_ignoreButForDB.insert(QString("ownergid"));
}

//=============================================================================
// openFile
//=============================================================================
bool CppCastorWriter::openFile (QFile & file, QString fileName, int mode) {
  //open files for writing.
  if(fileName.isEmpty()) {
    kdWarning() << "cannot find a file name" << endl;
    return false;
  } else {
    QDir outputDirectory = UMLApp::app()->getCommonPolicy()->getOutputDirectory();
    file.setName(outputDirectory.absFilePath(fileName));
    if(!file.open(mode)) {
      KMessageBox::sorry(0,i18n("Cannot open file %1. Please make sure the folder exists and you have permissions to write to it.").arg(file.name()),i18n("Cannot Open File"));
      return false;
    }
    return true;
  }
}

//=============================================================================
// computeFileName
//=============================================================================
QString CppCastorWriter::computeFileName(UMLClassifier* concept, QString ext) {

  //if we already know to which file this class was written/should be written, just return it.
  if(m_fileMap->contains(concept))
    return ((*m_fileMap)[concept]);

  //else, determine the "natural" file name
  QString name;
  // Get the package name
  QString package = concept->getPackage();

  // Replace all white spaces with blanks
  package.simplifyWhiteSpace();

  // Replace all blanks with underscore
  package.replace(QRegExp(" "), "_");

  // if package is given add this as a directory to the file name
  if (!package.isEmpty()) {
    name = package + "::" + concept->getName();
    package.replace(QRegExp("\\."), "/");
    package.replace(QString("::"),"/");
    package = "/" + package;
  } else {
    name = concept->getName();
  }

  // Convert all "." and "::" to "/" : Platform-specific path separator
  name.replace(QRegExp("\\."),"/"); // Simple hack!
  name.replace(QString("::"),"/");

  // if a package name exists check the existence of the package directory
  if (!package.isEmpty()) {
    QDir packageDir(UMLApp::app()->getCommonPolicy()->getOutputDirectory().absPath() + package);
    if (! (packageDir.exists() || mkpath(packageDir.absPath()) ) ) {
      KMessageBox::error(0, i18n("Cannot create the package folder:\n") +
                         packageDir.absPath() + i18n("\nPlease check the access rights"),
                         i18n("Cannot Create Folder"));
      return NULL;
    }
  }

  name.simplifyWhiteSpace();
  name.replace(QRegExp(" "),"_");

  ext.simplifyWhiteSpace();
  ext.replace(QRegExp(" "),"_");

  m_fileMap->insert(concept,name);
  return name; //if not, "name" is OK and we have not much to to
}

//=============================================================================
// getSimpleType
//=============================================================================
QString CppCastorWriter::getSimpleType(QString type) {
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
UMLClassifier* CppCastorWriter::getClassifier(QString type) {
  QString name = getSimpleType(type);
  UMLClassifierList inList = m_doc->getClassesAndInterfaces();
  for (UMLClassifier * obj = inList.first(); obj != 0; obj = inList.next()) {
    if (obj->getName() == name) {
      return obj;
    }
  }
  return NULL;
}

//=============================================================================
// getDatatype
//=============================================================================
UMLClassifier* CppCastorWriter::getDatatype(QString type) {
  QString name = getSimpleType(type);
  return new UMLClassifier(name);
}

//=============================================================================
// mkpath
//=============================================================================
bool CppCastorWriter::mkpath(const QString &name) {
  QString dirName = name;
  for(int oldslash = -1, slash = 0; slash != -1; oldslash = slash) {
    slash = dirName.find(QDir::separator(), oldslash + 1);
    if (slash == -1) {
      if (oldslash == (int)dirName.length())
	break;
      slash = dirName.length();
    }
    if (slash) {
      QByteArray chunk = QFile::encodeName(dirName.left(slash));
      struct stat st;
      if (stat(chunk, &st) != -1) {
	if ((st.st_mode & S_IFMT) != S_IFDIR)
	  return false;
      } else if (::mkdir(chunk, 0777) != 0) {
	return false;
      }
    }
  }
  return true;
}

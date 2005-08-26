// Includes
#include <list>
#include <limits>
#include <iostream>
#include <qobject.h>
#include <qregexp.h>
#include <kdebug.h>
#include <klocale.h>
#include <kmessagebox.h>

// local includes
#include "cppcastorwriter.h"
#include "../attribute.h"
#include "../classifier.h"
#include "../class.h"

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
CppCastorWriter::CppCastorWriter(UMLDoc *parent,
                                 const char *name) :
  SimpleCodeGenerator(parent, name) {
  // All used castor types
  m_castorTypes[QString("Cuuid")] = QString("\"Cuuid.h\"");
  m_castorIsComplexType[QString("Cuuid")] = true;
  m_castorTypes[QString("u_signed64")] = QString("\"osdep.h\"");
  m_castorIsComplexType[QString("u_signed64")] = false;
  // Types to ignore
  m_ignoreClasses.insert(QString("IPersistent"));
  m_ignoreClasses.insert(QString("IStreamable"));
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
		QDir outputDirectory = getPolicy()->getOutputDirectory();
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
    name = package + "." + concept->getName();
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
    QDir packageDir(getPolicy()->getOutputDirectory().absPath() + package);
    if (! (packageDir.exists() || packageDir.mkdir(packageDir.absPath()) ) ) {
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

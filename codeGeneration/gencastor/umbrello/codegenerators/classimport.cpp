/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 *  copyright (C) 2006                                                     *
 *  Umbrello UML Modeller Authors <uml-devel@ uml.sf.net>                  *
 ***************************************************************************/

// own header
#include "codeimport/classimport.h"
// qt/kde includes
#include <qregexp.h>
#include <klocale.h>
// app includes
#include "../umldoc.h"
#include "../uml.h"
#include "codeimport/cppimport.h"

void ClassImport::importFiles(QStringList fileList) {
    initialize();
    UMLDoc *umldoc = UMLApp::app()->getDocument();
    uint processedFilesCount = 0;
    for (QStringList::Iterator fileIT = fileList.begin();
            fileIT != fileList.end(); ++fileIT) {
        QString fileName = (*fileIT);
        umldoc->writeToStatusBar(i18n("Importing file: %1 Progress: %2/%3").
                                 arg(fileName).arg(processedFilesCount).arg(fileList.size()));
        parseFile(fileName);
        processedFilesCount++;
    }
    umldoc->writeToStatusBar(i18n("Ready."));
}

ClassImport *ClassImport::createImporterByFileExt(QString /*filename*/) {
    return 0;
}


/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/

#include "uml.h"
#include <kaboutdata.h>
#include <kapplication.h>
#include <kcmdlineargs.h>
#include <kconfig.h>
#include <iostream>

static KCmdLineOptions options[] =
    {
        { "+[File]", I18N_NOOP("file to open"), 0 },
        // INSERT YOUR COMMANDLINE OPTIONS HERE
        KCmdLineLastOption
    };

int main(int argc, char *argv[]) {

	KAboutData aboutData( "umbrello", I18N_NOOP("Umbrello UML Modeller"),
	                      "2.0", "Umbrello UML Modeller",
                        KAboutData::License_GPL,
	                      I18N_NOOP("2004 Sebastien Ponce"), 0,
	                      "http://cern.ch/castor");
	KCmdLineArgs::init( argc, argv, &aboutData );
	KCmdLineArgs::addCmdLineOptions( options ); // Add our own options.

	KApplication app;
  UMLApp *uml = new UMLApp();
  KConfig * cfg = app.config();
  uml->initGenerators();

  KCmdLineArgs *args = KCmdLineArgs::parsedArgs();
  if ( args -> count() ) {
    uml -> openDocumentFile( args -> url( 0 ) );
    uml->generateAllCode();
    args -> clear();
  } else {
    std::cout << "Invalid number of arguments\n"
              << "syntax : " << argv[0] << " <xmi File>"
              << std::endl;
  }
  uml->close();
}

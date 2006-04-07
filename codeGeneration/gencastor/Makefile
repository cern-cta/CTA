DEV=$(CASTORDEV_HOME)/codeGeneration/gencastor
UMBRELLOBASE=../kdesdk-3.5.1

UMBRELLO=$(UMBRELLOBASE)/umbrello/umbrello
UMBRELLOOBJBASE=$(UMBRELLOBASE)
UMBRELLOOBJ=$(UMBRELLOOBJBASE)/umbrello/umbrello
UMBRELLOGEN=$(UMBRELLOOBJ)/codegenerators

RAWOBJS=activitywidget.o actor.o actorwidget.o aligntoolbar.o artifact.o artifactwidget.o association.o associationwidget.o assocrules.o attribute.o boxwidget.o classifier.o classifiercodedocument.o classifierlistitem.o classifierwidget.o codeaccessormethod.o codeblock.o codeblockwithcomments.o codeclassfield.o codeclassfielddeclarationblock.o codecomment.o codedocument.o codegenerationpolicy.o codegenerator.o codegenobjectwithtextblocks.o codemethodblock.o codeoperation.o codeparameter.o component.o componentwidget.o configurable.o datatype.o datatypewidget.o docwindow.o dialog_utils.o entity.o entityattribute.o entitywidget.o enum.o enumliteral.o enumwidget.o floatingtext.o forkjoinwidget.o hierarchicalcodeblock.o infowidget.o kplayerslideraction.o kstartuplogo.o linepath.o linkwidget.o listpopupmenu.o messagewidget.o model_utils.o node.o nodewidget.o notewidget.o object_factory.o objectwidget.o operation.o ownedcodeblock.o ownedhierarchicalcodeblock.o package.o packagewidget.o plugin.o pluginloader.o seqlinewidget.o statewidget.o stereotype.o template.o textblock.o toolbarstate.o toolbarstatearrow.o toolbarstateassociation.o toolbarstatefactory.o toolbarstatemessages.o toolbarstateother.o toolbarstatepool.o uml.o umlattributelist.o umlcanvasobject.o umlclassifierlistitemlist.o umldoc.o umllistview.o umllistviewitem.o umlnamespace.o umlobject.o umlobjectlist.o umlrole.o umlview.o umlviewcanvas.o umlwidget.o usecase.o usecasewidget.o widgetbase.o widget_utils.o worktoolbar.o exportviewaction.o import_utils.o 
OBJS=$(patsubst %,$(UMBRELLOOBJ)/%,$(RAWOBJS))

UMBRELLORAWLIBS=refactoring/.libs/librefactoring.a classparser/.libs/libclassparser.a clipboard/.libs/libclipboard.a dialogs/.libs/libdialogs.a
UMBRELLOLIBS=$(patsubst %,$(UMBRELLOOBJ)/%,$(UMBRELLORAWLIBS))

MOCOBJS=umbrello/codegenerators/simplecodegenerator.moc

OWNGENSRCS=classifierinfo.cpp classimport.cpp codegenfactory.cpp cppcastorwriter.cpp cppbasewriter.cpp cppcppclasswriter.cpp cppcppwriter.cpp cpphclasswriter.cpp cpphwriter.cpp cppwriter.cpp chclasswriter.cpp ccclasswriter.cpp cpphbasecnvwriter.cpp cppcppbasecnvwriter.cpp cpphoracnvwriter.cpp cppcpporacnvwriter.cpp cpphdbcnvwriter.cpp cppcppdbcnvwriter.cpp cpphstreamcnvwriter.cpp cppcppstreamcnvwriter.cpp simplecodegenerator.cpp
OWNGENOBJS=$(patsubst %.cpp,umbrello/codegenerators/%.lo,$(OWNGENSRCS))
OWNGENRAWOBJS=$(patsubst %.cpp,umbrello/codegenerators/%.o,$(OWNGENSRCS))

INCLUDES=-I$(DEV)/umbrello -I$(DEV)/umbrello/codegenerators -I$(UMBRELLO) -I$(UMBRELLOOBJBASE) -I$(UMBRELLOOBJ) -I$(UMBRELLO)/codegenerators -I$(UMBRELLOOBJ)/dialogs -I/usr/include/kde -I/usr/share/qt3/include -I/usr/X11R6/include -DQT_THREAD_SUPPORT -D_REENTRANT
COMPILEFLAGS= --mode=compile --tag=CXX g++
LINKFLAGS= --mode=link --tag=CXX g++ -pthread
LINKDEPS=-lfl -lkdeprint -lkio
MACROS=-g -O0 -Wnon-virtual-dtor -Wno-long-long -Wundef -ansi -D_XOPEN_SOURCE=500 -D_BSD_SOURCE -Wcast-align -Wconversion -Wchar-subscripts -Wall -W -Wpointer-arith -Wwrite-strings -DNDEBUG -DNO_DEBUG -Wformat-security -Wmissing-format-attribute -fno-exceptions -fno-check-new -fno-common -DQT_CLEAN_NAMESPACE -DQT_NO_ASCII_CAST -DQT_NO_STL -DQT_NO_COMPAT -DQT_NO_TRANSLATION


all : gencastor.bin

install : gencastor.bin
	@echo Installing gencastor
	@install gencastor $(DESTDIR)/usr/bin
	@install gencastor.bin $(DESTDIR)/usr/bin
	@install gencastor.1 $(DESTDIR)/usr/share/man/man1
	@install -D headers/heading.cpp $(DESTDIR)/etc/castor/gencastor/heading.cpp
	@install -D headers/heading.h $(DESTDIR)/etc/castor/gencastor/heading.h

%.lo :: %.cpp
	@echo Compiling $<
	@libtool $(COMPILEFLAGS) $(INCLUDES) $(MACROS) -c -o $@ $<

%.moc : %.h
	moc $< -o $@

$(DEV)/libcodegenerator.la : $(OWNGENOBJS)
	@echo Linking libcodegenerator
	@libtool $(LINKFLAGS) $(MACROS) -o libcodegenerator.la  -L/usr/X11R6/lib -L/usr/share/qt3/lib -L/usr/lib $(OWNGENOBJS)

gencastor.bin : genCastor.o $(MOCOBJS) $(DEV)/libcodegenerator.la
	@echo Linking executable
	@libtool --mode=link $(LINKFLAGS) -o $@ $(OBJS) $(UMBRELLOLIBS) libcodegenerator.la genCastor.o $(LINKDEPS)
# -all-static 

genCastor.o : genCastor.cpp
	@echo Compiling $<
	gcc -c $(INCLUDES) $(MACROS) -o $@ $<

clean :
	@echo Cleaning
	@-rm -rf genCastor.o gencastor.bin $(OWNGENOBJS) $(OWNGENRAWOBJS) libcodegenerator.la  $(MOCOBJS) .libs umbrello/.libs umbrello/codegenerators/.libs

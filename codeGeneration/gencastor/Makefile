UMBRELLOBASE=../kdesdk-3.2.3

UMBRELLOOBJBASE=$(UMBRELLOBASE)/obj-i386-linux
UMBRELLO=$(UMBRELLOBASE)/umbrello/umbrello
UMBRELLOOBJ=$(UMBRELLOOBJBASE)/umbrello/umbrello
UMBRELLODIALOGS=$(UMBRELLOOBJ)/dialogs
UMBRELLOGEN=$(UMBRELLOOBJ)/codegenerators

RAWOBJS=activitywidget.o actor.o actorwidget.o artifact.o artifactwidget.o association.o associationwidget.o assocrules.o attribute.o boxwidget.o class.o classifiercodedocument.o classifierlistitem.o classimport.o classwidget.o codeaccessormethod.o codeblock.o codeblockwithcomments.o codeclassfield.o codeclassfielddeclarationblock.o codecomment.o codedocument.o codegenobjectwithtextblocks.o codemethodblock.o codeoperation.o codeparameter.o component.o componentwidget.o configurable.o datatype.o datatypewidget.o docwindow.o enum.o enumliteral.o enumwidget.o floatingtext.o kplayerslideraction.o hierarchicalcodeblock.o infowidget.o interface.o interfacewidget.o kstartuplogo.o linepath.o listpopupmenu.o messagewidget.o node.o nodewidget.o notewidget.o objectwidget.o operation.o ownedcodeblock.o ownedhierarchicalcodeblock.o package.o packagewidget.o plugin.o pluginloader.o seqlinewidget.o statewidget.o stereotype.o template.o textblock.o uml.o umldoc.o umllistview.o umllistviewitem.o umlnamespace.o umlobject.o umlrole.o umlview.o umlviewcanvas.o umlwidget.o usecase.o usecasewidget.o worktoolbar.o
OBJS=$(patsubst %,$(UMBRELLOOBJ)/%,$(RAWOBJS))

CHANGEDSRCS=classifier.cpp codegenerationpolicy.cpp codegenerator.cpp umlcanvasobject.cpp
CHANGEDOBJS=$(patsubst %.cpp,umbrello/%.lo,$(CHANGEDSRCS))
CHANGEDRAWOBJS=$(patsubst %.cpp,umbrello/%.o,$(CHANGEDSRCS))

UMBRELLORAWLIBS=refactoring/librefactoring.la classparser/libclassparser.la clipboard/libclipboard.la dialogs/libdialogs.la
UMBRELLOLIBS=$(patsubst %,$(UMBRELLOOBJ)/%,$(UMBRELLORAWLIBS))

CODEGENSRCS=codegenfactory.cpp classifierinfo.cpp cppcastorwriter.cpp cppbasewriter.cpp cppcppclasswriter.cpp cppcppwriter.cpp cpphclasswriter.cpp cpphwriter.cpp cppwriter.cpp chclasswriter.cpp ccclasswriter.cpp cpphbasecnvwriter.cpp cppcppbasecnvwriter.cpp cpphoracnvwriter.cpp cppcpporacnvwriter.cpp cpphodbccnvwriter.cpp cppcppodbccnvwriter.cpp cpphstreamcnvwriter.cpp cppcppstreamcnvwriter.cpp simplecodegenerator.cpp
CODEGENOBJS=$(patsubst %.cpp,umbrello/codegenerators/%.lo,$(CODEGENSRCS))
CODEGENRAWOBJS=$(patsubst %.cpp,umbrello/codegenerators/%.o,$(CODEGENSRCS))

INCLUDES=-DHAVE_CONFIG_H -I. -I umbrello/cogenerators -I$(UMBRELLO) -I$(UMBRELLOOBJBASE) -I$(UMBRELLOOBJ) -I$(UMBRELLO)/codegenerators -I$(UMBRELLOOBJ)/dialogs -I/usr/include/kde -I/usr/share/qt3/include -I/usr/X11R6/include -DQT_THREAD_SUPPORT -D_REENTRANT
COMPILEFLAGS=--silent --mode=compile --tag=CXX g++
LINKFLAGS=--silent --mode=link --tag=CXX g++
LINKDEPS=-lfl -lkdeprint -lkio
MACROS=-Wnon-virtual-dtor -Wno-long-long -Wundef -ansi -D_XOPEN_SOURCE=500 -D_BSD_SOURCE -Wcast-align -Wconversion -Wchar-subscripts -Wall -W -Wpointer-arith -Wwrite-strings -DNDEBUG -DNO_DEBUG -O2 -Wformat-security -Wmissing-format-attribute -fno-exceptions -fno-check-new -fno-common -DQT_CLEAN_NAMESPACE -DQT_NO_ASCII_CAST -DQT_NO_STL -DQT_NO_COMPAT -DQT_NO_TRANSLATION

all : gencastor

install : gencastor
	@echo Installing gencastor
	@install gencastor $(DESTDIR)/usr/bin
	@install gencastor.1 $(DESTDIR)/usr/share/man/man1

umbrello/codegenerators/simplecodegenerator.lo : umbrello/codegenerators/simplecodegenerator.cpp
	@echo Compiling $<
	@moc $(UMBRELLO)/codegenerators/simplecodegenerator.h -o umbrello/codegenerators/simplecodegenerator.moc
	@libtool $(COMPILEFLAGS) $(INCLUDES) $(MACROS) -c -o $@ $<

%.lo : %.cpp
	@echo Compiling $<
	@libtool $(COMPILEFLAGS) $(INCLUDES) $(MACROS) -c -o $@ $<

%.moc : %.h
	moc $< -o $@

libcodegenerator.la : $(CODEGENOBJS)
	@echo Linking libcodegenerator
	@libtool $(LINKFLAGS) $(MACROS) -o libcodegenerator.la  -L/usr/X11R6/lib -L/usr/share/qt3/lib -L/usr/lib  $(CODEGENOBJS)

gencastor : genCastor.o $(CHANGEDOBJS) libcodegenerator.la
	@echo Linking executable
	@libtool $(LINKFLAGS) -o gencastor $(OBJS) $(CHANGEDOBJS) $(UMBRELLOLIBS) libcodegenerator.la genCastor.o $(LINKDEPS)

genCastor.o : genCastor.cpp
	@echo Compiling $<
	@gcc -c -I$(UMBRELLO) -I$(UMBRELLODIALOGS) -I/usr/include/kde -I/usr/include/qt3 -o genCastor.o genCastor.cpp

clean :
	@echo Cleaning
	@-rm -rf genCastor.o gencastor $(CHANGEDOBJS) $(CODEGENOBJS) $(CHANGEDRAWOBJS) $(CODEGENRAWOBJS) libcodegenerator.la umbrello/codegenerators/simplecodegenerator.moc .libs umbrello/.libs umbrello/codegenerators/.libs
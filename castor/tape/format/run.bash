

valgrind --tool=memcheck --log-file-exactly=valgrind-albwriter.output ./albwriter


valgrind --tool=memcheck --log-file-exactly=valgrind-albreader.output ./albreader

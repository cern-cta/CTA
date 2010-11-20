#!/bin/bash
find ./ -type f | xargs file | grep ELF | grep -v codeGeneration | cut -d ":" -f 1 | xargs rm
find ./ -type f -name "*.d"  | xargs rm

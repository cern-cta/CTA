./test_check_nothing_pending.pl  | grep Remaining  | perl -p -i -e 's:^.* /castor:/castor:' | xargs -i su canoc3 -c "stager_rm -M {}"

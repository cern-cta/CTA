{
  for (i=1;i<=NF;i++) {
    if (index($i,"-V") != 0 || index($i,"-v") != 0 ) {
      i++;
      vid=$i;
      reqtype=26;

      if ($5 == "tpread") {
        reqtype=25;
      }

      printf("./rtcpc_test %s T10K60 \"\" \"\" %d\n",vid,reqtype)
    }
  }
}

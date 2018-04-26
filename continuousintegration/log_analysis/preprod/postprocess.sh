#!/bin/sh

process()
{
  echo "log_data = ["
  awk '/[^ ];$/ && NF == 21 { print $0; }' $1
  echo "];"
}

process log_analysis.m >log_analysis_post.m

#!/bin/bash

while [ -s corp.jsonl ]; do
	echo "File is not empty. Continuing..."
	head -n 999 corp.jsonl >> corp_imd.jsonl
	sed -i '1,999d' corp.jsonl
	wc -l corp_imd.jsonl
	python3 corp.py
	exit_code=$?
	if [ $exit_code -eq 0 ]; then
		echo "Python script finished successfully."
		truncate -s 0 corp_imd.jsonl
		wc -l corp_imd.jsonl
	else
		echo "Python script encountered an error. Retrying..."
	fi

	sleep 5
done


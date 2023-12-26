#!/bin/bash

# Initializing variables


echo "====INDIVIDUAL MONTHLY LOAD===="
  
while [ -s /home/ec2-user/acuris/ptest/indiv.jsonl ]; do
        echo "File is not empty. Continuing..."
        head -n 9 /home/ec2-user/acuris/ptest/indiv.jsonl >> /home/ec2-user/acuris/ptest/indiv_imd.jsonl
        head -n 9 /home/ec2-user/acuris/ptest/indiv.jsonl >> /home/ec2-user/acuris/ptest/indiv_temp.jsonl
	sed -i '1,9d' /home/ec2-user/acuris/ptest/indiv.jsonl
        wc -l /home/ec2-user/acuris/ptest/indiv_imd.jsonl
	wc -l /home/ec2-user/acuris/ptest/indiv.jsonl
	wc -l /home/ec2-user/acuris/ptest/1finale.csv
	wc -l /home/ec2-user/acuris/ptest/indiv_temp.jsonl
	python3 acuris_daily.py
        exit_code=$?
        if [ $exit_code -eq 0 ]; then
                echo "Python script finished successfully."
                truncate -s 0 /home/ec2-user/acuris/ptest/indiv_imd.jsonl
                wc -l /home/ec2-user/acuris/ptest/indiv_imd.jsonl
        else
                echo "Python script encountered an error. Retrying..."
        fi

done

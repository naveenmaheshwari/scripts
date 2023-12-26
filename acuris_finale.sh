#!/bin/bash

# Initializing variables

. /home/ec2-user/acuris/watchlist.env

cd $ETL_HOME

if [ $# -ne 1 ]; then
    echo "usage of acuris_finale.sh script: sh acuris_finale.sh mode YYYYMMDD"
    echo "mode can be M or D"
    exit 1
fi

mode=$1

if [ $mode = 'M' ]
then
        LOG_FILE=$LOG/acuris_monthly_${MONTH}.log
else
        LOG_FILE=$LOG/acuris_daily_${BUSINESS_DATE}.log
fi

echo "================= Business date is ${BUSINESS_DATE} ====================== ">> $LOG_FILE 2>&1

echo "================= Business month is ${MONTHLY} ====================== ">> $LOG_FILE 2>&1

echo "================= Running in $mode ====================== ">> $LOG_FILE 2>&1

if [ $mode = 'M' ]
then
        echo "===Monthly Update==="
	sh corp_month.sh

        sh indiv_month.sh


       	# a=${MONTH}
        #python3 acuris_monthly.py "$a"

else
        echo "===Daily Update==="
        b=${BUSINESS_DATE}
        python3 acuris_daily.py "$b"

fi

echo " ====== Target File Prepared ====== "


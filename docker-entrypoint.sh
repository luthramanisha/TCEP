#!/bin/bash
iperf3 -s -D
# ntpd complains about empty drift file otherwise
echo "0.000" >> /var/db/ntpd.drift
ntpd -s
chmod -R a+w /app/logs
java -DlogFilePath=${LOG_FILE_PATH} -cp /app/tcep_2.12-0.0.1-SNAPSHOT-one-jar.jar ${MAIN} ${ARGS}

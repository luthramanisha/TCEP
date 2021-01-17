FROM ibmjava:8-sfj-alpine

WORKDIR /app

RUN apk update && \
    apk upgrade && \
    apk add iproute2 && \
    apk add openntpd

RUN ln -s /usr/lib/tc /lib/tc
RUN sed -i '1 i\server nserver' /etc/ntpd.conf

RUN wget -O /usr/lib/libiperf.so.0 https://iperf.fr/download/ubuntu/libiperf.so.0_3.1.3
RUN wget -O /usr/bin/iperf3 https://iperf.fr/download/ubuntu/iperf3_3.1.3
RUN chmod +x /usr/bin/iperf3

COPY ./ /app/
RUN ["chmod", "+x", "/app/docker-entrypoint.sh"]

RUN mkdir logs && echo "alias myPerf='function __myalias() { iperf -c $* -f b; unset -f __myalias; }; __myalias'">> ~/.bashrc
ENTRYPOINT ["/app/docker-entrypoint.sh"]

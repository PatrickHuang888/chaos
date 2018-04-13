FROM ubuntu:16.04

ADD ./sources.list  /etc/apt/sources.list

RUN  apt-get -y -q update && \
     apt-get -yqq install ssh iputils-ping vim

#RUN apt-get -y -q update && \
#    apt-get -y -q install software-properties-common && \
#    apt-get -y -q update && \
#    apt-get install -qqy \
#        git \
#        gnuplot \
#        wget \

# You need to locate chaos in this directory (up.sh does that automatically)
ADD chaos /go/src/github.com/siddontang/chaos
ADD chaos/chaos-control /root/chaos-control

ADD ./bashrc /root/.bashrc
ADD ./init.sh /init.sh
RUN chmod +x /init.sh

CMD /init.sh

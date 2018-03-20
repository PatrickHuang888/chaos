FROM golang:1.9

#RUN echo 'deb http://mirrors.aliyun.com/ubuntu/ xenial universe' > /etc/apt/sources.list
#RUN rm /etc/apt/apt.conf.d/docker-clean
#RUN apt-get update
#RUN apt-get install -y sudo net-tools wget sysvinit-core sysvinit sysvinit-utils curl vim man faketime unzip iptables iputils-ping logrotate
#RUN apt-get remove -y --purge --auto-remove systemd


RUN apt-get update && apt-get install -y openssh-server net-tools


ADD ./authorized_keys /root/.ssh/authorized_keys

ADD http://hxm-server/tidb-v1.0.8-linux-amd64.tar.gz /root

ADD ./chaos-agent /root/chaos-agent

RUN go get -u github.com/derekparker/delve/cmd/dlv


RUN mkdir /var/run/sshd

RUN echo 'root:root' |chpasswd

RUN sed -ri 's/^PermitRootLogin\s+.*/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -ri 's/UsePAM yes/#UsePAM yes/g' /etc/ssh/sshd_config

EXPOSE 22

CMD    ["/usr/sbin/sshd", "-D"]
FROM ubuntu:22.04

RUN apt-get update \
 && apt-get install -y  \
         unzip \
         curl \
         ca-certificates \
         rpm \
         alien \
         libaio1 \
         # Install postgresql
         postgresql-client \
         # Install mysql
         libdbd-mysql \
         perl \
         libcpan-distnameinfo-perl \
         #install Perl Database Interface
         libdbi-perl \
         libpq-dev \
         libdbd-pg-perl \
         libio-compress-perl \
         libtest-nowarnings-perl \
         bzip2 \
         git \
&& 	apt-get clean

ARG ORAVERSION=21
ARG ORACLIENT_BASIC=https://download.oracle.com/otn_software/linux/instantclient/211000/oracle-instantclient-basic-21.1.0.0.0-1.x86_64.rpm
ARG ORACLIENT_DEVEL=https://download.oracle.com/otn_software/linux/instantclient/211000/oracle-instantclient-devel-21.1.0.0.0-1.x86_64.rpm
ARG ORACLIENT_SQLPLUS=https://download.oracle.com/otn_software/linux/instantclient/211000/oracle-instantclient-sqlplus-21.1.0.0.0-1.x86_64.rpm

# Install Oracle Client
RUN mkdir -p /usr/lib/oracle/$ORAVERSION/client64/network/admin \
 && curl -L -o /tmp/oracle-instantclient.rpm $ORACLIENT_BASIC \
 && (cd /tmp && alien -i ./oracle-instantclient.rpm && rm -f oracle-instantclient.rpm) \
 && curl -L -o /tmp/oracle-instantclient.rpm $ORACLIENT_DEVEL \
 && (cd /tmp && alien -i ./oracle-instantclient.rpm && rm -f oracle-instantclient.rpm) \
 && curl -L -o /tmp/oracle-instantclient.rpm $ORACLIENT_SQLPLUS \
 && (cd /tmp && alien -i ./oracle-instantclient.rpm && rm -f oracle-instantclient.rpm)

ENV ORACLE_HOME=/usr/lib/oracle/$ORAVERSION/client64 \
    TNS_ADMIN=/usr/lib/oracle/$ORAVERSION/client64/network/admin \
    LD_LIBRARY_PATH=/usr/lib/oracle/$ORAVERSION/client64/lib \
    PATH=$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/oracle/$ORAVERSION/client64/bin

# Install Oracle DBD client
RUN cpan install DBD::Oracle

ARG ORA2PG_VERSION=v23.1
RUN git clone https://github.com/Bluestep-Systems/ora2pg \
# RUN git clone https://github.com/darold/ora2pg.git \
  && (cd ora2pg && git checkout $ORA2PG_VERSION && perl Makefile.PL && make && make install && rm -r ../ora2pg)

ENTRYPOINT ["/usr/local/bin/ora2pg"]

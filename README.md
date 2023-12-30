Kafka FTP Source Connect
=================

[![Build Status](https://travis-ci.org/Eneco/kafka-connect-ftp.svg?branch=master)](https://travis-ci.org/Eneco/kafka-connect-ftp)

FTP Source Connector


-----

### Properties


| name                        | data type | required | default      | description                                   |
|:----------------------------|:----------|:---------|:-------------|:----------------------------------------------|
| `ftp.server`               | string    | yes      | -            | host\[:port\] of the ftp server               |
| `username`                  | string    | yes      | -            | ftp username                                      |
| `password`              | string    | yes      | -            | ftp password                                      |



Usage
-----

Build gradle.

    ./gradlew fetJar


Specify a class path in the Kafka connection plugin path

    plugin.path={FTP Source Connector Path}/build/libs

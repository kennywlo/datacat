# Copyright 2021 SLAC for the benefit of the SuperCDMS collaboration.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Authors:
# - Kenny Lo <kennywlo@slac.stanford.edu>, 2021

# To build this image to use with docker-compose:
#   docker build -t slac-lab/datacat:INT_TESTING .

FROM centos:7

MAINTAINER kennylo@slac.stanford.edu

RUN yum -y install git vim

ENV DCHOME=/opt/datacat
RUN mkdir -p $DCHOME

# copy over the current slaclab-datacat repo
ADD . $DCHOME

WORKDIR $DCHOME
RUN yum -y install maven
RUN mvn package -DskipTests

CMD ./docker/cp_config_files.sh
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y curl wget unzip git openssh-client ca-certificates gnupg lsb-release build-essential libgmp-dev rsync
RUN wget https://releases.hashicorp.com/vagrant/2.4.1/vagrant_2.4.1-1_amd64.deb
RUN dpkg -i vagrant_2.4.1-1_amd64.deb && rm vagrant_2.4.1-1_amd64.deb
WORKDIR /app
RUN vagrant plugin install vagrant-google --plugin-version 2.7.0
RUN sed -i 's/def self.wsl?/def self.wsl?; return false;/' /opt/vagrant/embedded/gems/gems/vagrant-2.4.1/lib/vagrant/util/platform.rb
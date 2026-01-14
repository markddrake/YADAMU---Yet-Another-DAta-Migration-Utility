FROM ubuntu:22.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# Install curl, jq (for parsing secureConnections.json), and unzip (for SnowSQL installer)
RUN apt-get update && \
    apt-get install -y curl jq unzip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install SnowSQL (latest version)
RUN curl -o snowsql.bash \
    https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-1.2.28-linux_x86_64.bash && \
    SNOWSQL_DEST=/usr/local SNOWSQL_LOGIN_SHELL=~/.profile bash snowsql.bash && \
    rm snowsql.bash

# Symlink for easier access
RUN ln -s /usr/local/snowsql /usr/local/bin/snowsql

WORKDIR /scripts

COPY --from=sql / sql
COPY --from=conn secureConnections.json .
COPY configure.sh .

# Default to opening a shell so scripts can be run interactively
CMD ["/bin/bash"]

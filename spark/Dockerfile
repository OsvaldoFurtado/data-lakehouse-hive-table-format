FROM bitnami/spark:3.5.3
USER root
RUN install_packages curl

# Install additional packages
RUN apt-get update && \
    apt-get install -y openssh-server sudo

# Configure SSH
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# Add a user named "me" with UID 1001 and set password to "changeme"
RUN useradd --uid 1001 --create-home --shell /bin/bash --user-group me && \
    echo 'me:changeme' | chpasswd

# Add "me" to the sudoers file
RUN echo 'me ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/me

# Start SSH server in the background
CMD sudo service ssh start && /opt/bitnami/scripts/spark/run.sh

EXPOSE 22

USER me
RUN rm -r /opt/bitnami/spark/jars && \
    curl --location https://dlcdn.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3-scala2.13.tgz | \
    tar --extract --gzip --strip=1 --directory /opt/bitnami/spark/ spark-3.5.3-bin-hadoop3-scala2.13/jars

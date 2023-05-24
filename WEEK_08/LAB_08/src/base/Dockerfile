FROM archlinux/base

# Install Java and other dependencies
RUN pacman -Syu --noconfirm && \
    pacman -S --noconfirm jdk8-openjdk wget

# Download and extract Hadoop
RUN wget -qO- https://downloads.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz | tar zx -C /opt && \
    mv /opt/hadoop-3.3.1 /opt/hadoop

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin

# Copy the entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]

# Thin layer over the pinned Apache Hadoop image: bakes in our config and pre-creates the
# data dirs owned by the non-root `hadoop` user so fresh named volumes inherit that ownership.
FROM apache/hadoop:3.4.3
COPY --chown=hadoop:users core-site.xml hdfs-site.xml httpfs-site.xml /opt/hadoop/etc/hadoop/
COPY --chown=hadoop:users --chmod=0755 start-namenode.sh /opt/f1sa/start-namenode.sh
RUN mkdir -p /opt/hadoop/data/name /opt/hadoop/data/data

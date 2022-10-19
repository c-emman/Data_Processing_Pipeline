from subprocess import call

def initialise():
    call(["gnome-terminal", "-x", "sh", "-c", "~/kafka_2.12-3.2.1/bin/zookeeper-server-start.sh ~/kafka_2.12-3.2.1/config/zookeeper.properties; bash"])

    call(["gnome-terminal", "-x", "sh", "-c", "~/kafka_2.12-3.2.1/bin/kafka-server-start.sh ~/kafka_2.12-3.2.1/config/server.properties; bash"])
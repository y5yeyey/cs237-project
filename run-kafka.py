# -*- coding: utf-8 -*-

import os
import subprocess as sb
import argparse

KAFKA = "kafka_2.11-0.10.2.0"
KAFKA_CONF = "/home/congweiw/kafka_2.11-0.10.2.0/config/server.properties"
KAFKA_LOG = "/home/congweiw/kafka_logs"
ZOOKEEPER_CLUSTER = "/home/congweiw/zookeeper.info"

def arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--group")
    parser.add_argument("--run")
    args = parser.parse_args()
    return args.group, args.run

def getProcess(cmd):
    ps = sb.Popen(cmd, shell=True, stdin=sb.PIPE, stdout=sb.PIPE, stderr=sb.PIPE)
    print cmd
    out, err = ps.communicate()
    if err:
        print err
    ps.stdin.close()
    return out, err

def install(name, zone):
    cmd = 'gcloud compute ssh {name} \
            --zone {zone} \
            --command "yes | sudo apt install -t jessie-backports openjdk-8-jre-headless ca-certificates-java"'.format(
            name=name, zone=zone)
    out, err = getProcess(cmd)
    # setup file directory
    cmd = 'gcloud compute ssh {name} --zone {zone} --command "rm -rf *; mkdir {path}"'.format(name=name, zone=zone, path=KAFKA_LOG)
    out, err = getProcess(cmd)
    print "Finish {name} java installation and log/data directory.".format(name=name)
                                                    
def scp(i, name, zone):
    kafka_port = "9093"
    zoo_port = "2181"
    # copy kafka directory to remote machine
    cmd = "gcloud compute copy-files {app} {name}:{app} --zone {zone}".format(app=KAFKA, name=name, zone=zone)
    out, err = getProcess(cmd)
    # obtain zookeeper cluster info
    ip_ports = []
    with open(ZOOKEEPER_CLUSTER, "r") as f:
        for line in f:
            ip_ports += [line.split(":")[0] + ":" + zoo_port]
    # assign id
    commands = [
        "broker.id={i}".format(i=i),
        "listeners=PLAINTEXT://:{kafka_port}".format(kafka_port=kafka_port),
        "log.dir={log}".format(log=KAFKA_LOG),
        "zookeeper.connect={ip_ports}".format(ip_ports=",".join(ip_ports))
    ]
    for command in commands:
        cmd = 'gcloud compute ssh {name} --zone {zone} --command "echo -e \"{c}\" >> {app_conf}"'.format(
            name=name, zone=zone, c=command, app_conf=KAFKA_CONF
        )
        out, err = getProcess(cmd)
    print "The [{i}] {name} is assigned.".format(i=i, name=name)

def getIP(name, zone):
    cmd = 'gcloud compute ssh {name} --zone {zone} --command "hostname -I"'.format(name=name, zone=zone)
    out, err = getProcess(cmd)
    return out.strip().split("\n")[-1]

def deploy(name, zone):
    cmd = 'gcloud compute ssh {name} \
            --zone {zone} \
            --command "nohup {kafka}/bin/kafka-server-start.sh {conf}" &'.format(name=name, zone=zone, kafka=KAFKA, conf=KAFKA_CONF)
    os.spawnl(os.P_NOWAIT, cmd)
    # out, err = getProcess(cmd)
    print "Deploy {name} kakfa.".format(name=name)

def stop(name, zone):
    cmd = 'gcloud compute ssh {name} \
            --zone {zone} \
            --command "pkill -u congweiw"'.format(name=name, zone=zone)
    out, err = getProcess(cmd)
    print "Stop {name} kafka.".format(name=name)

def run(instance_group, command):

    commands = ["install", "deploy", "stop"]
    # non-expected command
    if command not in commands:
        print "Please enter valid command!"
        return

    cmd = "gcloud compute instance-groups list-instances {group} --zone us-central1-c".format(group=instance_group) 
    out, err = getProcess(cmd)
    list_of_machines = [[i+1]+_.split("  ") for i, _ in enumerate(out.split("\n")[1:]) if _]
    
    # prepare config file
    for machine in list_of_machines:
        i, name, zone, status = machine
        # install packages only
        if command == "install":
            install(name, zone)
        elif command == "stop":
            stop(name, zone)
        elif command == "deploy":
            # ip = getIP(name, zone)
            scp(i,name, zone)

    if command in ("install", "stop"):
        print "Successfully {command}.".format(command=command)
        return

    if command == "deploy":
        for machine in list_of_machines:
            i, name, zone, status = machine
            deploy(name, zone)
    
    print "Successfully {command}.".format(command=command)


if __name__ == "__main__":
    try:
        group, command = arg_parser()
        run(group, command)
    except:
        raise
        sys.exit(1)

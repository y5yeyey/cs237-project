# -*- coding: utf-8 -*-

import subprocess as sb
import argparse

ZOOKEEPER = "zookeeper-3.4.10"
ZOO_FILE = "zookeeper-3.4.10/conf/zoo_sample.cfg"
ZOO_CONF = "zookeeper-3.4.10/conf/zoo.cfg"
ZOO_DATA = "/home/congweiw/zookeeper_data"
ZOO_LOG = "/home/congweiw/zookeeper_logs"
CLUSTER_INFO = "/home/congweiw/zookeeper.info"


def arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--group")
    parser.add_argument("--run")
    args = parser.parse_args()
    return args.group, args.run

def getProcess(cmd):
    ps = sb.Popen(cmd, shell=True, stdin=sb.PIPE, stdout=sb.PIPE, stderr=sb.PIPE)
    out, err = ps.communicate()
    if err:
        print err
    ps.stdin.close()
    return out, err

def install(name, zone):
    cmd = 'gcloud compute ssh {name} \
            --zone {zone} \
            --command "sudo apt-get update"'.format(
            name=name, zone=zone)
    out, err = getProcess(cmd)
    cmd = 'gcloud compute ssh {name} \
            --zone {zone} \
            --command "yes | sudo apt install -t jessie-backports openjdk-8-jre-headless ca-certificates-java"'.format(
            name=name, zone=zone)
    out, err = getProcess(cmd)
    # setup file directory
    cmd = 'gcloud compute ssh {name} --zone {zone} --command "rm -rf *; mkdir {log}; mkdir {data}"'.format(name=name, zone=zone, log=ZOO_LOG, data=ZOO_DATA)
    out, err = getProcess(cmd)
    print "Finish {name} java installation and log/data directory.".format(name=name)
                                                    
def scp(i, name, zone):
    # copy zookeeper directory to remote machine
    cmd = "gcloud compute copy-files {zk} {name}:{zk} --zone {zone}".format(zk=ZOOKEEPER, name=name, zone=zone)
    out, err = getProcess(cmd)
    # assign id
    cmd = 'gcloud compute ssh {name} --zone {zone} --command "echo \"{i}\" > {zoo_data}/myid"'.format(name=name, zone=zone, i=i, zoo_data=ZOO_DATA)
    out, err = getProcess(cmd)
    print "The [{i}] {name} is assigned.".format(i=i, name=name)

def getIP(name, zone):
    cmd = 'gcloud compute ssh {name} --zone {zone} --command "hostname -I"'.format(name=name, zone=zone)
    out, err = getProcess(cmd)
    return out.strip().split("\n")[-1]

def clean(name, zone):
    cmd = 'gcloud compute ssh {name} --zone {zone} --command "rm -rf *"'.format(name=name, zone=zone)
    out, err = getProcess(cmd)

def deploy(name, zone):
    cmd = 'gcloud compute ssh {name} \
            --zone {zone} \
            --command "{zk}/bin/zkServer.sh start"'.format(name=name, zone=zone, zk=ZOOKEEPER)
    out, err = getProcess(cmd)
    print "Deploy {name} zookeeper.".format(name=name)

def stop(name, zone):
    cmd = 'gcloud compute ssh {name} \
            --zone {zone} \
            --command "pkill -u congweiw"'.format(name=name, zone=zone)
    out, err = getProcess(cmd)
    print "Stop {name} zookeeper.".format(name=name)

def run(instance_group, command):

    commands = ["install", "deploy", "stop"]
    # non-expected command
    if command not in commands:
        print "Please enter valid command!"
        return

    cmd = "gcloud compute instance-groups list-instances {group} --zone us-east1-d".format(group=instance_group) 
    out, err = getProcess(cmd)
    list_of_machines = [[i+1]+_.split("  ") for i, _ in enumerate(out.split("\n")[1:]) if _]
    
    # prepare config file
    zoo_file = ""
    port1 = "2888"
    port2 = "3888"
    server_info = ["dataDir={zoo_data}".format(zoo_data=ZOO_DATA), "dataLogDir={zoo_log}".format(zoo_log=ZOO_LOG)]
    for machine in list_of_machines:
        i, name, zone, status = machine
        print name, zone
        # install packages only
        if command == "install":
            install(name, zone)
            continue
        elif command == "stop":
            stop(name, zone)
            continue
        ip = getIP(name, zone)
        server_info += ["server.{i}={ip}:{port1}:{port2}".format(i=i, ip=ip, port1=port1, port2=port2)]
    
    # save cluster config file
    if command == "deploy":
        with open(CLUSTER_INFO, "w") as f:
            for line in server_info[2:]:
                f.write(":".join("".join(line.split("=")[1:]).split(":")[:2]) + "\n")
        print "Output cluster info."

    if command in ("install", "stop"):
        print "Successfully {command}.".format(command=command)
        return
    
    out, err = getProcess("cp {zoo_file} {zoo}".format(zoo_file=ZOO_FILE, zoo=ZOO_CONF))
    with open(ZOO_CONF, "a") as f:
        f.write("\n".join(server_info) + "\n")
    print "The config file is ready."

    # scp on each machine
    for machine in list_of_machines:
        i, name, zone, status = machine
        scp(i, name, zone)

    # deploy on each machine
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

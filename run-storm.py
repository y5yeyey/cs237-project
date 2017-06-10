# -*- coding: utf-8 -*-

import subprocess as sb
import argparse

STORM = "apache-storm-1.1.0"
STORM_DATA = "/home/congweiw/storm_data"


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
            --command "yes | sudo apt install -t jessie-backports openjdk-8-jre-headless ca-certificates-java"'.format(
            name=name, zone=zone)
    out, err = getProcess(cmd)
    # setup file directory
    cmd = 'gcloud compute ssh {name} --zone {zone} --command "rm -rf *; mkdir {data}"'.format(name=name, zone=zone, data=STORM_DATA)
    out, err = getProcess(cmd)
    print "Finish {name} java installation and log/data directory.".format(name=name)
                                                    
def scp(i, name, zone):
    # copy storm directory to remote machine
    cmd = "gcloud compute copy-files {storm} {name}:{storm} --zone {zone}".format(storm=STORM, name=name, zone=zone)
    out, err = getProcess(cmd)
    print "The [{i}] {name} is assigned.".format(i=i, name=name)

def clean(name, zone):
    cmd = 'gcloud compute ssh {name} --zone {zone} --command "rm -rf *"'.format(name=name, zone=zone)
    out, err = getProcess(cmd)

def deploy(name, zone):
    cmd = 'gcloud compute ssh {name} \
            --zone {zone} \
            --command "{storm}/bin/storm supervisor"'.format(name=name, zone=zone, storm=STORM)
    out, err = getProcess(cmd)
    print "Deploy {name} storm supervios.".format(name=name)

def stop(name, zone):
    cmd = 'gcloud compute ssh {name} \
            --zone {zone} \
            --command "pkill -u congweiw"'.format(name=name, zone=zone)
    out, err = getProcess(cmd)
    print "Stop {name} storm.".format(name=name)

def run(instance_group, command):
    commands = ["install", "deploy", "stop"]
    # non-expected command
    if command not in commands:
        print "Please enter valid command!"
        return

    cmd = "gcloud compute instance-groups list-instances {group} --zone us-west1-a".format(group=instance_group) 
    out, err = getProcess(cmd)
    list_of_machines = [[i+1]+_.split("  ") for i, _ in enumerate(out.split("\n")[1:]) if _]
    
    for machine in list_of_machines:
        i, name, zone, status = machine
        # install packages only
        if command == "install":
            install(name, zone)
            continue
        elif command == "stop":
            stop(name, zone)
            continue
    
    if command in ("install", "stop"):
        print "Successfully {command}.".format(command=command)
        return
    
    # scp on each machine
    for machine in list_of_machines:
        i, name, zone, status = machine
        scp(i, name, zone)

    # deploy on each machine
    for machine in list_of_machines:
        i, name, zone, status = machine
        # deploy(name, zone)
    
    print "Successfully {command}.".format(command=command)


if __name__ == "__main__":
    try:
        group, command = arg_parser()
        run(group, command)
    except:
        raise
        sys.exit(1)

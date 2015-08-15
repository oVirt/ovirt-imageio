#!/bin/sh -ev

IP=192.168.1.52

ssh root@${IP} 'mkdir -p /root/ovirt-image-proxy'
scp -rp * root@${IP}:/root/ovirt-image-proxy/

#!/bin/sh

echo > /etc/environment
# systemd doesn't inherit environment from root, workaround for this.
for e in $(tr "\000" "\n" < /proc/1/environ); do
  if [[ $e == *"="* ]]; then
    echo $e >> /etc/environment
  fi
done
echo "PYTHONPATH=/high-flyer/code/multi_gpu_runner_server" >> /etc/environment

[[ ! `cat /etc/environment | grep -e "^NAMESPACE="` ]] && echo "NAMESPACE is not set" && exit 1;
[[ ! `cat /etc/environment | grep -e "^TASK_ID="` ]] && echo "TASK_ID is not set" && exit 1;
[[ ! `cat /etc/environment | grep -e "^HW_TEST_LOGPATH="` ]] && echo "HW_TEST_LOGPATH is not set" && exit 1;

echo "manager mkdir";
NAMESPACE=`cat /etc/environment | grep -e "^NAMESPACE=" | awk -F '=' '{print $2}'`
TASK_ID=`cat /etc/environment | grep -e "^TASK_ID=" | awk -F '=' '{print $2}'`
HW_TEST_LOGPATH=`cat /etc/environment | grep -e "^HW_TEST_LOGPATH=" | awk -F '=' '{print $2}'`
mkdir -p /var/log/experiment_manager_log/${NAMESPACE}_${TASK_ID};
mkdir -p ${HW_TEST_LOGPATH}/manager;
echo "finished and exit";

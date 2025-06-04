#!/bin/bash

manager_services=("manager_init.service" "manager_check_logs.service" "manager_check_resource_released.service" \
                  "manager_check_running.service" "manager_check_unschedulable.service" "manager_stop_func.service" \
                  "manager_suspend_func.service")

inactive_services=""
for svc in ${manager_services[@]}; do
  if [[ `systemctl is-active $svc` != "active" ]]; then
    inactive_services+="$svc;"
  fi
done

if [[ $inactive_services != "" ]]; then
  echo "inactive_services: $rst"
  exit 1;
fi

echo "all services are active."
exit 0;

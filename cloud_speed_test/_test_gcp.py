import subprocess
from  ._test_base import run_test


import pandas as pd
from google.cloud import compute_v1

def gcp_machines_from_zone(project_id, zone):
    client = compute_v1.MachineTypesClient()
    machine_types_data = []

    request = compute_v1.ListMachineTypesRequest(project=project_id, zone=zone)
    for machine_type in client.list(request=request):
        data = {
            "Machine Type": machine_type.name,
            "CPUs": machine_type.guest_cpus,
            "Memory (GiB)": machine_type.memory_mb / 1024,  # Convert MB to GiB
            "GPUs": 0  # Default to 0, adjust below if GPU info is available
        }
        # Assuming GPU info needs to be manually adjusted since it's not directly available from machine_type
        machine_types_data.append(data)

    df = pd.DataFrame(machine_types_data)
    return df

class run_gcp_test(run_test):
    def __init__(self,test_name,docker,base_args,project,zone,min_cores=0,min_mem=0):

        super().__init__(test_name,docker,base_args,min_cores,min_mem)
        self._project=project
        self._zone=zone

    def create_instance_command(self,machine_type, instance_name,image_family="debian-12",wrap=False):
        res=[
        "gcloud", "compute", "instances", "create", instance_name,
        "--machine-type", machine_type,
        "--image-family", image_family,
        "--image-project", "debian-cloud",
        "--zone", self._zone,
        "--project", self._project]
        if wrap:
            res.append("'--metadata=startup-script=sudo apt-get update && sudo apt-get install -y docker.io'")
        else:
            res.append("--metadata=startup-script=sudo apt-get update && sudo apt-get install -y docker.io")
        return res
    
    def run_docker_command(self,instance_name, docker, command_args):
        docker_run_command = f"/usr/bin/sudo /usr/bin/docker run {docker} {command_args}"

        ssh_command = f"gcloud compute ssh {instance_name} --zone {self._zone} --project {self._project} --command \"{docker_run_command}\""
        return ssh_command
    
    def delete_instance_command(self,instance_name):
        return [
        "gcloud", "compute", "instances", "delete", instance_name,
        "--zone", self._zone,
        "--project", self._project,
        "--quiet"
        ]

    def valid_machine_name(self,machine_type):
        undescore_name=machine_type
        undescore_name.replace('-', '_')
        instance_name = f"{self._test_name}-{undescore_name}"
        return instance_name



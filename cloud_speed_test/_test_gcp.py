import subprocess
from  ._test_base import run_test



class run_gcp_test(run_test):
    def __init__(self,test_name,docker,base_args,project,zone,min_cores=0,min_mem=0):

        super().__init__(test_name,docker,base_args,min_cores,min_mem)
        self._project=project
        self._zone=zone

    def create_instance_command(self,machine_type, instance_name,wrap=False):
        res=[
        "gcloud", "compute", "instances", "create", instance_name,
        "--machine-type", machine_type,
        "--image-family", "debian-10",
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


            
def gcp_machines_from_zone(zone):
    result=subprocess.run([
        "gcloud", "compute", "machine-types", "list",
        "--zones=us-central1-c" ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True, check=True)
    gcloud_output=result.stdout
    df = pd.read_csv(StringIO(gcloud_output), sep="\s+", skipinitialspace=True)
    return df

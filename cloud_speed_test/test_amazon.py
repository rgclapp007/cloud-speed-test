from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
import pandas as pd
from ._test_base import run_test

def azure_vms_from_region(subscription_id, location):
    credential = DefaultAzureCredential()
    compute_client = ComputeManagementClient(credential, subscription_id)
    vm_sizes = compute_client.virtual_machine_sizes.list(location)
    
    vms_data = [{
        "Machine Type": vm.name,
        "CPUs": vm.number_of_cores,
        "Memory (GiB)": vm.memory_in_mb / 1024,
        "GPUs": 0
    } for vm in vm_sizes]
    
    return pd.DataFrame(vms_data)

class run_azure_test(run_test):
    def __init__(self, test_name, docker, base_args, subscription_id, location, resource_group, min_cores=0, min_mem=0):
        super().__init__(test_name, docker, base_args, min_cores, min_mem)
        self._subscription_id = subscription_id
        self._location = location
        self._resource_group = resource_group
    
    def create_instance_command(self, machine_type, instance_name, image_id="ami-123456", wrap=False):
        ec2 = boto3.resource('ec2', region_name=self._region_name)
        instance = ec2.create_instances(
            ImageId=image_id,
            InstanceType=machine_type,
            MinCount=1,
            MaxCount=1,
            # Additional parameters as needed (e.g., KeyName for SSH access, SecurityGroupIds, etc.)
        )[0]
        instance.wait_until_running()
        # You may want to tag the instance or perform additional setup here
        return instance.id  # Return the instance ID or other relevant info

    def run_docker_command(self, instance_name, docker, command_args):
        # Running Docker commands on AWS EC2 instances typically involves SSH access
        # This step might require setting up SSH access and executing the command over SSH
        # Placeholder for SSH command execution logic
        pass

    def delete_instance_command(self, instance_name):
        ec2 = boto3.resource('ec2', region_name=self._region_name)
        instance = ec2.Instance(instance_name)
        response = instance.terminate()
        return response  # Return termination status or other relevant info

    def valid_machine_name(self, machine_type):
        # AWS does not impose strict naming conventions for EC2 instances like Azure does for VM names
        # However, you might still want to standardize the names for your own tracking purposes
        valid_name = machine_type.replace('_', '-')
        return f"{self._test_name}-{valid_name}"

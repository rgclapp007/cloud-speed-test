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
        # Azure doesn't provide GPU details in this API, additional steps required
        "GPUs": 0
    } for vm in vm_sizes]
    
    return pd.DataFrame(vms_data)

class run_azure_test(run_test):
    def __init__(self, test_name, docker, base_args, subscription_id, location, resource_group, min_cores=0, min_mem=0):
        super().__init__(test_name, docker, base_args, min_cores, min_mem)
        self._subscription_id = subscription_id
        self._location = location
        self._resource_group = resource_group
    

    def create_instance_command(self, machine_type, instance_name, image_family="UbuntuLTS", wrap=False):
        # Create or update a virtual machine
        # Note: This is a simplified example. A real implementation would require handling networking, storage, etc.
        compute_client = ComputeManagementClient(self.credential, self.subscription_id)
        vm_parameters = {
            # Define VM parameters here, including VM size (machine_type), image reference, etc.
        }
        async_vm_creation = compute_client.virtual_machines.begin_create_or_update(
            self.resource_group_name,
            instance_name,
            vm_parameters
        )
        vm_info = async_vm_creation.result()
        return vm_info.id  # Return VM ID or other relevant info as needed

    def run_docker_command(self, instance_name, docker, command_args):
        # Running Docker commands on Azure VMs typically involves using SSH or Azure Command Execution features
        # This step might require setting up SSH access or using Azure Run Command
        # Placeholder for command execution logic
        pass

    def delete_instance_command(self, instance_name):
        compute_client = ComputeManagementClient(self.credential, self.subscription_id)
        async_vm_deletion = compute_client.virtual_machines.begin_delete(
            self.resource_group_name,
            instance_name
        )
        deletion_info = async_vm_deletion.result()
        return deletion_info  # Return deletion status or other relevant info as needed

    def valid_machine_name(self, machine_type):
        # Azure has naming restrictions for VMs
        # This method can be used to ensure the VM name meets Azure's requirements
        valid_name = machine_type.replace('_', '-')
        return f"{self._test_name}-{valid_name}"

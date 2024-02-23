import subprocess
import time
import pandas as pd
from abc import ABC, abstractmethod
from concurrent.futures import ProcessPoolExecutor


def run_check_command(command,verbose=0):
    if verbose>1:
        if isinstance(command,list):
            print(f"Running: {' '.join(command)}")
            result = subprocess.run(command,
                            stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        else:
            print(f"Running: {command}")
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
    else:
        return result.stdout 
    
class run_test(ABC):
    def __init__(self,test_name,docker,base_args,min_cpus=0,min_mem=0):
        self._docker=docker
        self._base_args=base_args
        self._docker_start_time=90
        self._min_mem=min_mem
        self._min_cpus=min_cpus
        self._test_name=test_name
        self.docker_start_time=70

    @abstractmethod
    def create_instance_command(self,machine_type,instance_name,image_family="debian-10"):
        pass

    @abstractmethod
    def run_docker_command(self,instance_name,docker,command_args):
        pass
    
    @abstractmethod
    def delete_instance_command(self,instance_name):
        pass
        
    @abstractmethod
    def parse_output(self,return_string):
        pass
    
    def valid_machine_name(self,machine_type):
        return f"{self._test_name}-{machine_type}"
    
    def add_threads(self,com_string,nthreads):
        return com_string
    
    def valid_job(self,mach,cpus,mem):
        if mem<self._min_mem: return False
        if cpus<self._min_cpus: return False
        return True
    
def add_test(df, test, max_workers=45, verbose=0, retry_delay=90, max_retries=3):
    if 'Machine Type' not in df.columns:
        raise ValueError("DataFrame must contain a 'Machine Type' column")
    
    retry_queue = []
    retries = 0

    while True:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Prepare tasks, either initial or retry queue
            tasks = retry_queue if retries > 0 else df[['Machine Type', 'CPUs', 'Memory (GiB)', 'GPUs']].to_dict('records')
            retry_queue = []  # Reset retry queue
            
            # Execute the processing in parallel
            results = list(executor.map(run_single_test, tasks, [test]*len(tasks), [verbose]*len(tasks)))
            
            # Process results and populate retry queue if necessary
            for row_data, (status, speed) in zip(tasks, results):
                if status == 'success':
                    df.loc[df['Machine Type'] == row_data['Machine Type'], test._test_name] = speed
                elif status == 'unavailable' and retries < max_retries:
                    retry_queue.append(row_data)
                else:       
                    retry_queue.append(row_data)
        if not retry_queue or retries >= max_retries:
            break
        else:
            retries += 1
            if verbose > 0: print(f"Retrying. Attempt {retries}/{max_retries}")
            time.sleep(retry_delay)  # Wait before retrying
    
    if retries >= max_retries:
        for row_data in retry_queue:
            df.loc[df['Machine Type'] == row_data['Machine Type'], test._test_name] = 'Unavailable after retries'
    
    return df

def run_single_test(row_data, test, verbose=0):
    # Initialize status and speed
    status = 'success'
    speed = -999.
    
    # Unpack row data
    dv = {'Machine Type': row_data['Machine Type'], 'CPUs': row_data['CPUs'], 'Memory (GiB)': row_data['Memory (GiB)']}
    
    if test.valid_job(dv["Machine Type"], dv["CPUs"], dv["Memory (GiB)"]):
        instance_name = test.valid_machine_name(dv["Machine Type"])
        create_result = run_check_command(test.create_instance_command(dv["Machine Type"], instance_name), verbose)
        if create_result is None:
            status = "failed"
        else:
            if "unavailable" in create_result.lower():  # Simulated check for unavailability
                status = 'unavailable'
            else:
                if verbose > 1:
                    print(f"{instance_name} sleeping for docker to be installed")
                time.sleep(120)
                speed = test.parse_output(run_check_command(
                    test.run_docker_command(instance_name, test._docker, test.add_threads(test._base_args, int(dv["CPUs"] / 2))), verbose))
                run_check_command(test.delete_instance_command(instance_name), verbose)
                if verbose > 0: print(f"Finished testing {dv['Machine Type']} in {speed}")
    else:
        if verbose > 0: print(f"Not running on {dv['Machine Type']}")
        status = 'invalid'
    
    return status, speed
def test_run(df, test, mach):
    # Ensure 'NAME' is the DataFrame's index for direct access
    if 'Machine Type' not in df.index.names:
        df = df.set_index('Machine Type')

    # Check if the machine name exists in the DataFrame
    if mach in df.index:
        one_row = df.loc[mach, ['CPUs', 'Memory (GiB)']].to_dict()
        
        # Assuming 'NAME' is the index, you don't need to extract it again from the row
        one_row['Machine Type'] = mach
        
        if test.valid_job(one_row["Machine Type"], one_row["CPUs"], one_row["Memory (GiB)"]):
            instance_name = test.valid_machine_name(one_row["Machine Type"])
            print(f"{' '.join(test.create_instance_command(one_row['Machine Type'], instance_name,True))}")
            nth_command = test.add_threads(test._base_args, int(one_row['CPUs'] / 2))
            print(f"{test.run_docker_command(instance_name, test._docker, nth_command)}")
            print(f"{' '.join(test.delete_instance_command(instance_name))}")
    else:
        print(f"Machine name {mach} not found in DataFrame.")


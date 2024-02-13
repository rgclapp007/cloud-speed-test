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
    def create_instance_command(slf,machine_type,instance_name):
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

def run_single_test(row_data, test,verbose=0):
    # Unpack row data
    dv = {'NAME': row_data['NAME'], 'CPUS': row_data['CPUS'], 'MEMORY_GB': row_data['MEMORY_GB']}
    
    if test.valid_job(dv["NAME"], dv["CPUS"], dv["MEMORY_GB"]):
        instance_name = test.valid_machine_name(dv["NAME"])
        result=run_check_command(test.create_instance_command(dv["NAME"], instance_name),verbose)
        if verbose>1:
            print(f"{instance_name} sleeping for docker to be installed")
        time.sleep(120)
        speed=test.parse_output(run_check_command(
            test.run_docker_command(instance_name,test._docker,test.add_threads(test._base_args,
                                                                  int(dv["CPUS"] / 2))),verbose))
        run_check_command(test.delete_instance_command(instance_name),verbose)
        if verbose >0: print(f"Finished testing {dv['NAME']} in {speed}")
        return speed
    else:
        if verbose >0: print(f"Not running on {dv['NAME']}")
        return -999.

def add_test(df, test,verbose=0):
    # Ensure 'NAME' column exists
    if 'NAME' not in df.columns:
        raise ValueError("DataFrame must contain a 'NAME' column")

    # Specify 8 workers for the ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=8) as executor:
        # Prepare data for multiprocessing
        # Note: Ensure we're not modifying the original DataFrame's structure
        rows_data = df[['NAME', 'CPUS', 'MEMORY_GB']].head(8).to_dict('records')
        
        # Execute the processing in parallel
        speeds = list(executor.map(run_single_test, rows_data, [test]*len(rows_data),[verbose]*len(rows_data)))
        
    # Update the DataFrame with the speeds
    for row_data, speed in zip(rows_data, speeds):
        df.loc[df['NAME'] == row_data['NAME'], test._test_name] = speed
    return df

def test_run(df, test, mach):
    # Ensure 'NAME' is the DataFrame's index for direct access
    if 'NAME' not in df.index.names:
        df = df.set_index('NAME')

    # Check if the machine name exists in the DataFrame
    if mach in df.index:
        one_row = df.loc[mach, ['CPUS', 'MEMORY_GB']].to_dict()
        
        # Assuming 'NAME' is the index, you don't need to extract it again from the row
        one_row['NAME'] = mach
        
        if test.valid_job(one_row["NAME"], one_row["CPUS"], one_row["MEMORY_GB"]):
            instance_name = test.valid_machine_name(one_row["NAME"])
            print(f"{' '.join(test.create_instance_command(one_row['NAME'], instance_name,True))}")
            nth_command = test.add_threads(test._base_args, int(one_row['CPUS'] / 2))
            print(f"{test.run_docker_command(instance_name, test._docker, nth_command)}")
            print(f"{' '.join(test.delete_instance_command(instance_name))}")
    else:
        print(f"Machine name {mach} not found in DataFrame.")


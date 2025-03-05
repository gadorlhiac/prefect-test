import json
import os
import time
import yaml
from datetime import datetime
from typing import Optional, Any, Dict, List, Union
from typing_extensions import TypedDict

from prefect import flow, task
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner

#from .tasks.tasks import *

class FlowConf(TypedDict):
    experiment: str
    run_id: str
    JID_UPDATE_COUNTERS: Optional[str]
    ARP_ROOT_JOB_ID: str
    ARP_LOCATION: str
    Authorization: str
    user: str
    lute_location: str
    kerb_file: Optional[str]
    lute_params: Dict[str, Union[str, bool]]
    slurm_params: List[str]
    workflow: str #Dict[str, Any]

flow_name: str = f"lute_{os.path.splitext(os.path.basename(__file__))[0]}"

@task
def run_managed_task(lute_task_id: str) -> str:
    print(f"Running: {lute_task_id}")
    return "SUCCESS"

def create_workflow(
    wf_dict: Dict[str, Any],
    wait_for = [],
    all_futures = [],
) -> None:
    slurm_params: str = wf_dict.get("slurm_params", "")
    future = run_managed_task.submit(wf_dict["task_name"], wait_for=wait_for)
    all_futures.append(future)
    if wf_dict["next"] == []:
        return
    else:
        child_tasks = []
        for task in wf_dict["next"]:
            create_workflow(task, [future])
    return

@flow(name=flow_name, task_runner=ThreadPoolTaskRunner(max_workers=8), log_prints=True)
def dynamic_flow(flow_conf: FlowConf) -> None:

    wf_dict: Dict[str, str] = json.loads(conf.get("workflow"))

    wait_for = []
    all_futures = []
    create_workflow(wf_dict, wait_for, all_futures)
    wait(all_futures)

if __name__ == "__main__":
    with open("test_dag.yaml", "r") as f:
        wf_dict = yaml.load(stream=f, Loader=yaml.FullLoader)
    conf: FlowConf = {
        "experiment": "mfx",
        "run_id": "123",
        "JID_UPDATE_COUNTERS": None,
        "ARP_ROOT_JOB_ID": "",
        "ARP_LOCATION": "S3DF",
        "Authorization": "auth",
        "user": "dorlhiac",
        "lute_location": "~",
        "kerb_file": None,
        "lute_params": {"a":1},
        "slurm_params": ["--nodes=1"],
        "workflow": json.dumps(wf_dict),
    }
    dynamic_flow.deploy(
        name="test-deployment",
        work_pool_name="psdm-prefect-workers",
    )

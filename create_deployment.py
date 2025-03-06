import json
import yaml
from typing import Any, Dict

from prefect import flow
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner

from flow_dataclasses import FlowConf
from tasks.jidtasks import run_managed_task

flow_name: str = f"lute_dynamic"

#@task
#def run_managed_task(lute_task_id: str) -> str:
#    print(f"Running: {lute_task_id}")
#    return "SUCCESS"

def create_workflow(
    wf_dict: Dict[str, Any],
    wait_for = [],
    all_futures = [],
) -> None:
    slurm_params: str = wf_dict.get("slurm_params", "")
    future = run_managed_task.submit(wf_dict["task_name"], conf, slurm_params, wait_for=wait_for)
    all_futures.append(future)
    if wf_dict["next"] == []:
        return
    else:
        child_tasks = []
        for task in wf_dict["next"]:
            create_workflow(task, [future], all_futures)
    return

@flow(name=flow_name, task_runner=ThreadPoolTaskRunner(max_workers=8), log_prints=True)
def dynamic_flow(flow_conf: FlowConf) -> None:

    wf_dict: Dict[str, str] = json.loads(flow_conf.get("workflow"))

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
        "lute_params": {"a":"1"},
        "slurm_params": ["--nodes=1"],
        "workflow": json.dumps(wf_dict),
    }
    dynamic_flow.from_source(
        source="https://github.com/gadorlhiac/prefect-test.git",
        entrypoint="dynamic.py:dynamic_flow",
    ).deploy(
        name="test-deployment2",
        parameters={"flow_conf":conf},
        work_pool_name="psdm-prefect-workers",
    )

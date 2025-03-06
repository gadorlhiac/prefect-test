import json
import os
import sys
import yaml
from typing import Dict, List, Literal, Optional, Union
from typing_extensions import TypedDict

import requests
from requests.auth import HTTPBasicAuth

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

class FlowRequestDict(TypedDict):
    parameters: Dict[Literal["flow_conf"], FlowConf]

if __name__ == "__main__":
    with open("test_dag.yaml", "r") as f:
        wf_dict = yaml.load(stream=f, Loader=yaml.FullLoader)
    PREFECT_API_URL: Optional[str] = os.getenv("PREFECT_API_URL")
    PREFECT_API_KEY: Optional[str] = os.getenv("PREFECT_API_AUTH_STRING")

    if PREFECT_API_URL is None or PREFECT_API_KEY is None:
        print("Setup prefect environment variables!")
        sys.exit(-1)

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

    endpoint: str = f"{PREFECT_API_URL}/csrf-token"
    auth: HTTPBasicAuth = HTTPBasicAuth(
        PREFECT_API_KEY.split(":")[0],PREFECT_API_KEY.split(":")[1]
    )
    resp: requests.models.Response = requests.get(
        endpoint, auth=auth, params={"client":PREFECT_API_KEY.split(":")[0]}
    )

    token: str = resp.json()["token"]
    client: str = resp.json()["client"]

    data: FlowRequestDict = {"parameters": {"flow_conf": conf}}
    headers: Dict[str, str] = {"Prefect-Csrf-Token": token,"Prefect-Csrf-Client": client}

    endpoint = f"{PREFECT_API_URL}/deployments/4ed13443-a443-483e-a702-78e50803bb2d/create_flow_run"
    resp = requests.post(endpoint, headers=headers, json=data, auth=auth)
    print(resp.json())

#!/sdf/group/lcls/ds/ana/sw/conda1/inst/envs/ana-4.0.62-py3/bin/python

"""Script submitted by Automated Run Processor (ARP) to trigger a Prefect flow.

This script is submitted by the ARP to the batch nodes. It triggers Prefect to
begin running the tasks of the specified deployment of a flow.
"""

__author__ = "Gabriel Dorlhiac"

import argparse
import datetime
import getpass
import json
import logging
import os
import sys
import uuid
import yaml
from typing import Any, cast, Dict, List, Optional, Tuple

import requests
from requests.auth import HTTPBasicAuth
#from requests.exceptions import HTTPError

from flow_dataclasses import FlowConf, FlowRequestDict, LuteParams

# Requests, urllib have lots of debug statements. Only set level for this logger
logger: logging.Logger = logging.getLogger("Launch_Prefect")
handler: logging.Handler = logging.StreamHandler()
formatter: logging.Formatter = logging.Formatter(logging.BASIC_FORMAT)
handler.setFormatter(formatter)
logger.addHandler(handler)

if __debug__:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


def _retrieve_creds_and_url(instance: str = "experimental") -> Tuple[str, str, str]:
    path: str = "/sdf/group/lcls/ds/tools/lute/prefect_{instance}.txt"
    if instance == "experimental":
        path = path.format(instance=instance)
    else:
        raise ValueError('`instance` must be "experimental"')
    with open(path, "r") as f:
        user_pw: str = f.readline().strip()
        url: str = f.readline().strip()
    user: str
    pw: str
    user, pw = user_pw.split(":")
    return user, pw, url

def _request_arp_token(exp: str, lifetime: int = 300) -> str:
    """Request an ARP token via Kerberos endpoint.

    A token is required for job submission.

    Args:
        exp (str): The experiment to request the token for. All tokens are
            scoped to a single experiment.

        lifetime (int): The lifetime, in minutes, of the token. After the token
            expires, it can no longer be used for job submission. The maximum
            time you can request is 480 minutes (i.e. 8 hours). NOTE: since this
            token is used for the entirety of a workflow, it must have a lifetime
            equal or longer than the duration of the workflow's execution time.
    """
    from kerberos import GSSError  # type: ignore
    from krtc import KerberosTicket  # type: ignore

    try:
        krbheaders: Dict[str, str] = KerberosTicket(
            "HTTP@pswww.slac.stanford.edu"
        ).getAuthHeaders()
    except GSSError:
        logger.info(
            "Cannot proceed without credentials. Try running `kinit` from the command-line."
        )
        raise
    base_url: str = "https://pswww.slac.stanford.edu/ws-kerb/lgbk/lgbk"
    token_endpoint: str = (
        f"{base_url}/{exp}/ws/generate_arp_token?token_lifetime={lifetime}"
    )
    resp: requests.models.Response = requests.get(token_endpoint, headers=krbheaders)
    resp.raise_for_status()
    token: str = resp.json()["value"]
    formatted_token: str = f"Bearer {token}"
    return formatted_token


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="trigger_prefect_lute_flow",
        description="Trigger Prefect to begin executing a LUTE flow.",
        epilog="Refer to https://github.com/slac-lcls/lute for more information.",
    )
    parser.add_argument("-c", "--config", type=str, help="Path to config YAML file.")
    parser.add_argument("-d", "--debug", help="Run in debug mode.", action="store_true")
    parser.add_argument(
        "-W",
        "--workflow_defn",
        type=str,
        help="Path to a YAML file with workflow.",
        default="",
    )
    # Optional arguments for when running from command-line
    parser.add_argument(
        "-e",
        "--experiment",
        type=str,
        help="Provide an experiment if not running with ARP.",
        required=False,
    )
    parser.add_argument(
        "-r",
        "--run",
        type=str,
        help="Provide a run number if not running with ARP.",
        required=False,
    )

    args: argparse.Namespace
    extra_args: List[str]  # Should contain all SLURM arguments!
    args, extra_args = parser.parse_known_args()

    # Check if was submitted from ARP - look for token
    use_kerberos: bool = (
        True  # Always copy kerberos ticket so non-active experiments can work.
    )
    cache_file: Optional[str] = os.getenv("KRB5CCNAME")
    if (
        os.getenv("Authorization") is None
        or os.getenv("EXPERIMENT") is None
        or os.getenv("RUN_NUM") is None
    ):
        if cache_file is None:
            logger.info("No Kerberos cache. Try running `kinit` and resubmitting.")
            sys.exit(-1)

        if args.experiment is None or args.run is None:
            logger.info(
                (
                    "You must provide a `-e ${EXPERIMENT}` and `-r ${RUN_NUM}` "
                    "if not running with the ARP!\n"
                    "If you submitted this from the eLog and are seeing this error "
                    "please contact the maintainers."
                )
            )
            sys.exit(-1)
        os.environ["EXPERIMENT"] = args.experiment
        os.environ["RUN_NUM"] = args.run

        os.environ["Authorization"] = _request_arp_token(args.experiment)
        os.environ["ARP_JOB_ID"] = str(uuid.uuid4())


    user: str
    pw: str
    PREFECT_API_URL: str
    user, pw, PREFECT_API_URL = _retrieve_creds_and_url()
    auth: HTTPBasicAuth = HTTPBasicAuth(user, pw)

    flow_name: str = "lute_dynamic"
    deployment_name: str = "test-deployment2"

    csrf_endpoint: str = f"{PREFECT_API_URL}/csrf-token"
    name_endpoint: str = f"{PREFECT_API_URL}/deployments/name/{flow_name}/{deployment_name}"

    if not os.path.exists(args.workflow_defn):
        logger.error("Workflow definition path does not exist! Exiting!")
        sys.exit(-1)

    wf_defn: Dict[str, Any]
    with open(args.workflow_defn, "r") as f:
        wf_defn = yaml.load(stream=f, Loader=yaml.FullLoader)


    # Experiment, run #, and ARP env variables come from ARP submission only
    # We override above or exit if we cannot, so we cast here
    assert isinstance(os.getenv("EXPERIMENT"), str)
    assert isinstance(os.getenv("RUN_NUM"), str)
    assert isinstance(os.getenv("ARP_JOB_ID"), str)
    assert isinstance(os.getenv("Authorization"), str)

    params: LuteParams = {
        "config_file": args.config,
        "debug": args.debug
    }

    conf: FlowConf = {
        "experiment": cast(str, os.getenv("EXPERIMENT")),
        "run_id": f"{cast(str, os.getenv('RUN_NUM'))}_{datetime.datetime.utcnow().isoformat()}",
        "JID_UPDATE_COUNTERS": os.getenv("JID_UPDATE_COUNTERS"),
        "ARP_ROOT_JOB_ID": cast(str, os.getenv("ARP_JOB_ID")),
        "ARP_LOCATION": os.getenv("ARP_LOCATION", "S3DF"),
        "Authorization": cast(str, os.getenv("Authorization")),
        "user": getpass.getuser(),
        "lute_location": os.path.abspath(f"{os.path.dirname(__file__)}/.."),
        "kerb_file": cache_file,
        "lute_params": params,
        "slurm_params": extra_args,
        "workflow": wf_defn
    }

    # Get CSRF
    ###############################################
    resp: requests.models.Response = requests.get(
        csrf_endpoint, auth=auth, params={"client":user}
    )

    token: str = resp.json()["token"]
    client: str = resp.json()["client"]


    # Get ID from name
    ##############################################
    resp = requests.get(name_endpoint, auth=auth)

    deployment_id: str = resp.json()["id"]

    # Launch flow_run
    ##############################################
    launch_endpoint = f"{PREFECT_API_URL}/deployments/{deployment_id}/create_flow_run"

    data: FlowRequestDict = {"parameters": {"flow_conf": conf}}
    headers: Dict[str, str] = {"Prefect-Csrf-Token": token,"Prefect-Csrf-Client": client}

    resp = requests.post(launch_endpoint, headers=headers, json=data, auth=auth)
    print(resp.json())

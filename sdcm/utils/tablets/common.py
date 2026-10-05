import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

from sdcm import wait
from sdcm.rest.remote_curl_client import RemoteCurlClient
from sdcm.rest.task_manager_client import TaskManagerClient
from sdcm.sct_events import Severity
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.adaptive_timeouts import adaptive_timeout, Operations
from sdcm.utils.features import is_tablets_feature_enabled

LOGGER = logging.getLogger(__name__)

AUTO_REPAIR_PARAM = "auto_repair_enabled_default"
# With auto-repair kept enabled, new sessions can start while draining, so allow well beyond
# the ~17-minute single-session durations observed in SCT-905
AUTO_REPAIR_DRAIN_TIMEOUT = 2 * 60 * 60


@dataclass
class TabletsConfiguration:
    enabled: Optional[bool] = None
    initial: Optional[int] = None

    def __str__(self):
        items = []
        for k, v in self.__dict__.items():
            if v is not None:
                value = str(v).lower() if isinstance(v, bool) else v
                items.append(f"'{k}': {value}")
        return "{" + ", ".join(items) + "}"


def wait_tablets_balanced(node, timeout: int = 3600):
    """
    Wait for tablets to be balanced, no more pending splits/merges and no ongoing tablets topology operations using REST API.
    A single request is enough as it is submitted as a global topology request and completes only after fresh tablet load stats produce an empty balance plan and tablets are idle."""
    if not is_tablets_feature_enabled(node):
        LOGGER.info("Tablets are disabled, skipping wait for balance")
        return
    client = RemoteCurlClient(host="127.0.0.1:10000", endpoint="", node=node)
    LOGGER.info("Waiting for tablets balancing (no pending splits/merges, no ongoing topology operations)")
    try:
        with adaptive_timeout(Operations.TABLET_MIGRATION, node, timeout=timeout) as adaptive_timeout_value:
            client.run_remoter_curl(
                method="POST", path="storage_service/quiesce_topology", params={}, timeout=adaptive_timeout_value
            )
        LOGGER.info("Tablets are balanced")
    except Exception as exc:  # noqa: BLE001
        InfoEvent(
            f"Failed to wait for tablets to be balanced. Exception: {exc.__repr__()}",
            severity=Severity.ERROR,
        ).publish()


def wait_no_active_repair_tasks(nodes: list, timeout: int = AUTO_REPAIR_DRAIN_TIMEOUT, step: int = 30) -> bool:
    """Wait until no node reports active repair-module tasks. Returns False on timeout instead of raising."""

    def no_active_repair_tasks():
        for node in nodes:
            try:
                if active_tasks := TaskManagerClient(node).get_active_repair_tasks():
                    LOGGER.debug("Node %s still runs %d repair tasks", node.name, len(active_tasks))
                    return False
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Could not list repair tasks on node %s, skipping it: %s", node.name, exc)
        return True

    return bool(
        wait.wait_for(
            func=no_active_repair_tasks,
            timeout=timeout,
            step=step,
            throw_exc=False,
            text="Waiting for in-flight repair tasks to finish",
        )
    )


@contextmanager
def temporarily_disable_auto_repair(db_cluster, drain_timeout: int = AUTO_REPAIR_DRAIN_TIMEOUT):
    """Debug variant (SCT-905 experiment): auto-repair is left enabled; only wait for in-flight
    repair tasks to finish before yielding, then run the user-driven repair alongside it.
    """
    LOGGER.info("Waiting for active repair tasks before repair (auto-repair stays enabled)")
    if not wait_no_active_repair_tasks(db_cluster.data_nodes, timeout=drain_timeout):
        LOGGER.warning("In-flight repair tasks did not finish within %s seconds, proceeding anyway", drain_timeout)
    yield

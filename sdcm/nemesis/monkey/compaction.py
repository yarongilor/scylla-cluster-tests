"""
Module containing all compaction nemesis classes.

Includes a plain major compaction, the start/stop disruptions that interrupt a
running major, scrub, cleanup or validation compaction, and the scrub of all test
keyspaces that skips corrupted partitions.

Module-level helper functions are shared across the start/stop monkey classes
defined here.
"""

import logging
from functools import partial

from sdcm.cluster import HOUR_IN_SEC
from sdcm.exceptions import UnsupportedNemesis, WatcherCallableException
from sdcm.nemesis import NemesisBaseClass, target_data_nodes
from sdcm.nemesis.utils.common import major_compaction
from sdcm.sct_events.group_common_events import ignore_scrub_invalid_errors
from sdcm.utils.adaptive_timeouts import Operations, adaptive_timeout
from sdcm.utils.common import clear_snapshots
from sdcm.utils.compaction_ops import CompactionOps, StartStopCompactionArgs
from sdcm.utils.context_managers import DbNodeLogger
from sdcm.utils.parallel_object import ParallelObject, ParallelObjectResult

LOGGER = logging.getLogger(__name__)


def handle_start_stop_compaction_results(
    trigger_and_watcher_futures: dict[str, ParallelObjectResult], allow_trigger_exceptions: bool = True
):
    """
    Handle the results from running in parallel a compaction
    trigger and watcher / stop-compaction functions.
    """
    if trigger_and_watcher_futures["trigger"].exc:
        LOGGER.warning("Trigger function failed with:\n%s", trigger_and_watcher_futures["trigger"].exc)

    if trigger_and_watcher_futures["watcher"].exc:
        raise WatcherCallableException from trigger_and_watcher_futures["watcher"].exc
    return trigger_and_watcher_futures


def get_random_non_system_ks_cf(runner, filter_empty_tables: bool = True) -> tuple[str, str] | None:
    """
    Get a random (ks, cf) from the target node.
    The get_non_system_ks_cf_list returns a list of strings
    in the format 'keyspace_name.cf_name' so we need to
    call split() on the output to get keyspace_name and
    cf_name separately.
    """
    if ks_cf_list := runner.cluster.get_non_system_ks_cf_list(
        db_node=runner.target_node, filter_empty_tables=filter_empty_tables
    ):
        return runner.random.choice(ks_cf_list).split(".")
    else:
        return None, None


def prepare_start_stop_compaction(runner) -> StartStopCompactionArgs:
    """Pick a random non-system table of the target node and build the start/stop compaction arguments."""
    ks, cf = get_random_non_system_ks_cf(runner)

    if not ks or not cf:
        raise UnsupportedNemesis("No non-system keyspaces to run the nemesis with. Skipping.")

    return StartStopCompactionArgs(
        keyspace=ks,
        columnfamily=cf,
        timeout=360,
        target_node=runner.target_node,
        compaction_ops=CompactionOps(cluster=runner.cluster, node=runner.target_node),
    )


class MajorCompactionMonkey(NemesisBaseClass):
    """Run nodetool compact on the target node."""

    disruptive = False
    kubernetes = True
    xcloud = True
    limited = True

    def disrupt(self):
        """Run major compaction."""
        major_compaction(self.runner)


@target_data_nodes
class CorruptThenScrubMonkey(NemesisBaseClass):
    """Run nodetool scrub --skip-corrupted on every test keyspace of the target node and clear the snapshots."""

    disruptive = False
    supports_high_disk_utilization = False  # Failed for: https://github.com/scylladb/scylladb/issues/22088

    def disrupt(self):
        """
        Try to rebuild the sstables of all test keyspaces by scrub, the corrupted partitions
        will be skipped.
        """
        self.runner.log.debug(
            "Rebuild sstables by scrub with `--skip-corrupted`, corrupted partitions will be skipped."
        )
        test_keyspaces = self.runner.cluster.get_test_keyspaces()
        with (
            ignore_scrub_invalid_errors(),
            adaptive_timeout(Operations.SCRUB, self.runner.target_node, timeout=HOUR_IN_SEC * 48),
        ):
            for ks in test_keyspaces:
                with self.runner.action_log_scope(f"Scrub {ks} keyspace on {self.runner.target_node.name} node"):
                    self.runner.target_node.run_nodetool("scrub", args=f"--skip-corrupted {ks}")

        for ks in test_keyspaces:
            with self.runner.action_log_scope(f"Clear {ks} pre-scrub snapshots on {self.runner.target_node.name} node"):
                clear_snapshots([self.runner.target_node], "pre-scrub-", ks)


class StartStopMajorCompaction(NemesisBaseClass):
    """Start a major compaction on a non-system table and stop it with nodetool stop COMPACTION once it is logged."""

    disruptive = False

    def disrupt(self):
        """
        Start and stop a major compaction on a non-system columnfamily.

        1. Query the target node for a non-system cf. If no cf is
        found, skip the nemesis.

        In parallel:
        2.1 Start a major compaction using a REST API call (via a Remoter
        curl call on the target node).
        2.2. Watch for the major compaction to show up in logs.
        3. Stop the running major compaction by issuing a <nodetool stop
        COMPACTION> command after the watcher picks up the compaction
        started.

        The REST API call starting the compaction may not return, since
        it returns on completing the compaction task (which we attempt
        to interrupt). We ignore such timeouts and validate only the
        success of the command in (4).
        """
        compaction_args = prepare_start_stop_compaction(self.runner)
        trigger_func = partial(
            compaction_args.compaction_ops.trigger_major_compaction,
            keyspace=compaction_args.keyspace,
            cf=compaction_args.columnfamily,
        )
        watch_func = partial(
            compaction_args.compaction_ops.stop_on_user_compaction_logged,
            node=compaction_args.target_node,
            mark=compaction_args.target_node.mark_log(),
            watch_for="User initiated compaction started on behalf of",
            timeout=compaction_args.timeout,
            stop_func=compaction_args.compaction_ops.stop_major_compaction,
        )
        ks_cf = f"{compaction_args.keyspace}.{compaction_args.columnfamily}"
        with (
            DbNodeLogger(
                self.runner.cluster.nodes,
                "start and stop major compaction",
                target_node=self.runner.target_node,
                additional_info=f"on {ks_cf}",
            ),
            self.runner.action_log_scope(
                f"start and stop major compaction on {ks_cf} table on {self.runner.target_node.name} node"
            ),
        ):
            results = ParallelObject.run_named_tasks_in_parallel(
                tasks={"trigger": trigger_func, "watcher": watch_func},
                timeout=compaction_args.timeout + 5,
                ignore_exceptions=True,
            )

        handle_start_stop_compaction_results(trigger_and_watcher_futures=results, allow_trigger_exceptions=True)


class StartStopScrubCompaction(NemesisBaseClass):
    """Start a scrub compaction on a non-system table and stop it with nodetool stop SCRUB once it is logged."""

    disruptive = False

    def disrupt(self):
        """
        Start and stop a scrub compaction on a non-system columnfamily.

        1. Query the target node for a non-system cf. If no cf is found,
        skip the nemesis.

        In parallel:
        2.1 Start a scrub compaction using a REST API call (via a Remoter
        curl call on the target node).
        2.2. Watch for the scrub compaction to show up in logs.
        3. Stop the running scrub compaction by issuing a <nodetool stop
        SCRUB> command after the watcher picks up the compaction started.

        The REST API call starting the compaction may not return, since it
        returns on completing the compaction task (which we attempt to
        interrupt). We ignore such timeouts and validate only the success
        of the command in (4).
        """
        compaction_args = prepare_start_stop_compaction(self.runner)
        trigger_func = partial(
            compaction_args.compaction_ops.trigger_scrub_compaction,
            keyspace=compaction_args.keyspace,
            cf=compaction_args.columnfamily,
        )
        watch_func = partial(
            compaction_args.compaction_ops.stop_on_user_compaction_logged,
            node=compaction_args.target_node,
            mark=compaction_args.target_node.mark_log(),
            watch_for="Scrubbing",
            timeout=compaction_args.timeout,
            stop_func=compaction_args.compaction_ops.stop_scrub_compaction,
        )
        ks_cf = f"{compaction_args.keyspace}.{compaction_args.columnfamily}"
        with (
            DbNodeLogger(
                self.runner.cluster.nodes, "start and stop scrub compaction", target_node=self.runner.target_node
            ),
            self.runner.action_log_scope(
                f"start and stop scrub compaction on {ks_cf} table on {self.runner.target_node.name} node"
            ),
        ):
            results = ParallelObject.run_named_tasks_in_parallel(
                tasks={"trigger": trigger_func, "watcher": watch_func},
                timeout=compaction_args.timeout + 5,
                ignore_exceptions=True,
            )

        handle_start_stop_compaction_results(trigger_and_watcher_futures=results, allow_trigger_exceptions=True)

        with self.runner.action_log_scope(f"Clear {ks_cf} pre-scrub snapshots on {self.runner.target_node.name} node"):
            clear_snapshots(
                [self.runner.target_node], "pre-scrub-", compaction_args.keyspace, compaction_args.columnfamily
            )


class StartStopCleanupCompaction(NemesisBaseClass):
    """Start a cleanup compaction on a non-system table and stop it with nodetool stop CLEANUP once it is logged."""

    disruptive = False

    def disrupt(self):
        """
        Start and stop a cleanup compaction on a non-system columnfamily.

        1. Query the target node for a non-system cf. If no cf is found,
        skip the nemesis.

        In parallel:
        2.1 Start a cleanup compaction using a REST API call (via a Remoter
        curl call on the target node).
        2.2. Watch for the cleanup compaction to show up in logs.
        3. Stop the running cleanup compaction by issuing a <nodetool stop
        CLEANUP> command after the watcher picks up the compaction started.

        The REST API call starting the compaction may not return, since it
        returns on  completing the compaction task (which we attempt to
        interrupt). We ignore such timeouts and validate only the success
        of the command in (4).
        """
        compaction_args = prepare_start_stop_compaction(self.runner)
        trigger_func = partial(compaction_args.compaction_ops.trigger_cleanup_compaction, timeout=600)
        watch_func = partial(
            compaction_args.compaction_ops.stop_on_user_compaction_logged,
            node=compaction_args.target_node,
            mark=compaction_args.target_node.mark_log(),
            timeout=compaction_args.timeout,
            watch_for="Cleaning",
            stop_func=compaction_args.compaction_ops.stop_cleanup_compaction,
        )

        ks_cf = f"{compaction_args.keyspace}.{compaction_args.columnfamily}"
        with (
            DbNodeLogger(
                self.runner.cluster.nodes, "start and stop cleanup compaction", target_node=self.runner.target_node
            ),
            self.runner.action_log_scope(
                f"start and stop cleanup compaction on {ks_cf} table on {self.runner.target_node.name} node"
            ),
        ):
            results = ParallelObject.run_named_tasks_in_parallel(
                tasks={"trigger": trigger_func, "watcher": watch_func},
                timeout=compaction_args.timeout + 5,
                ignore_exceptions=True,
            )

        handle_start_stop_compaction_results(trigger_and_watcher_futures=results, allow_trigger_exceptions=True)


class StartStopValidationCompaction(NemesisBaseClass):
    """Start a validation compaction on a non-system table and stop it with nodetool stop SCRUB once it is logged."""

    disruptive = False
    supports_high_disk_utilization = False  # Failed for: https://github.com/scylladb/scylladb/issues/22088

    def disrupt(self):
        """
        Start and stop a validation compaction on a non-system columnfamily.

        1. Query the target node for a non-system cf. If no cf is found,
        skip the nemesis.

        In parallel:
        2.1 Start a validation compaction using a REST API call (via a
        Remoter curl call on the target node).
        2.2. Watch for the validation compaction to show up in logs.
        3. Stop the running validation compaction by issuing a <nodetool
        stop SCRUB> command after the watcher picks up the compaction
        started.

        The REST API call starting the compaction may not return, since
        it returns on completing the compaction task (which we attempt
        to interrupt). We ignore such timeouts and validate only the
        success of the command in (4).
        """
        compaction_args = prepare_start_stop_compaction(self.runner)
        trigger_func = partial(
            compaction_args.compaction_ops.trigger_validation_compaction,
            keyspace=compaction_args.keyspace,
            cf=compaction_args.columnfamily,
        )
        watch_func = partial(
            compaction_args.compaction_ops.stop_on_user_compaction_logged,
            node=compaction_args.target_node,
            mark=compaction_args.target_node.mark_log(),
            watch_for="Scrubbing ",
            timeout=compaction_args.timeout,
            stop_func=compaction_args.compaction_ops.stop_validation_compaction,
        )

        ks_cf = f"{compaction_args.keyspace}.{compaction_args.columnfamily}"
        with (
            DbNodeLogger(
                self.runner.cluster.nodes,
                "start and stop validation compaction",
                target_node=self.runner.target_node,
            ),
            self.runner.action_log_scope(
                f"start and stop validation compaction on {ks_cf} on {self.runner.target_node.name} node"
            ),
        ):
            results = ParallelObject.run_named_tasks_in_parallel(
                tasks={"trigger": trigger_func, "watcher": watch_func},
                timeout=compaction_args.timeout + 5,
                ignore_exceptions=True,
            )

        handle_start_stop_compaction_results(trigger_and_watcher_futures=results, allow_trigger_exceptions=True)

        with self.runner.action_log_scope(f"Clear {ks_cf} pre-scrub snapshots on {self.runner.target_node.name} node"):
            clear_snapshots(
                [self.runner.target_node], "pre-scrub-", compaction_args.keyspace, compaction_args.columnfamily
            )

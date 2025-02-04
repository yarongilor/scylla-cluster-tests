from longevity_twcs_test import TWCSLongevityTest
from sdcm.sct_events.system import InfoEvent


class CqlConnectionTest(TWCSLongevityTest):
    keyspace = 'scylla_bench'
    table = 'test'
    ks_cf = f'{keyspace}.{table}'

    def test_cql_connection(self):
        """
        Test cql connection to cluster nodes.
        Verify both works as expected.
        check if the "node" parameter is required or obsolete.
        check what happens if the cql connection is given a node that is either down or decommissioned.
        """

        self.create_tables_for_scylla_bench()
        node1, node2, node3 = original_nodes = self.db_cluster.nodes
        stress_queue = []

        stress_cmd = self.params.get('stress_cmd')
        params = {'stress_cmd': stress_cmd, 'round_robin': self.params.get('round_robin')}
        self._run_all_stress_cmds(stress_queue, params)

        def cql_connection_patient_query(node):
            with self.db_cluster.cql_connection_patient(node) as session:
                res = session.execute(f"SELECT * from {self.ks_cf} limit 1")
                self.log.debug("cql_connection_patient result: %s", list(res))

        def patient_connect_to_a_stopped_node():
            InfoEvent('check cql_connection_patient when a node is down').publish()
            for node in self.db_cluster.data_nodes:
                InfoEvent(f'Connecting node {node.name}').publish()
                self.log.info(f'stopping node: {node.name}')
                node.stop_scylla_server(verify_up=True, verify_down=True, ignore_status=False)
                self.log.info("check cql_connection_patient when node %s is down", node.name)
                cql_connection_patient_query(node)
                self.log.info(f'starting node: {node.name}')
                node.start_scylla_server(verify_up=True)

        def patient_connect_to_nodes(nodes):
            InfoEvent('check cql_connection_patient to nodes..').publish()
            for node in nodes:
                InfoEvent(f'Connecting node {node.name}').publish()
                cql_connection_patient_query(node)

        # Test scenario start
        InfoEvent('check cql_connection_patient when all nodes are up normal').publish()
        cql_connection_patient_query(node1)
        # check cql_connection_patient when a node is down
        patient_connect_to_a_stopped_node()
        # Replace all cluster nodes
        for node in original_nodes:
            InfoEvent(f'Start decommissioning node {node.name}').publish()
            self.db_cluster.decommission(node)
            InfoEvent('Adding a new node..').publish()
            self.log.info('Bootstrapping a new node...')
            new_node = self.db_cluster.add_nodes(count=1, enable_auto_bootstrap=True)[0]
            self.log.info('Waiting for new node to finish initializing...')
            self.db_cluster.wait_for_init(node_list=[new_node])
            self.monitors.reconfigure_scylla_monitoring()

        self.log.info('Check CQL connection to cluster, using all original decommissioned nodes')
        patient_connect_to_nodes(original_nodes)

        self.log.info('Check cql_connection_patient when a node is down again, a second time')
        patient_connect_to_a_stopped_node()

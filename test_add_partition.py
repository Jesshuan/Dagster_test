from dagster import DagsterInstance

instance = DagsterInstance.get()
instance.add_dynamic_partitions("dynamic_parts_test", ["new_partition_key_4"])
from dagster import (
    AssetsDefinition,
    AssetSelection,
    AssetKey,
    AssetOut,
    AssetExecutionContext,
    OpExecutionContext,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    Definitions,
    JobDefinition,
    ScheduleDefinition,
    DefaultScheduleStatus,
    define_asset_job,
    validate_run_config,
    load_assets_from_modules,
    load_assets_from_package_name,
    InMemoryIOManager,
    IOManager,
    SourceAsset,
    InputContext,
    io_manager,
    asset,
    In,
    op,
    graph_asset,
    multi_asset,
    Config,
    RunConfig,
    AssetOut,
    AssetIn,
    Out,
    Output,
    In,
    multi_asset_sensor,
    SensorDefinition,
    MultiAssetSensorEvaluationContext,
    DefaultSensorStatus,
    RunRequest
)

"""

class RedisIOManager(IOManager):
    def __init__(self, redis_host: str, redis_port: int):
        self.redis_client = redis.Redis(host=redis_host, port=redis_port)

    def handle_output(self, context: OutputContext, obj: object) -> None:
        key = self._get_redis_key(context)
        pickled_obj = pickle.dumps(obj)
        self.redis_client.set(key, pickled_obj)

    def load_input(self, context: InputContext) -> object:
        key = self._get_redis_key(context)
        pickled_obj = self.redis_client.get(key)
        if pickled_obj is None:
            raise ValueError(f"Object not found for key {key} in Redis.")
        if isinstance(pickled_obj, bytes):
            return pickle.loads(pickled_obj)
        else:
            raise ValueError(f"Unexpected data type for key {key}: {type(pickled_obj)}")

    def _get_redis_key(self, context: Union[InputContext, OutputContext]) -> str:
        print(f"context.asset_key.path: {context.asset_key.path}")
        return "|".join(context.asset_key.path)

"""
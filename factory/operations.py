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

from factory.dagster_config import AssetConfig, OpMainProcessConfig, OpPreprocInterpConfig, OpPreprocTimeConfig

from factory.process import asset_process

@op(name="interpolation_inputer") #required_resource_keys=["in_memory_io_manager"]
def op_interp_imputer(context: OpExecutionContext, data, config: OpPreprocInterpConfig):

                context.log.info(f"Received : {data}")

                context.log.info(f"And preprocessing interpolation type : {config.interpolation_type}")

                return data
        
@op(name="time_preprocessing") #required_resource_keys=["in_memory_io_manager"]
def op_time_prepocess(context: OpExecutionContext, data, config: OpPreprocTimeConfig):

                context.log.info(f"Received : {data}")

                context.log.info(f"And preprocessing time type : {config.time_preprocessing_type}")

                return data
        

@op(name="main_process") #required_resource_keys=["in_memory_io_manager"]
def op_main_process(context: OpExecutionContext, data, config: OpMainProcessConfig):

                context.log.info(f"Received : {data}")

                data = asset_process(context,
                                        model_type=config.model_type,
                                        params=config.params,
                                        upstream_data=data
                                        )

                return data
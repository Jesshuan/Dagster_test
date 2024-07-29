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

#import redis

#import pickle

from factory.specs import specs

from factory.factory import Pipeline_Factory



# -- DEFINITION -- 


factory = Pipeline_Factory(specs = specs)

factory.build_preprocess()

resources = {
    "in_memory_io_manager": InMemoryIOManager(),
    #Ajouter ici d'autres resources si nécessaire
}

print("--- ASSETS BUILT ---")
print(factory.assets_to_build)
print("--- GRAPH ASSETS BUILT ---")
print(factory.graph_assets_to_build)
print("--- JOBS BUILT ---")
print(factory.jobs_to_build)
print("--- SCHEDULES BUILT ---")
print(factory.schedule_to_build)
print("--- SENSORS BUILT ---")
print(factory.sensors_to_build)

assets_list = factory.build_assets()
graph_assets_list = factory.build_graph_assets()
jobs_list = factory.build_jobs()
schedules_list = factory.build_schedules()
sensors_list = factory.build_sensors()

print ("SENSOR OBJECTS LIST ")

print(dir(jobs_list[0]))

defs = Definitions(assets=assets_list + graph_assets_list, 
                   jobs=jobs_list,
                   schedules=schedules_list ,
                   sensors=sensors_list
                   )
                   # resources=resources, sensors=[my_custom_auto_materialize_sensor],
"""


# FOR ASSETS PLAYGROUND TESTING

from factory.assets_playground import * 

defs = Definitions(assets=[asset_test_1, asset_test_2, asset_test_3, graph_asset_test_4, graph_asset_test_5, graph_asset_test_4_bis, graph_asset_test_6],
                   jobs=[job_1, job_4, job_5],
                   schedules=[schedule_1, schedule_4, schedule_5])


"""
auto_materialize_policy=AutoMaterializePolicy.eager()\
            .with_rules(AutoMaterializeRule.materialize_on_cron("*/3 * * * *"))\
            .without_rules(AutoMaterializeRule.materialize_on_parent_updated())"""
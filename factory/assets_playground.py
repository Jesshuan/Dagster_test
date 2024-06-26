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

from dagster_config import AssetConfig, OpMainProcessConfig, OpPreprocInterpConfig, OpPreprocTimeConfig

import numpy as np


@asset(name="test_1",
        group_name="group",#io_manager_key='in_memory_io_manager',
        auto_materialize_policy=None
                )
def asset_test_1(context:AssetExecutionContext, config: AssetConfig) : #**kwargs)

    data = 1

    context.log.info(f"This is my param : {config.params}")
                    
    return Output(data,metadata={"Non-empty items": "test_meta"})  

@op(name="interpolation_inputer", #required_resource_keys=["in_memory_io_manager"]
    )
def op_interp_imputer(context: OpExecutionContext, data, config: OpPreprocInterpConfig):

    context.log.info(f"Received : {data}")

    context.log.info(f"And preprocessing interpolation type : {config.interpolation_type}")

    data_re = data + 4

    return data_re

@op(name="time_preprocessing", #required_resource_keys=["in_memory_io_manager"]
    )
def op_time_prepocess(context: OpExecutionContext, data, config: OpPreprocTimeConfig):

    context.log.info(f"Received : {data}")

    context.log.info(f"And preprocessing time type : {config.time_preprocessing_type}")

    data_re = np.sum(data)

    return data_re

@op(name="main_process", #required_resource_keys=["in_memory_io_manager"]
    )
def op_main_process(context: OpExecutionContext, data, config: OpMainProcessConfig):

    context.log.info(f"Received : {data}")

    data_re = data + 1000

    return data_re

@graph_asset(name="graph_asset_4",
             group_name="group",
             ins={'input_1':AssetIn("test_2"), 'input_2':AssetIn("test_3")})
def graph_asset_test_4(**kwargs):

    data_list=[]

    for i in range(2):
        data_list.append(op_interp_imputer(kwargs[f"input_{i+1}"]))
    

    #for i, key in enumerate(kwargs.keys()):
        #data_list.append(op_test_1(kwargs[f'input_{i+1}']))

    return op_main_process(op_time_prepocess(data_list))


@asset(name="test_2",
        group_name="group", #io_manager_key='in_memory_io_manager',
        deps=["test_1"]) #auto_materialize_policy=AutoMaterializePolicy.eager()) #.with_rules(AutoMaterializeRule.materialize_on_cron("*/7 * * * *")
def asset_test_2(context:AssetExecutionContext, config: AssetConfig) : #**kwargs)

    data = 2

    context.log.info(f"This is my param : {config.params}")
                    
    return Output(data,metadata={"Non-empty items": "test_meta"})

@asset(name="test_3",
        group_name="group", #io_manager_key='in_memory_io_manager',
        deps=["test_1"])#.with_rules(AutoMaterializeRule.materialize_on_cron("*/7 * * * *")
def asset_test_3(context:AssetExecutionContext, config: AssetConfig) : #**kwargs

    data = 3

    context.log.info(f"This is my param : {config.params}")
                    
    return Output(data,metadata={"Non-empty items": "test_meta"})


job_1 = define_asset_job("job_asset_1", selection="test_1", config={'ops':
                                          {"test_1":
                                            {'config':
                                                {'model_type': "default",
                                                    'params': {"param_1":1},
                                                    'sources': []
                                                }
                                            }
                                          }
                                  })

job_2 = define_asset_job("job_asset_2", selection="test_2", config={'ops':
                                          {"test_2":
                                            {'config':
                                                {'model_type': "default",
                                                    'params': {"param_2":2},
                                                    'sources': []
                                                }
                                            }
                                          }
                                  })

job_3 = define_asset_job("job_asset_3", selection="test_3", config={'ops':
                                          {"test_3":
                                            {'config':
                                                {'model_type': "default",
                                                    'params': {"param_3":3},
                                                    'sources': []
                                                }
                                            }
                                          }
                                  })


job_4 = define_asset_job("job_asset_4", selection="graph_asset_4",
                                            config=
                                            {'ops':
                                                {'graph_asset_4': 
                                                    {'ops':
                                                    {'interpolation_inputer': 
                                                        {'config':
                                                        {'interpolation_type': 'linear_interp'}
                                                        },
                                                    'interpolation_inputer_2':
                                                        {'config':
                                                        {'interpolation_type': 'polynomial_interp'}
                                                        },
                                                    'main_process':
                                                        {'config':
                                                        {'model_type': 'time_series', 
                                                         'params': {'day':1, 'week':1}}
                                                        },
                                                    'time_preprocessing':
                                                        {'config':
                                                        {'time_preprocessing_type': 'shifter_past_01'}
                                                        }
                                                    }
                                                }
                                            }
                                        })

schedule_1 = ScheduleDefinition(job_name="job_asset_1", \
                                   cron_schedule="*/2 * * * *", \
                                    default_status=DefaultScheduleStatus.RUNNING)

schedule_2 = ScheduleDefinition(job_name="job_asset_2", \
                                   cron_schedule="*/3 * * * *", \
                                    default_status=DefaultScheduleStatus.RUNNING)

schedule_3 = ScheduleDefinition(job_name="job_asset_3", \
                                   cron_schedule="*/3 * * * *", \
                                    default_status=DefaultScheduleStatus.RUNNING)


schedule_4 = ScheduleDefinition(job_name="job_asset_4", \
                                   cron_schedule="*/5 * * * *", \
                                    default_status=DefaultScheduleStatus.RUNNING)

@multi_asset_sensor(monitored_assets=AssetSelection.assets("test_1"),
                    job=job_2, minimum_interval_seconds=10,
                    default_status=DefaultSensorStatus.RUNNING)
def sensor_2(context: MultiAssetSensorEvaluationContext):
    context.log.info(context.latest_materialization_records_by_key())
    for asset_key, materialization in context.latest_materialization_records_by_key().items():
        if materialization:
            context.advance_cursor({asset_key: materialization})
            return RunRequest()
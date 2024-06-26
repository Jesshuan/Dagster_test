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

from typing import List, Union

from factory.process import asset_process

from factory.operations import op_interp_imputer, op_main_process, op_time_prepocess

from factory.dagster_config import AssetConfig, OpMainProcessConfig, OpPreprocInterpConfig, OpPreprocTimeConfig

import numpy as np


MINIMAL_INTERVAL_SENSOR = 10 #seconds



class Pipeline_Factory():
    def __init__(self, specs):
        self.specs = specs
        self.job_to_generate = []

        known_eq_id_list = []
        upstreams_list = []

        self.eq_id_descr_index = {}

        for spec in self.specs:
            known_eq_id_list.append(spec["eq_id"])
            customer = spec["customer"]
            self.eq_id_descr_index[spec["eq_id"]] = spec["descr"]

            if "upstreams" in spec.keys():
                for upstream in spec["upstreams"]:
                    upstreams_list.append((upstream, customer))
        self.upstreams_list = list(set(upstreams_list))

        self.known_eq_id_list = list(set(known_eq_id_list))

        for up, cust in self.upstreams_list:
            if up not in self.known_eq_id_list:
                self.specs.append({"descr": "unknow_descr",
                                   "customer":cust,
                                   "eq_id":up,
                                    "model_type":"source",
                                    "upstream": [],
                                    "params":{},
                                    "computation_frequency":60,
                                    "refresh_on_upstreams_up":[]})
                self.eq_id_descr_index[up] = "unknow_descr"
                
        
        self.associated_comp_frequency = {}

        for spec in self.specs:
            self.associated_comp_frequency[spec["eq_id"]] = spec["computation_frequency"]

        self.assets_to_build = []
        self.graph_assets_to_build = []
        self.jobs_to_build = []
        self.schedule_to_build = []
        self.sensors_to_build = []

        self.assets_index = {}

    def get_block_modelization(self, model_type):

        if model_type=="influx_operation":
            return ["inference"]
        
        elif model_type=="source":
            return ["stream_consumer"]
        
        elif model_type=="time_series":
            return ["inference"]
        
        elif model_type=="time_series_training":
            return ["training"]
        
    def convert_freq_to_cron(self, freq):
        freq = str(freq // 60)
        mapping = {"1": "* * * * *",
                   "2": "*/2 * * * *",
                   "3": "*/3 * * * *",
                   "5": "*/5 * * * *",
                   "10": "*/10 * * * *"}
        return mapping[freq]

    def build_preprocess(self):

        for spec in self.specs:
            
            # Mandatory sepcifications
            eq_id=spec["eq_id"]
            model_type = spec["model_type"]
            descr = spec["descr"]
            customer = spec["customer"]
            computation_freq = spec["computation_frequency"]

            # Not mandatory

            if "params" in spec.keys():
                params = spec["params"]
            else:
                params = None

            if "interpolation_type" in spec.keys():
                interpolation_time = spec["interpolation_type"]
            else:
                interpolation_time = None

            if "time_preprocessing" in spec.keys():
                time_preprocessing = spec["time_preprocessing"]
            else:
                time_preprocessing = None

            if "upstreams" in spec.keys():
                upstreams = spec["upstreams"]
            else:
                upstreams = None

            if "refresh_on_upstreams_up" in spec.keys():
                refresh_on_upstreams_up_list = spec["refresh_on_upstreams_up"]
            else:
                refresh_on_upstreams_up_list = []

            if computation_freq:
                cron_string = self.convert_freq_to_cron(computation_freq)
            else:
                cron_string = None

            block_modelization = self.get_block_modelization(model_type)

            for block in block_modelization:

                if block == "inference" or block=="training":
                        upstream_without_cache = []
                        internal_cache = []
                        global_refresh_up_list = []
                        # Cache_trigger blocks
                        for upstream in upstreams:

                            if computation_freq:
                            
                                if self.associated_comp_frequency[upstream] < computation_freq: # If we have to cache the upstream source
                                    #We build a special cache asset wich is used as an intermediare 
                                    # This cache is running when the usptream source has running (with a dedicated sensor)
                                    name=f"{eq_id}_cache_{block}_{upstream}"
                                    self.assets_to_build.append({"name":name,
                                                        "group_name":customer, #f"{descr}_{customer}"
                                                        "upstreams":[upstream],
                                                        "key_prefix":descr}) #None
                                    self.jobs_to_build.append({"job_name":f'job_{name}',
                                                            "selection":f"{descr}/{name}",
                                                            "ops_name":f"{descr}__{name}",
                                                            "model_type":'cache',
                                                            "params":params,
                                                            "interpolation_time":None,
                                                            "time_preprocessing":None,
                                                            "sources":[upstream]})
                                    
                                    refresh_up = [self.eq_id_descr_index[upstream], upstream]
                                    refresh_up_list = [refresh_up]
                                    if upstream in refresh_on_upstreams_up_list:
                                        global_refresh_up_list.append(refresh_up)

                                    self.sensors_to_build.append({"sensor_name":f'sensor_{name}',
                                                                "refresh_up_list":refresh_up_list,
                                                                "job_name":f'job_{name}'})
                                    '''self.schedule_to_build.append({"job_name":f'job_{name}',
                                                                "cron":associated_cron})'''                              
                                    internal_cache.append(name)
                                
                                else:
                                    upstream_without_cache.append(upstream)
                                    if upstream in refresh_on_upstreams_up_list:
                                        refresh_up = [self.eq_id_descr_index[upstream], upstream]
                                        global_refresh_up_list.append(refresh_up)

                                    

                            else: # Upstream source is directly linked to the asset
                                upstream_without_cache.append(upstream)
                                if upstream in refresh_on_upstreams_up_list:
                                        refresh_up = [self.eq_id_descr_index[upstream], upstream]
                                        global_refresh_up_list.append(refresh_up)

                        # Main Process block
                        name = eq_id
                        self.graph_assets_to_build.append({"name":name,
                                                        "group_name":customer, #f"{descr}_{customer}"
                                                        "upstreams":upstream_without_cache + internal_cache,
                                                        "key_prefix":descr, #None
                                                        })
                        self.jobs_to_build.append({"job_name":f'job_{name}',
                                                           "selection":f"{descr}/{name}",
                                                           "ops_name":f"{descr}__{name}",
                                                           "model_type":model_type,
                                                           "params":params,
                                                           "interpolation_time":interpolation_time,
                                                            "time_preprocessing":time_preprocessing,
                                                           "sources":upstream_without_cache + internal_cache})
                        if cron_string:
                            self.schedule_to_build.append({"job_name":f'job_{name}',
                                                               "cron":cron_string})
                        """if refresh_on_upstreams_up_list: # If we have to refresh the asset depending on the upstreams
                            if len(refresh_on_upstreams_up_list) > 0:
                                refresh_up_list = [[self.eq_id_descr_index[upst], upst] for upst in refresh_on_upstreams_up_list]"""
                        if len(global_refresh_up_list) > 0:
                            self.sensors_to_build.append({"sensor_name":f'sensor_{name}',
                                                            "refresh_up_list":global_refresh_up_list,
                                                            "job_name":f'job_{name}'})

                        
                elif block == "stream_consumer":
                    name= eq_id
                    self.assets_to_build.append({"name":name,
                                                "group_name":customer, #f"{descr}_{customer}"
                                                "upstreams":[],
                                                "key_prefix":descr, #None
                                                })
                    self.jobs_to_build.append({"job_name":f'job_{name}',
                                                           "selection":f"{descr}/{name}",
                                                           "ops_name":f"{descr}__{name}",
                                                           "model_type":model_type,
                                                           "params":params,
                                                           "interpolation_time":None,
                                                            "time_preprocessing":None,
                                                           "sources":[]})
                    if cron_string:
                        self.schedule_to_build.append({"job_name":f'job_{name}',
                                                               "cron":cron_string})
                    
    def build_assets(self) -> Union[AssetsDefinition]:

        assets_list = []

        for asset_to_build in self.assets_to_build:

            name = asset_to_build["name"]
            group_name = asset_to_build["group_name"]
            upstreams = asset_to_build["upstreams"]
            key_prefix = asset_to_build["key_prefix"]
                
            @asset(name=name,
                    deps=upstreams, #ins={dep: AssetIn(dep) for dep in upstreams},
                    group_name=group_name, #io_manager_key='in_memory_io_manager',
                    key_prefix=key_prefix
                )
            def _asset(context:AssetExecutionContext, config: AssetConfig) : #**kwargs)

                context.log.info("HERE")
                    #for upstream in config.sources:
                        #context.log.info(context.resources.in_memory_io_manager.load_input(context))
                    # Récupérer les données des assets upstreams

                data = asset_process(context,
                                        model_type=config.model_type,
                                        params=config.params,
                                        sources=config.sources,
                                        upstream_data=None
                                        )
                    
                return Output(data,
                                metadata={"Non-empty items": "test_meta"}
                                )

            assets_list.append(_asset)
            #self.assets_index[name] = _asset

        return assets_list
    
    def build_graph_assets(self) -> Union[AssetsDefinition]:

        graph_assets_list = []

        for graph_asset_to_build in self.graph_assets_to_build:

            name = graph_asset_to_build["name"]
            group_name = graph_asset_to_build["group_name"]
            upstreams = graph_asset_to_build["upstreams"]
            key_prefix = graph_asset_to_build["key_prefix"]

            @graph_asset(name=name,
                    group_name=group_name,
                    key_prefix=key_prefix,
                    ins={upstream_str : AssetIn(upstream_str) for upstream_str in upstreams}) # ins={'input_1':AssetIn("test_2"), 'input_2':AssetIn("test_3")},
            def _graph_asset(**kwargs):

                data_list=[]

                for upstream in upstreams:
                    data_list.append(op_interp_imputer(kwargs[upstream]))

                data = op_main_process(op_time_prepocess(data_list))

                return data

            graph_assets_list.append(_graph_asset)
            #self.assets_index[name] = _asset

        return graph_assets_list

    
    def build_jobs(self) -> Union[JobDefinition]:

        jobs_list = []

        for job_to_build in self.jobs_to_build:

            job_name = job_to_build["job_name"]
            selection = job_to_build["selection"]
            ops_name = job_to_build["ops_name"]
            model_type = job_to_build["model_type"]
            params = job_to_build["params"]
            interpolation_time = job_to_build["interpolation_time"]
            time_preprocessing = job_to_build["time_preprocessing"]
            sources = job_to_build["sources"]

            if params is None:
                 params = {}

            if model_type in ["cache", "source"]:

                jobs_list.append(define_asset_job(job_name, \
                                 selection=selection, \
                                  config={'ops':
                                          {ops_name:
                                            {'config':
                                                {'model_type': model_type,
                                                    'params': params,
                                                    'sources': sources
                                                }
                                            }
                                          }
                                        }
                                  ))
                
            else:

                sub_dict = {}

                sub_dict["interpolation_inputer"] = {'config':
                                                        {'interpolation_type': interpolation_time}
                                                        }

                for i in range(len(sources) - 1):
                     sub_dict[f"interpolation_inputer_{i + 2}"] = {'config':
                                                        {'interpolation_type': interpolation_time}
                                                        }

                sub_dict["time_preprocessing"] = {'config':
                                                        {'time_preprocessing_type': time_preprocessing}
                                                        }

                sub_dict["main_process"] = {'config':
                                                        {'model_type': model_type, 
                                                         'params': params}
                                                        }
                
                jobs_list.append(define_asset_job(job_name, \
                                 selection=selection, \
                                  config={'ops':
                                                {ops_name: 
                                                    {'ops': sub_dict
                                                    }
                                                }
                                            }
                                        ))



        return jobs_list
    
    
    
    def build_schedules(self) -> Union[ScheduleDefinition]:
    
        return [ScheduleDefinition(job_name=schedule_to_build["job_name"], \
                                   cron_schedule=schedule_to_build["cron"], \
                                    default_status=DefaultScheduleStatus.RUNNING) \
                for schedule_to_build in self.schedule_to_build]
    
    def build_sensors(self) -> Union[SensorDefinition]:

        sensors_list = []

        for sensor in self.sensors_to_build:

            refresh_up_list = sensor["refresh_up_list"]

            @multi_asset_sensor(name=sensor["sensor_name"],
                                monitored_assets=[AssetKey(key) for key in refresh_up_list], #AssetSelection.assets( [AssetKey(["taxi_trips"]), AssetKey(["taxi_zones"])]
                                job_name=sensor["job_name"],
                                minimum_interval_seconds=MINIMAL_INTERVAL_SENSOR,
                                default_status=DefaultSensorStatus.RUNNING)
            def _sensor(context: MultiAssetSensorEvaluationContext):
                for asset_key, materialization in context.latest_materialization_records_by_key().items():
                    if materialization:
                        context.advance_cursor({asset_key: materialization})
                        return RunRequest()
                    
            sensors_list.append(_sensor)

        return sensors_list
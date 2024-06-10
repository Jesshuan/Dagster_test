from dagster import (
    AssetsDefinition,
    AssetSelection,
    AssetKey,
    AssetExecutionContext,
    Definitions,
    JobDefinition,
    ScheduleDefinition,
    DefaultScheduleStatus,
    define_asset_job,
    load_assets_from_modules,
    load_assets_from_package_name,
    asset,
)


from typing import List, Union
#from . import assets

import requests

#from .resources import DataGeneratorResource

import os

import requests

from . import assets

import json

import logging

specs = [
    {"descr": "debit_sans_pluie",
     "eq_id":"eq_333333",
     "model_type":"influx_operation",
     "upstream": ["eq_111111", "eq_222222"],
     "params": {"formula":"je prends le débit mais ... sans la pluie"},
     "computation_frequency":300
     },
     {"descr": "pluie",
     "eq_id":"eq_111111",
     "model_type":"source",
     "upstream": [],
     "params": {},
     "computation_frequency":60
     },
     {"descr": "debit_entrant",
     "eq_id":"eq_222222",
     "model_type":"source",
     "upstream": [],
     "params": {},
     "computation_frequency":600},
    {"descr": "prediction_debit_sans_pluie",
     "eq_id":"eq_444444",
     "model_type":"time_series",
     "upstream": ["eq_333333"],
     "params":{"day":1,
                "week":1,
                "year":0},
    "computation_frequency":[3600, 86400]}
]



def process_influx_operation(context, params):
    context.log.info(f"Influx op param process : {params}")

def process_source(context, params):
    context.log.info(f"SOURCE param process : {params}")

def process_time_series(context, params):
    context.log.info(f"TIME SERIES param process : {params}")


class Pipeline_Factory():
    def __init__(self, specs):
        self.specs = specs
        self.job_to_generate = []

        self.associated_comp_frequency = {}
        for spec in self.specs:
            self.associated_comp_frequency[spec["eq_id"]] = spec["computation_frequency"]

        self.assets_to_build = []
        self.jobs_to_build = []
        self.schedule_to_build = []

    def get_block_modelization(self, model_type):

        if model_type=="influx_operation":
            return ["inference"]
        
        elif model_type=="source":
            return ["stream_consumer"]
        
        elif model_type=="time_series":
            return ["inference","training"]
        
    def asset_process(self, context:AssetExecutionContext, model_type, params=None):

        if model_type == "influx_operation":
            process_influx_operation(context, params)

        elif model_type == "source":
            process_source(context, params)

        elif model_type == "time_series":
            process_time_series(context, params)


    def build_preprocess(self):

        for spec in self.specs:

            eq_id=spec["eq_id"]
            model_type = spec["model_type"]
            upstreams = spec["upstream"]
            descr = spec["descr"]
            computation_freq = spec["computation_frequency"]
            params = spec["params"]

            block_modelization = self.get_block_modelization(model_type)

            for block in block_modelization:

                if block == "inference" or block=="training":
                        upstream_without_cache = []
                        internal_cache = []
                        # Cache_trigger blocks
                        for upstream in upstreams:
                            print(self.associated_comp_frequency)
                            if self.associated_comp_frequency[upstream] != computation_freq:
                                name=f"{eq_id}_cache_{block}_{upstream}"
                                self.assets_to_build.append({"name":name,
                                                        "group_name":descr,
                                                        "upstreams":[upstream],
                                                        "model_type":None,
                                                        "key_prefix":None,
                                                        "params":None})
                                internal_cache.append(name)
                            else:
                                upstream_without_cache.append(upstream)

                        # Process block
                        name = eq_id if block =="inference" else f"{eq_id}_training"
                        self.assets_to_build.append({"name":name,
                                                        "group_name":descr,
                                                        "upstreams":upstream_without_cache + internal_cache,
                                                        "model_type":model_type,
                                                        "key_prefix":None,
                                                        "params":params})

                        
                elif block == "stream_consumer":
                    self.assets_to_build.append({"name":eq_id,
                                                "group_name":descr,
                                                "upstreams":None,
                                                "model_type":model_type,
                                                "key_prefix":None,
                                                "params":params
                                                })
                    
    def build_assets(self) -> Union[AssetsDefinition]:

        assets_list = []

        for asset_to_build in self.assets_to_build:
            print(asset_to_build)
            @asset(name=asset_to_build["name"], deps=asset_to_build["upstreams"], group_name=asset_to_build["group_name"], key_prefix=asset_to_build["key_prefix"])
            def _asset(context:AssetExecutionContext):
                self.asset_process(context, asset_to_build["model_type"], asset_to_build["params"])

            assets_list.append(_asset)

        return assets_list

"""    

    def build_assets_from_spec(self, spec) -> AssetsDefinition:

        descr = spec["name"]
        eq_id = spec["eq_id"]
        upstreams = set(spec["upstream"])
        model_type = spec["model_type"]
        params = spec["params"]
        computation_frequency = spec["computation_frequency"]




        @asset(name=name, non_argument_deps=upstreams, key_prefix=eq_id)
        def _asset(context:AssetExecutionContext):
            process(context, model_type, params)

        job = define_asset_job(f"{eq}_asset_job", selection=f"{eq}/_asset")

        schedule = ScheduleDefinition(
            job_name=f"{eq}_asset_job",
            cron_schedule=cron)
        
        return _asset




def build_job(spec) -> JobDefinition:
    eq = spec["eq"]
    name = spec["name"]
    job = define_asset_job(f"{eq}_asset_job", selection=f"{eq}/{name}")

    return job

def build_schedule(spec) -> ScheduleDefinition:
    eq = spec["eq"]
    cron = spec["cron"]
    schedule = ScheduleDefinition(
        job_name=f"{eq}_asset_job",
        cron_schedule=cron,
        default_status=DefaultScheduleStatus.RUNNING)
    
    return schedule


assets_list = []
job_list = []
schedule_list = []
for spec in specs:
    _asset, job, schedule = build_asset(spec)
    assets_list.append(_asset)
    job_list.append(job)
    schedule_list.append(schedule)"""

#assets_list = [build_asset(spec) for spec in specs]
#job_list = [build_job(spec) for spec in specs]
#schedule_list = [build_schedule(spec) for spec in specs]

factory = Pipeline_Factory(specs = specs)

factory.build_preprocess()


defs = Definitions(assets=factory.build_assets())


                   #jobs=[build_job(spec) for spec in specs],
                   #schedules=[build_schedule(spec) for spec in specs])



"""
    
    def build_jobs(spec) -> JobDefinition:

    hackernews_job_10minutes = define_asset_job("spec["name]", selection=AssetSelection.assets("topstories_2"))
    hackernews_job_1minute = define_asset_job("hackernews_job_1min", selection=AssetSelection.assets("most_frequent_words_2"))

    """


"""  
def define_a_job():

    @asset(group_name="pipeline_2") # add the asset decorator to tell Dagster this is an asset
    def topstory_ids_2(context: AssetExecutionContext) -> None:
        newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
        top_new_story_ids = requests.get(newstories_url).json()[:20]
        context.log.info(f"Request done with this : {top_new_story_ids}!")
        os.makedirs("data", exist_ok=True)
        with open("data/topstory_ids_2.json", "w") as f:
            json.dump(top_new_story_ids, f)

    #   Addition: define a job that will materialize the assets
    topstory_job = define_asset_job("topstory_job", selection=AssetSelection.assets("topstories_ids_2"))

    return topstory_job


all_assets = load_assets_from_modules([assets])


datagen = DataGeneratorResource()  # Make the resource

# Addition: define a job that will materialize the assets
hackernews_job_10minutes = define_asset_job("hackernews_job_10min", selection=AssetSelection.assets("topstories_2"))
hackernews_job_1minute = define_asset_job("hackernews_job_1min", selection=AssetSelection.assets("most_frequent_words_2"))

# Addition: a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
hackernews_schedule_10min = ScheduleDefinition(
    job=hackernews_job_10minutes,
    cron_schedule="*/7 * * * *",  # every hour
)

hackernews_schedule_1min = ScheduleDefinition(
    job=hackernews_job_1minute,
    cron_schedule="*/3 * * * *",  # every hour
)

defs = Definitions(
    assets=all_assets,
    schedules=[hackernews_schedule_10min, hackernews_schedule_1min],  # Addition: add the job to Definitions object (see below)
    resources={
        "hackernews_api": datagen,  # Add the newly-made resource here
    }
)

"""

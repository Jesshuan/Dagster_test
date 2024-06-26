from dagster import AssetExecutionContext

from datetime import datetime

import numpy as np


def process_influx_operation(context:AssetExecutionContext, params, sources=None, upstream_data=None):
    context.log.info("In influx function")
    context.log.info(f"Received sources from upstream : {sources}")
    context.log.info(f"Received upstream data from upstream : {upstream_data}")
    rand_value = np.random.rand(1,1)
    date = datetime.now()
    data = {"datetime":date,
            "value":rand_value}
    context.log.info(f"INFLUX OPERATION return : {data}")
    return data


def process_source(context:AssetExecutionContext, params):
    context.log.info(f"SOURCE param process : {params}")
    rand_value = np.random.rand(1,1)
    date = datetime.now()
    data = {"datetime":date,
            "value":rand_value}
    context.log.info(f"SOURCE return : {data}")
    return data

def process_cache(context:AssetExecutionContext, params, sources=None, upstream_data=None):
    context.log.info(f"Received sources from upstream : {sources}")
    context.log.info(f"Received upstream data from upstream : {upstream_data}")
    context.log.info(f"CACHE param process : {params}")
    rand_value = np.random.rand(1,1)
    date = datetime.now()
    data = {"datetime":date,
            "value":rand_value}
    context.log.info(f"CACHE return : {data}")
    return data

def process_time_series(context:AssetExecutionContext, params, sources=None, upstream_data=None):
    context.log.info(f"Received sources from upstream : {sources}")
    context.log.info(f"Received upstream data from upstream : {upstream_data}")
    context.log.info(f"TIME SERIES param process : {params}")
    rand_value = np.random.rand(1,1)
    date = datetime.now()
    data = {"datetime":date,
            "value":rand_value}
    context.log.info(f"TIME SERIES return : {data}")
    return data

def process_time_series_training(context:AssetExecutionContext, params, sources=None, upstream_data=None):
    context.log.info(f"Received sources from upstream : {sources}")
    context.log.info(f"Received upstream data from upstream : {upstream_data}")
    context.log.info(f"TIME SERIES TRAINING param process : {params}")
    rand_value = np.random.rand(1,1)
    date = datetime.now()
    data = {"datetime":date,
            "value":rand_value}
    context.log.info(f"TIME SERIES TRAINING return : {data}")
    return data

def asset_process(context:AssetExecutionContext, model_type, params=None, sources=None, upstream_data=None):
    context.log.info("In asser_process")
    #context.log.info(context.resources.in_memory_io_manager.load_input)

    if model_type == "influx_operation":
        data = process_influx_operation(context, params, sources, upstream_data)

    elif model_type == "source":
        data = process_source(context, params)

    elif model_type == "time_series":
        data = process_time_series(context, params, sources, upstream_data)

    elif model_type == "time_series_training":
        data = process_time_series_training(context, params, sources, upstream_data)

    elif model_type == "cache":
        data = process_cache(context, params, sources, upstream_data)

    return data
from dagster import Definitions, DagsterInstance, RunConfig 
from factory import defs 

# Test updating of params for an equipment

EQ_ID = "eq_333333"
DESCR = "debit_sans_pluie"
SOURCES = ["eq_111111", "eq_222222"]


if __name__ == "__main__":
    instance = DagsterInstance.get()
    job = defs.get_job_def(f"job_{EQ_ID}")
    
    sub_dict = {}

    sub_dict["interpolation_inputer"] = {'config':
                                        {'interpolation_type': "other_linear_2"}
                                        }

    for i in range(len(SOURCES) - 1):
        sub_dict[f"interpolation_inputer_{i + 2}"] = {'config':
                                                    {'interpolation_type': "other_linear-2"}
                                                        }

    sub_dict["main_process"] = {'config':
                                        {'model_type': "influx_operation", 
                                            'params': {"influx_type":"C without D - new"}
                                        }
                                    }
    sub_dict["time_preprocessing"] = {'config':
                                        {'time_preprocessing_type': 'other_shifter_2'}
                                            }

    
                

    run_config={'ops':
                    {f"{DESCR}__{EQ_ID}": 
                            {'ops': sub_dict
                                        }
                                    }
                                }
    
    print("---RUN CONFIG CUSTOM ---")
    print(run_config)
    
    result = job.execute_in_process(run_config=run_config, instance=instance)
    print(result)



"""
   Expected:
{ debit_sans_pluie__eq_333333:
 { ops: { interpolation_inputer:
         { config: { interpolation_type: (String | { env: String }) } }
                        interpolation_inputer_2: { config: { interpolation_type: (String | { env: String }) } }
                        main_process: { config: { model_type: (String | { env: String })
                            params: { } } }
                        time_preprocessing: { config: { time_preprocessing_type: (String | { env: String }) } } } } }
{'debit_sans_pluie__eq_333333':
 {'ops': {'interpolation_inputer':
            {'config': {'interpolation_type': '...'}},
        'interpolation_inputer_2': {'config': {'interpolation_type': '...'}},
        'main_process': {'config': {'model_type': '...', 'params': {}}},
        'time_preprocessing': {'config': {'time_preprocessing_type': '...'}}}}}

{'ops': {'débit_sans_pluie__eq_333333':
{'ops': {'interpolation_inputer':
    {'config': {'interpolation_type': 'interpolation_updated'}},
        'interpolation_inputer_2': {'config': {'interpolation_type': 'interpolation_updated'}},
        'time_preprocessing': {'config': {'time_preprocessing_type': 'time_preprocessing_updated'}},
        'main_process': {'config': {'model_type': 'model_type_updtaded', 'params': {'param_key_1': 'updated_param', 'param_key_2': 'updated_param'}}}}}}}
"""
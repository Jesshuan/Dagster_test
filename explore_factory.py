from factory import Pipeline_Factory, specs

factory = Pipeline_Factory(specs)

factory.build_preprocess()

result = factory.build_assets()

print("RESULT")

print(dir(result[0]))

print("ATTRIBUTES")

print(result[0].specs)

print("SOURCE ASSET")

print(dir(result[0].to_source_asset()))

print(dir(result[0].to_source_assets()))
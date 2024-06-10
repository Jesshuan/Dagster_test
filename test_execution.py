from dagster_graphql import ReloadRepositoryLocationInfo, ReloadRepositoryLocationStatus

from dagster_graphql import DagsterGraphQLClient

client = DagsterGraphQLClient("localhost", port_number=3000)


reload_info: ReloadRepositoryLocationInfo = client.reload_repository_location("tutorial")
if reload_info.status == ReloadRepositoryLocationStatus.SUCCESS:
    print("Deployment Reloaded")
else:
    raise Exception(
        "Repository location reload failed because of a "
        f"{reload_info.failure_type} error: {reload_info.message}"
    )


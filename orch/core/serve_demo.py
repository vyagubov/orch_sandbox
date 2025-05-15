import asyncio

from prefect import get_client
from orch.flow.demo.depl import Demo

async def undeploy_by_name(deployment_name: str) -> None:
    async with get_client() as client:
        deployments = await client.read_deployments()
        deployment_id = next(
            (
                deployment.id
                for deployment in deployments
                if deployment.name == deployment_name
            ), 
            None,
        )
        if deployment_id:
            await client.delete_deployment(deployment_id)

if __name__ == "__main__":

    asyncio.run(undeploy_by_name(Demo.deployment_name))
    Demo.flow.serve(name=Demo.deployment_name)
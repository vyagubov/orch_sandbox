import httpx
from prefect import flow, task
from typing import Optional

from orch.core.base import BaseDepl


@task
def get_url(url: str, params: Optional[dict[str, any]] = None):
    response = httpx.get(url, params=params)
    response.raise_for_status()
    return response.json()


class InheritBase(BaseDepl):

    @classmethod
    def flow(cls, repo_name: str = "PrefectHQ/prefect"):
        url = f"https://api.github.com/repos/{repo_name}"
        repo_stats = get_url(url)
        print(f"{repo_name} repository statistics 🤓:")
        print(f"Stars 🌠 : {repo_stats['stargazers_count']}")
        print(f"Forks 🍴 : {repo_stats['forks_count']}")


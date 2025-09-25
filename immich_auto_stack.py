#!/usr/bin/env python3

import asyncio
import logging
import sys
import os
from uuid import UUID
from typing import Callable, Optional, TypeVar, Any

from immich_client.api.search import search_assets
from immich_client.api.stacks import search_stacks, create_stack
from immich_client.client import AuthenticatedClient
from immich_client.models.asset_response_dto import AssetResponseDto
from immich_client.models.metadata_search_dto import MetadataSearchDto
from immich_client.models.stack_create_dto import StackCreateDto

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# holy shit i just replaced an entire dependency in one line
str2bool: Callable[[str], bool] = lambda v: v.strip().lower() in ("1", "true", "yes")

conversions: dict[type, Callable[[str], Any]] = {
        bool: str2bool,
        str: lambda x: x
}

seen: dict[str, int] = {}

T = TypeVar("T")
def environment(key: str, typ: type[T], default: Optional[T] = None, cast: Optional[Callable[[str], T]] = None):
    if key in os.environ:
        if cast is None:
            if typ in conversions: cast = conversions[typ]
            else: raise Exception(f"Could not convert {key} from string")
        return cast(os.environ[key])
    elif default is not None: return default
    else: raise Exception(f"Environment variable {key} not set")

class AsyncImmich:
    def __init__(self, url: str, key: str):
        self.client = AuthenticatedClient(url, key)
        # Cache for stack lookups to avoid redundant API calls
        self._stack_cache: dict[str, set[str]] = {}

    async def fetchAssets(self, size: int = 1000) -> list[AssetResponseDto]:
        page: int = 1
        assets: list[AssetResponseDto] = []

        logger.info('⬇️    Fetching assets: ')
        logger.info(f'     Page size: {size}')

        while True:
            response = await search_assets.asyncio(
                client=self.client,
                body=MetadataSearchDto(size=size, page=page, with_stacked=True)
            )

            if response is None:
                logger.error('     Error fetching assets')
                break

            assets.extend(response.assets.items)

            if response.assets.next_page is None:
                break

            page = int(response.assets.next_page)

        self.assets = assets

        logger.info(f'     Pages: {page}')
        logger.info(f'     Assets: {len(self.assets)}')

        return self.assets

    async def makeStack(self, ids: list[UUID]) -> bool:
        response = await create_stack.asyncio_detailed(
            client=self.client,
            body=StackCreateDto(asset_ids=ids)
        )

        if response.status_code.is_success:
            logger.info("    🟢 Success!")
            return True
        else:
            logger.error(f"    🔴 Error! {response.status_code} {response.content.decode('utf-8')}")
            return False

    async def isStacked(self, q_primary: AssetResponseDto, asset: AssetResponseDto) -> bool:
        primary_id = q_primary.id

        # Check cache first
        if primary_id in self._stack_cache:
            return asset.id in self._stack_cache[primary_id]

        # Fetch and cache stack info
        res = await search_stacks.asyncio(client=self.client, primary_asset_id=UUID(primary_id))

        if res is None:
            self._stack_cache[primary_id] = set()
            return False

        # Cache all asset IDs in stacks for this primary
        stacked_ids = {a.id for stk in res for a in stk.assets}
        self._stack_cache[primary_id] = stacked_ids

        return asset.id in stacked_ids

    async def batchIsStacked(self, checks: list[tuple[AssetResponseDto, AssetResponseDto]]) -> list[bool]:
        """Batch check if assets are stacked, with concurrency control"""
        semaphore = asyncio.Semaphore(10)  # Limit concurrent API calls

        async def check_with_semaphore(primary, asset):
            async with semaphore:
                return await self.isStacked(primary, asset)

        tasks = [check_with_semaphore(primary, asset) for primary, asset in checks]
        return await asyncio.gather(*tasks)

def stackAssets(data: list[AssetResponseDto]) -> dict[str, list[AssetResponseDto]]:
    filenames = [(asset.original_file_name, asset) for asset in data]
    stacks = {}

    for (filename, asset) in filenames:
        key = filename.split('.')[0]
        if key not in stacks:
            stacks[key] = [asset]
        else:
            stacks[key].append(asset)

    return stacks

def stratifyStack(stack: list[AssetResponseDto]) -> list[AssetResponseDto]:
    stack.sort(key=lambda asset: asset.original_file_name.split(".")[-1].lower() in ["jpg", "jpeg"])
    stack.reverse()
    return stack

async def processStack(immich: AsyncImmich, key: str, stack: list[AssetResponseDto],
                      stack_index: int, total_stacks: int, dry_run: bool) -> bool:
    """Process a single stack asynchronously"""
    stack = stratifyStack(stack)

    if stack[0].id not in seen or seen[stack[0].id] != len(stack[1:]):
        seen[stack[0].id] = len(stack[1:])
    else:
        logger.info(f'{stack_index}/{total_stacks} Key: {key} SKIP! Seen before!')
        return False

    # Batch check if children are already stacked
    checks = [(stack[0], child) for child in stack[1:]]
    if checks:
        stacked_results = await immich.batchIsStacked(checks)
        unstacked_children = [child for child, is_stacked in zip(stack[1:], stacked_results) if not is_stacked]
    else:
        unstacked_children = []

    if len(unstacked_children) == 0:
        logger.info(f'{stack_index}/{total_stacks} Key: {key} SKIP! No new children!')
        return False

    logger.info(f'{stack_index}/{total_stacks} Key: {key}')
    logger.info(f'     Parent name: {stack[0].original_file_name} ID: {stack[0].id}')

    for child in stack[1:]:
        logger.info(f'     Child name:    {child.original_file_name} ID: {child.id}')

    if not dry_run:
        success = await immich.makeStack([UUID(asset.id) for asset in stack])
        return success

    return True

async def processStacksConcurrently(immich: AsyncImmich, stacks: dict[str, list[AssetResponseDto]], dry_run: bool):
    """Process stacks with controlled concurrency"""
    semaphore = asyncio.Semaphore(5)  # Limit concurrent stack processing

    async def process_with_semaphore(args):
        key, stack, index = args
        async with semaphore:
            return await processStack(immich, key, stack, index, len(stacks), dry_run)

    # Create tasks for all stacks
    stack_items = [(key, stack, i) for i, (key, stack) in enumerate(stacks.items())]
    tasks = [process_with_semaphore(args) for args in stack_items]

    # Process all stacks concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Log any exceptions
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            key = stack_items[i][0]
            logger.error(f"Error processing stack {key}: {result}")

async def main():
    api_key = environment("API_KEY", str)
    api_url = environment("API_URL", str, "http://immich_server:3001/api")
    dry_run = environment("DRY_RUN", bool, False)

    if not api_key:
        logger.warning("API key is required")
        return

    logger.info('============== INITIALIZING ==============')

    if dry_run:
        logger.info('🔒    Dry run enabled, no changes will be applied')

    immich = AsyncImmich(api_url, api_key)
    assets = await immich.fetchAssets()

    stacks = stackAssets(assets)

    # Process stacks concurrently
    await processStacksConcurrently(immich, stacks, dry_run)

if __name__ == '__main__':
    asyncio.run(main())

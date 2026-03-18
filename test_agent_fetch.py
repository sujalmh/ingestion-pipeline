import asyncio
from app.agent.source_config import SourceConfig, APIEndpointConfig, AuthConfig
from app.agent.fetcher import fetch_api
from app.agent.converter import convert_and_save

async def run_tests():
    # 1. JSONPlaceholder (Standard Array)
    json_source = SourceConfig(
        source_id="mock_json",
        name="JSON Placeholder",
        base_url="https://jsonplaceholder.typicode.com",
        origin="test"
    )
    json_api = APIEndpointConfig(
        id="posts",
        name="Posts",
        endpoint="/posts",
        method="GET",
        output_format="json"
    )
    
    print("--- 1. Fetching JSON Placeholder (Standard Array) ---")
    res1 = await fetch_api(json_source, json_api)
    print(f"Fetch Success: {res1.success} | Row Count: {res1.row_count}")
    if res1.success:
        conv1 = await convert_and_save(res1)
        print(f"Conversion Success: {conv1.success} | File: {conv1.file_path}\n")

    # 2. NBP Gold Rates (Standard Array)
    nbp_source = SourceConfig(
        source_id="mock_nbp_gold",
        name="NBP Gold",
        base_url="http://api.nbp.pl",
        origin="test"
    )
    nbp_api = APIEndpointConfig(
        id="gold_30_days",
        name="Gold 30 Days",
        endpoint="/api/cenyzlota/last/30/?format=json",
        method="GET",
        output_format="json"
    )
    
    print("--- 2. Fetching NBP Gold (Standard Array) ---")
    res2 = await fetch_api(nbp_source, nbp_api)
    print(f"Fetch Success: {res2.success} | Row Count: {res2.row_count}")
    if res2.success:
        conv2 = await convert_and_save(res2)
        print(f"Conversion Success: {conv2.success} | File: {conv2.file_path}\n")

    # 3. JSONPlaceholder Paginated (Testing the new pagination engine feature!)
    # typicode supports pagination via ?_page=X&_limit=Y (total 100 rows)
    json_pag_api = APIEndpointConfig(
        id="posts_paginated",
        name="Posts Paginated",
        endpoint="/posts",
        method="GET",
        output_format="json",
        pagination={
            "type": "page",
            "page_param": "_page",
            "size_param": "_limit",
            "size": 25,          # fetch 25 records per page
            "max_pages": 4       # simulate loop stopping exactly at end of 4 pages (100 total rows)
        }
    )
    
    print("--- 3. Fetching Paginated API (Testing new feature with 4 pages x 25 limit) ---")
    res3 = await fetch_api(json_source, json_pag_api)
    print(f"Fetch Success: {res3.success} | Row Count: {res3.row_count}")
    if res3.success:
        conv3 = await convert_and_save(res3)
        print(f"Conversion Success: {conv3.success} | File: {conv3.file_path}\n")

asyncio.run(run_tests())

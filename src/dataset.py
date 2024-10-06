from pathlib import Path

import typer
from loguru import logger
from tqdm import tqdm

import aiohttp
import asyncio
from typing import Callable, List, Mapping


from config import RAW_DATA_DIR, CKAN_API_ENDPOINT, DEFAULT_PACKAGE_ID

        
app = typer.Typer()

              
async def download_all_files(api_endpoint: str = CKAN_API_ENDPOINT, 
                             package_id: str = DEFAULT_PACKAGE_ID,
                             fn_get_metadata: Callable[[aiohttp.ClientResponse],List[Mapping[str, str]]] = None) -> None:
    try:
        params = {'id': package_id}
        async with aiohttp.ClientSession() as session:
            async with session.get(CKAN_API_ENDPOINT, params=params) as response:
                if response.status == 200:
                    logger.info(f"Starting the download of all files for {package_id}")
                    content = await response.json()
                    if fn_get_metadata:
                        resource_list = await fn_get_metadata(content)
                    else:
                        resource_list = await get_traffic_metadata(content)
                    tasks = []
                    
                    for resource in resource_list:
                        name = resource.get('name')
                        url = resource.get('url')
                        
                        tasks.append(download_file(session, url, name))
                    await asyncio.gather(*tasks)
                else:
                    logger.error(f"An error occurred with status code: {response.status}")
    except Exception as e:
        logger.error(f"An exception occurred: {e}")
        
                        
                    

async def get_traffic_metadata(content: Mapping) -> List[Mapping[str, str]]:
    pacakge_info_list = content['result']['resources']
  
    resource_list = []
    for resource in pacakge_info_list:
        if resource.get('format') == 'CSV':
            resource_dict = {
                'name': resource.get('name'),
                'url': resource.get('url'),
                'resource_id': resource.get('id'),
                'package_id': resource.get('package_id')
            }
            resource_list.append(resource_dict)
    return resource_list

async def download_file(session: aiohttp.ClientSession, url: str, name: str, file_format: str = '.csv') -> None:
    async with session.get(url) as response:
        if response.status == 200:
            logger.info(f"Downloading file {name}")
            
            resource = await response.read()
            
            file_name = name + file_format
            file_destination = Path(RAW_DATA_DIR / file_name)
            with open(file_destination, 'wb') as f:
                f.write(resource)
            logger.info(f"{name} downloaded successfully")
        else:
            logger.error(f"A connection error occurred with status: {response.status}")

            

# app = typer.Typer()


# @app.command()
# # def main(
# #     # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
# #     input_path: Path = RAW_DATA_DIR / "dataset.csv",
# #     output_path: Path = PROCESSED_DATA_DIR / "dataset.csv",
# #     # ----------------------------------------------
# # ):
# #     # ---- REPLACE THIS WITH YOUR OWN CODE ----
# #     logger.info("Processing dataset...")
# #     for i in tqdm(range(10), total=10):
# #         if i == 5:
# #             logger.info("Something happened for iteration 5.")
# #     logger.success("Processing dataset complete.")
# #     # -----------------------------------------

@app.command()
def main(debug: bool = True) -> None:
    if debug:
        asyncio.run(download_all_files())
        return
    logger.info(f"Not running anything, and that's ok!")

if __name__ == "__main__":
    app()

import json
import aiohttp
import asyncio
import aiofiles
import lzma
import logging
import os

# Define the logging configuration
logging.basicConfig(filename="warframe_public_export.log", level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Define the langauges we want to download
# Language codes defined https://warframe.fandom.com/wiki/Public_Export#Available_Languages
languages = ["en", "de", "es", "fr", "it", "ja"]

# Characters to remove from JSON
characters_to_sanitize = ["\r", "\n", "\t", "\x00", "\x1f"]

# Define the Public Export URL with the given language
urls = [f"https://origin.warframe.com/PublicExport/index_{lang}.txt.lzma" for lang in languages]

def sanitize_json(json_data):
    """
    Sanitizes a JSON string by removing specific characters and replacing certain Unicode characters.

    Args:
        json_data (str): The JSON string to be sanitized.
    Returns:
        str: The sanitized JSON string.
    """
    
    for char in characters_to_sanitize:
        json_data = json_data.replace(char, "")
    json_data = json_data.replace("\u2019", "'")
    return json_data

# Define the function to fetch and save the data
async def fetch_and_save(session, url, lang):
    """
    Fetches data from a given URL, decompresses it, and saves it to a file.

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for making the HTTP request.
        url (str): The URL to fetch the data from.
        lang (str): The language code to use for naming the output file and directory.
    Raises:
        aiohttp.ClientError: If there is an HTTP error during the request.
        lzma.LZMAError: If there is an error decompressing the data.
        Exception: If there is any other unexpected error.
    Logs:
        Info: When data is successfully saved to a file.
        Error: When there is an HTTP error, decompression error, or any other unexpected error.
    """

    try:
        folder = f"data/{lang}"
        os.makedirs(folder, exist_ok=True)
        filename = f"{folder}/warframe_public_export_endpoints_{lang}.txt"

        # Check if the file already exists
        if os.path.exists(filename):
            logging.info(f"File {filename} already exists, skipping download")
            return
        
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.read()
            decompressed_data = lzma.decompress(data).decode("utf-8")
            folder = f"data/{lang}"
            os.makedirs(folder, exist_ok=True)
            filename = f"{folder}/warframe_public_export_endpoints_{lang}.txt"
            async with aiofiles.open(filename, "w") as f:
                await f.write(decompressed_data)
            logging.info(f"Data saved to {filename}.txt")
    except aiohttp.ClientError as e:
        logging.error(f"HTTP error fetching {url}: {e}")
    except lzma.LZMAError as e:
        logging.error(f"LZMA error decompressing {url}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error fetching {url}: {e}")

# Define the function to fetch and save the manifest data
async def fetch_and_save_manifest(session, item, lang):
    """
    Fetches a manifest file from the Warframe public export and saves it locally.

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for making the HTTP request.
        item (str): The item identifier, which includes the filename and hash value separated by an exclamation mark.
        lang (str): The language code to determine the folder where the file will be saved.
    Raises:
        aiohttp.ClientError: If there is an HTTP error during the request.
        Exception: If there is any other unexpected error during the process.
    Logs:
        Info: When the manifest data is successfully saved.
        Error: If there is an HTTP error or any other unexpected error.
    """

    url = f"http://content.warframe.com/PublicExport/Manifest/{item}"
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.read()

            # Extract filename and hash
            filename, hash_value = item.split("!")

            if filename == "ExportManifest.json":
                folder = "data"
            else:
                folder = f"data/{lang}"

            # Use the first part of the filename
            # e.g. ExportWeapons_en.json!00_HghAEHejKwa2JJrj9gZW3g ->  ExportWeapons_en.json
            file_path = f"{folder}/{filename}"

            async with aiofiles.open(file_path, "wb") as f:
                await f.write(data)
            logging.info(f"Manifest data saved to {file_path} with hash {hash_value}")
    except aiohttp.ClientError as e:
        logging.error(f"HTTP error fetching {url}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error fetching {url}: {e}")

async def reformat_json_file(file_path):
    """
    Asynchronously reformats a JSON file to be pretty-printed with an indentation of 4 spaces.

    Args:
        file_path (str): The path to the JSON file to be reformatted.
    Raises:
        json.JSONDecodeError: If the file contains invalid JSON.
        Exception: For any other unexpected errors during the reformatting process.
    Logs:
        Info: When the JSON data is successfully reformatted and saved.
        Error: If there is an error decoding the JSON or any unexpected error occurs.
    """

    try:
        # Adjust file path if it's ExportManifest.json
        if os.path.basename(file_path) == "ExportManifest.json":
            file_path = "data/ExportManifest.json"

        async with aiofiles.open(file_path, "r") as f:
            content = await f.read()
            sanitized_content = sanitize_json(content)
            data = json.loads(sanitized_content) # Load and validate teh JSON data

        # Reformat the JSON
        formatted_json = json.dumps(data, indent=4) # Pretty print the JSON data

        async with aiofiles.open(file_path, "w") as f:
            await f.write(formatted_json)
        logging.info(f"Reformatted JSON data saved to {file_path}")
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON file {file_path}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error reformatting JSON file {file_path}: {e}")

async def download_and_save_png(session, url, save_path):
    """
    Downloads an image from a given URL and saves it to a file.

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for making the HTTP request.
        url (str): The URL to fetch the image from.
        save_path (str): The path to save the image file.
    Raises:
        aiohttp.ClientError: If there is an HTTP error during the request.
        Exception: If there is any other unexpected error.
    Logs:
        Info: When the image is successfully saved to a file.
        Error: If there is an HTTP error or any other unexpected error.
    """

    try:
        # Check if the file already exists
        if os.path.exists(save_path):
            logging.info(f"File {save_path} already exists, skipping download")
            return
        
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.read()
            async with aiofiles.open(save_path, "wb") as f:
                await f.write(data)
            logging.info(f"Image saved to {save_path}")
    except aiohttp.ClientError as e:
        logging.error(f"HTTP error fetching {url}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error fetching {url}: {e}")

async def process_manifest_and_download_pngs(manifest_file_path, base_download_folder):
    """
    Processes a manifest file and downloads PNG images specified in the manifest.

    Args:
        manifest_file_path (str): The file path to the manifest JSON file.
        base_download_folder (str): The base folder where images will be downloaded and saved.
    Raises:
        json.JSONDecodeError: If the manifest file contains invalid JSON.
        aiohttp.ClientError: If there is an issue with the HTTP request while downloading images.
        OSError: If there is an issue creating directories or writing files.
        Exception: For any other unexpected errors during the process.
    """

    try:
        async with aiofiles.open(manifest_file_path, "r") as f:
            manifest_data = await f.read()
            manifest_data = json.loads(manifest_data)

        async with aiohttp.ClientSession() as session:
            tasks = []
            for item in manifest_data["Manifest"]:
                unique_name = item["uniqueName"]
                texture_location = item["textureLocation"].lstrip('/') # Ensure no leading slashes

                # Should be something like 
                # http://content.warframe.com/PublicExport/Lotus/Interface/Icons/StoreIcons/Weapons/MeleeWeapons/Weapons/InfBoomerang.png!00_JIrxycYjijIwKrUK7uqIWA
                image_url = f"http://content.warframe.com/PublicExport/{texture_location}"

                # We want to save the image with the original filename and not the hash
                file_name = os.path.basename(texture_location.split('!')[0])

                # Create folder structure based on unique name
                folder_structure = os.path.join(base_download_folder, os.path.dirname(unique_name.lstrip('/')))
                os.makedirs(folder_structure, exist_ok=True)

                # Download and save the image
                save_path = os.path.join(folder_structure, file_name)
                tasks.append(download_and_save_png(session, image_url, save_path))
            await asyncio.gather(*tasks)
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {e}")
        raise
    except aiohttp.ClientError as e:
        logging.error(f"HTTP error: {e}")
        raise
    except OSError as e:
        logging.error(f"OS error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

# Define the function to update the manifest JSON files with local PNG paths
async def update_manifest_with_png_paths(manifest_file_path, base_download_folder):
    """
    Asynchronously updates the manifest file with local PNG paths.
    This function reads a JSON manifest file, updates each item's "localPngPath" 
    with the corresponding local file path based on the provided base download folder, 
    and writes the updated manifest back to the file.

    Args:
        manifest_file_path (str): The path to the manifest JSON file.
        base_download_folder (str): The base folder where the PNG files are downloaded.
    Returns:
        None
    Raises:
        FileNotFoundError: If the manifest file does not exist.
        json.JSONDecodeError: If the manifest file contains invalid JSON.
        KeyError: If the expected keys are not found in the manifest data.
        OSError: If there are issues reading from or writing to the file system.
    """
    try:
        # Check if the manifest file exists
        if not os.path.exists(manifest_file_path):
            raise FileNotFoundError(f"Manifest file not found: {manifest_file_path}")
        
        # Load the ExportManifest file
        async with aiofiles.open(manifest_file_path, "r") as f:
            content = await f.read()
            manifest_data = json.loads(content)

        # Check if the manifest data contains the expected keys
        if "Manifest" not in manifest_data:
            logging.error(f"Manifest key not found in {manifest_file_path}")
            return

        for item in manifest_data["Manifest"]:
            unique_name = item["uniqueName"]
            texture_location = item["textureLocation"]
            file_name = os.path.basename(texture_location.split('!')[0])
            
            folder_structure = os.path.join(base_download_folder, os.path.dirname(unique_name.lstrip('/')))
            local_path = os.path.join(folder_structure, file_name)

            # Ensure the path is within the base_download_folder
            if not local_path.startswith(base_download_folder):
                logging.error(f"Invalid path detected: {local_path}")
                continue
            
            # Update the item with the local PNG path
            item["localPngPath"] = local_path

        # Save the updated manifest data back to the file
        async with aiofiles.open(manifest_file_path, "w") as f:
            await f.write(json.dumps(manifest_data, indent=4))
    except FileNotFoundError as e:
        logging.error(f"File not found error: {e}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {e}")
        raise
    except KeyError as e:
        logging.error(f"Key error: {e}")
        raise
    except OSError as e:
        logging.error(f"OS error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

async def update_export_files(export_manifest_path, base_data_folder):
    """
    Updates export files with image paths from an export manifest.
    This function reads an export manifest file to extract unique names and their corresponding
    local image paths. It then iterates through language folders and their respective export JSON
    files, updating each item with the appropriate image path based on the unique name.

    Args:
        export_manifest_path (str): The file path to the export manifest JSON file.
        base_data_folder (str): The base directory containing language folders with export JSON files.
    Raises:
        json.JSONDecodeError: If there is an error decoding JSON content.
        OSError: If there is an error reading or writing files.
    """

    try:
        # Chcek if the manifest file exists
        if not os.path.exists(export_manifest_path):
            raise FileNotFoundError(f"Export manifest file not found: {export_manifest_path}")

        # Load the ExportManifest file
        async with aiofiles.open(export_manifest_path, "r") as f:
            export_manifest_content = await f.read()
            export_manifest_data = json.loads(export_manifest_content)

        # Extract uniqueName and localPngPath from ExportManifest
        manifest_items = export_manifest_data.get("Manifest", [])
        unique_name_to_png_path = {item['uniqueName']: item['localPngPath'] for item in manifest_items if 'uniqueName' in item and 'localPngPath' in item}

        logging.info(f"unique_name_to_png_path contains {len(unique_name_to_png_path)} entries")

        # Iterate through each language folder
        for language_folder in os.listdir(base_data_folder):
            language_folder_path = os.path.join(base_data_folder, language_folder)
            if os.path.isdir(language_folder_path):
                # Iterate through each Export_xxxx.json file in the language folder
                for export_json_file in os.listdir(language_folder_path):
                    if export_json_file.startswith("Export") and export_json_file.endswith(".json"):
                        export_json_file_path = os.path.join(language_folder_path, export_json_file)

                        # Ensure the path is within the base_data_folder
                        if not export_json_file_path.startswith(base_data_folder):
                            logging.error(f"Invalid path detected: {export_json_file_path}")
                            continue

                        async with aiofiles.open(export_json_file_path, "r") as f:
                            export_content = await f.read()
                            export_data = json.loads(export_content)

                        # Log the type and length of export_data
                        logging.debug(f"Processing {export_json_file}: type(export_data)={type(export_data)}, len(export_data)={len(export_data)}")
                        logging.debug(f"Keys in export_data: {export_data.keys()}")

                        # Iterate through all keys in the export_data
                        for key, items in export_data.items():
                            if isinstance(items, list):
                                for item in items:
                                    if isinstance(item, dict):
                                        unique_name = item.get('uniqueName')
                                        logging.debug(f"Checking item with uniqueName: {unique_name}")
                                        if unique_name and unique_name in unique_name_to_png_path:
                                            item["imagePath"] = unique_name_to_png_path[unique_name]
                                            logging.info(f"Updated imagePath for {unique_name} in {export_json_file}")

                        # Save the updated export data back to the file
                        async with aiofiles.open(export_json_file_path, "w") as f:
                            await f.write(json.dumps(export_data, indent=4))
    except FileNotFoundError as e:
        logging.error(f"File not found error: {e}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {e}")
        raise
    except OSError as e:
        logging.error(f"OS error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

# Main function to run the script
async def main():
    async with aiohttp.ClientSession() as session:
        # Fetch and save the endpoints file
        tasks = [fetch_and_save(session, url, lang) for url, lang in zip(urls, languages)]
        await asyncio.gather(*tasks)

        # Read the endpoints file and fetch the manifest data
        export_manifest_downloaded = False
        for lang in languages:
            endpoints_file_path = f"data/{lang}/warframe_public_export_endpoints_{lang}.txt"
            async with aiofiles.open(endpoints_file_path, "r") as f:
                lines = await f.readlines()
                tasks = []
                for line in lines:
                    if "ExportManifest.json" in line and not export_manifest_downloaded:
                        tasks.append(fetch_and_save_manifest(session, line.strip(), lang))
                        export_manifest_downloaded = True
                    elif "ExportManifest.json" not in line:
                        tasks.append(fetch_and_save_manifest(session, line.strip(), lang))
                await asyncio.gather(*tasks)

        # Once the manifest data is downloaded, reformat the JSON files
        for lang in languages:
            folder = f"data/{lang}"
            for manifest_file_name in os.listdir(folder):
                if manifest_file_name.endswith(".json"):
                    file_path = os.path.join(folder, manifest_file_name)
                    await reformat_json_file(file_path)

        # Reformat the main ExportManifest.json file
        await reformat_json_file("data/ExportManifest.json")

        # Process the main ExportManifest.json file and download the PNGs
        manifest_file_path = "data/ExportManifest.json"
        base_download_folder = "data/images"
        os.makedirs(base_download_folder, exist_ok=True)
        await process_manifest_and_download_pngs(manifest_file_path, base_download_folder)

        # Update the manifest JSON files with local PNG paths
        for lang in languages:
            folder = f"data/{lang}"
            for manifest_file_name in os.listdir(folder):
                if manifest_file_name.endswith(".json"):
                    await update_manifest_with_png_paths(manifest_file_path, base_download_folder)

        # Update the Export_.json files with local PNG paths
        base_data_folder = "data"
        export_manifest_path = "data/ExportManifest.json"
        await update_export_files(export_manifest_path, base_data_folder)

# Run the main function
if __name__ == "__main__":
    logging.info("Starting download of Warframe Public Export data")
    asyncio.run(main())
    logging.info("Download finished")

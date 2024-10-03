import glob
import json
from logging.handlers import RotatingFileHandler
import aiohttp
import asyncio
import aiofiles
import lzma
import logging
import os
from datetime import datetime

date = datetime.today().strftime("%Y-%m-%d")
log_filename = f"logs/warframe_export_logs_{date}.log"

os.makedirs(os.path.dirname(log_filename), exist_ok=True)

# Define the logging configuration
logging.basicConfig(
    level="INFO",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        RotatingFileHandler(
            log_filename,
            mode="w",
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
),

logger = logging.getLogger("warframe_export")


# Define the langauges we want to download
# Language codes defined https://warframe.fandom.com/wiki/Public_Export#Available_Languages
languages = ["en", "de", "es", "fr", "it", "ja"]

# Characters to remove from JSON
# * Other characters may exist in the other languages, but I believe these are accented characters specific to that language
characters_to_sanitize = ["\r", "\n", "\t", "\x00", "\x1f"]

# Define the Public Export URL with the given language
urls = [
    f"https://origin.warframe.com/PublicExport/index_{lang}.txt.lzma"
    for lang in languages
]


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


async def check_and_update_export_files(lang):
    """
    Checks if the endpoints file exists and updates it if necessary.

    Args:
        lang (str): The language code to use for naming the output file and directory.
    Returns:
        list: A list of endpoints that need to be updated.
    """
    endpoint_file_path = f"data/{lang}/warframe_public_export_endpoints_{lang}.txt"

    if not os.path.exists(endpoint_file_path):
        logging.error(f"Endpoints file not found: {endpoint_file_path}")
        return []

    async with aiofiles.open(endpoint_file_path, "r") as f:
        logging.info(f"Reading endpoints from {endpoint_file_path}")
        new_endpoints = await f.readlines()

    files_to_update = []
    for line in new_endpoints:
        filename, new_hash = line.strip().split("!")
        split_filename, file_ext = os.path.splitext(filename)
        full_filename = f"{split_filename}_{new_hash}{file_ext}"

        # Check if the line matches the ExportManifest file
        # We're storing this differently in the base data directory
        if filename.startswith("ExportManifest"):
            logging.info("ExportManifest file detected.")
            existing_file = os.path.join("data", full_filename)
            matching_files = [
                f for f in os.listdir("data") if f.startswith("ExportManifest_")
            ]
            logging.info(f"Matching files: {matching_files} in data directory")
        else:
            logging.info(f"Checking file: {filename}")
            existing_file = os.path.join("data", lang, full_filename)
            logging.info(f"Checking file: {existing_file}")
            matching_files = [
                f
                for f in os.listdir(os.path.dirname(existing_file))
                if f.startswith(split_filename)
            ]
            logging.info(
                f"Matching files: {matching_files} in {os.path.dirname(existing_file)}"
            )

        if not any(f.endswith(f"_{new_hash}{file_ext}") for f in matching_files):
            files_to_update.append(line.strip())
            logging.info(f"File {existing_file} not found, adding to update list")
        else:
            logging.info(f"File {existing_file} does not need updating. Skipping...")
    return files_to_update


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
        filename, hash_value = item.strip().split("!")
        folder = "data" if filename == "ExportManifest.json" else f"data/{lang}"

        file_name, file_ext = os.path.splitext(filename)
        new_filename = f"{file_name}_{hash_value}{file_ext}"

        file_path = os.path.join(folder, new_filename)

        # Remove existing files with the same base name
        for existing_file in glob.glob(
            os.path.join(folder, f"{file_name}_*{file_ext}")
        ):
            if existing_file != file_path:
                os.remove(existing_file)
                logging.info(f"Removed existing file: {existing_file}")

        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.read()

            os.makedirs(folder, exist_ok=True)

            if file_ext.lower() == ".json":
                text_data = data.decode("utf-8", errors="ignore")
                sanitized_data = sanitize_json(text_data)
                parsed_data = json.loads(sanitized_data)

                async with aiofiles.open(file_path, "w") as f:
                    await f.write(json.dumps(parsed_data, indent=4))
            else:
                async with aiofiles.open(file_path, "wb") as f:
                    await f.write(data)

            logging.info(f"Manifest data saved to {file_path} with hash {hash_value}")
    except aiohttp.ClientError as e:
        logging.error(f"HTTP error fetching {url}: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error for {url}: {e}")
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
            data = json.loads(sanitized_content)  # Load and validate teh JSON data

        # Reformat the JSON
        formatted_json = json.dumps(data, indent=4)  # Pretty print the JSON data

        async with aiofiles.open(file_path, "w") as f:
            await f.write(formatted_json)
        logging.info(f"Reformatted JSON data saved to {file_path}")
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON file {file_path}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error reformatting JSON file {file_path}: {e}")


# I created this to extract the logic from the main function and make it more modular
async def download_and_save_png(session, url, save_path, hash_value):
    """
    Downloads an image from a given URL and saves it to a file.

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for making the HTTP request.
        url (str): The URL to fetch the image from.
        save_path (str): The path to save the image file.
        hash_value (str): The hash value to be used in the filename.
    Raises:
        aiohttp.ClientError: If there is an HTTP error during the request.
        Exception: If there is any other unexpected error.
    Logs:
        Info: When the image is successfully saved to a file.
        Error: If there is an HTTP error or any other unexpected error.
    """

    try:
        file_name, file_ext = os.path.splitext(os.path.basename(save_path))
        directory = os.path.dirname(save_path)
        existing_files = [
            f
            for f in os.listdir(directory)
            if f.startswith(file_name.rsplit("_", 1)[0])
        ]

        logging.info(f"Existing image files: {existing_files}")

        if any(f.endswith(f"_{hash_value}{file_ext}") for f in existing_files):
            logging.info(
                f"Image {save_path} with hash {hash_value} already exists, skipping download"
            )
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

    The manifest file will be named like: ExportManifest_<hash>.json

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
                texture_location = item["textureLocation"].lstrip(
                    "/"
                )  # Ensure no leading slashes

                # Should be something like
                # http://content.warframe.com/PublicExport/Lotus/Interface/Icons/StoreIcons/Weapons/MeleeWeapons/Weapons/InfBoomerang.png!00_JIrxycYjijIwKrUK7uqIWA
                full_url = (
                    f"http://content.warframe.com/PublicExport/{texture_location}"
                )
                image_url, hash_value = texture_location.split("!")
                file_name, file_ext = os.path.splitext(os.path.basename(image_url))
                new_file_name = f"{file_name}_{hash_value}{file_ext}"

                # Create folder structure based on unique name
                folder_structure = os.path.join(
                    base_download_folder, os.path.dirname(unique_name.lstrip("/"))
                )
                os.makedirs(folder_structure, exist_ok=True)

                # Download and save the image
                save_path = os.path.join(folder_structure, new_file_name)
                tasks.append(
                    download_and_save_png(session, full_url, save_path, hash_value)
                )
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


async def update_export_files(export_manifest_path, base_data_folder):
    """
    Updates the export files with a `imagePath` field based on the `uniqueName` field from the ExportManifest.
    Updates every Export file in `data/{lang}`

    Args:
        export_manifest_path (str): The file path to the export manifest JSON file.
        base_data_folder (str): The base folder where the data is stored.
    Raises:
        json.JSONDecodeError: If the export manifest file contains invalid JSON.
        OSError: If there is an issue reading the export manifest file or writing to the export files.
        Exception: For any other unexpected errors during the process.
    """
    logging.info(f"Starting update_export_files with manifest: {export_manifest_path}")

    async with aiofiles.open(export_manifest_path, "r") as f:
        export_manifest_content = await f.read()
        export_manifest_data = json.loads(export_manifest_content)

    unique_name_to_texture = {
        item["uniqueName"]: item["textureLocation"]
        for item in export_manifest_data["Manifest"]
        if "uniqueName" in item and "textureLocation" in item
    }

    logging.info(
        f"Found {len(unique_name_to_texture)} items with uniqueName and textureLocation"
    )

    for language_folder in os.listdir(base_data_folder):
        language_folder_path = os.path.join(base_data_folder, language_folder)
        if os.path.isdir(language_folder_path):
            logging.info(f"Processing language folder: {language_folder}")
            for json_file in os.listdir(language_folder_path):
                if json_file.endswith(".json"):
                    json_file_path = os.path.join(language_folder_path, json_file)
                    logging.info(f"Processing file: {json_file_path}")

                    async with aiofiles.open(json_file_path, "r") as f:
                        content = await f.read()
                        data = json.loads(content)

                    updates_made = 0
                    if isinstance(data, dict):
                        for key, items in data.items():
                            if isinstance(items, list):
                                for item in items:
                                    if isinstance(item, dict) and "uniqueName" in item:
                                        unique_name = item["uniqueName"]
                                        if unique_name in unique_name_to_texture:
                                            texture_location = unique_name_to_texture[
                                                unique_name
                                            ]
                                            item["imagePath"] = texture_location.split(
                                                "!"
                                            )[
                                                -2
                                            ]  # Remove the hash at the end
                                            updates_made += 1
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and "uniqueName" in item:
                                unique_name = item["uniqueName"]
                                if unique_name in unique_name_to_texture:
                                    texture_location = unique_name_to_texture[
                                        unique_name
                                    ]
                                    item["imagePath"] = texture_location.split("!")[
                                        -2
                                    ]  # Remove the hash at the end
                                    updates_made += 1

                    logging.info(f"Made {updates_made} updates in {json_file_path}")

                    if updates_made > 0:
                        async with aiofiles.open(json_file_path, "w") as f:
                            await f.write(json.dumps(data, indent=4))
                        logging.info(f"Saved updates to {json_file_path}")

    logging.info("Finished updating export files with image paths")


async def get_world_state_data(session):
    """
    Gets the world state data from the Public Endpoints

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for making the request.
    """
    urls = [
        "https://content.warframe.com/dynamic/worldState.php",
        "https://content-ps4.warframe.com/dynamic/worldState.php",
        "https://content-xb1.warframe.com/dynamic/worldState.php",
        "https://content-swi.warframe.com/dynamic/worldState.php",
    ]
    for url in urls:
        # Extract console name from URL
        console = url.split("-")[-1].split(".")[0] if "-" in url else "pc"

        try:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.text()

            # Sanitize the data
            sanitized_data = sanitize_json(data)

            # Parse the sanitized data
            parsed_data = json.loads(sanitized_data)

            # Check if there are existing world_state_data files
            # If there are, check the WorldState from the response and compare to the existing files
            # If they match, skip saving the file
            # If they don't match, save the file
            # If there are no existing files, save the file
            file_path = f"data/world_state_data_{console}.json"
            if os.path.exists(file_path):
                async with aiofiles.open(file_path, "r") as f:
                    existing_data = json.loads(await f.read())
                    logging.debug(
                        f"Existing World Seed {console}: {existing_data.get('WorldSeed')}"
                    )
                    logging.debug(
                        f"New World Seed {console}: {parsed_data.get('WorldSeed')}"
                    )
                if existing_data.get("WorldSeed") == parsed_data.get("WorldSeed"):
                    logging.info(
                        f"World state data for {console} is up to date. Skipping save."
                    )
                    continue
                else:
                    logging.info(
                        f"World state data for {console} is out of date. Saving new file with World Seed {parsed_data.get('WorldSeed')}"
                    )
                    async with aiofiles.open(file_path, "w") as f:
                        await f.write(json.dumps(parsed_data, indent=4))
                        logging.info(
                            f"Saved world state data for {console} to {file_path}"
                        )

        except aiohttp.ClientError as e:
            logging.error(f"Error fetching world state data: {e}")
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing JSON: {e}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")


# Main function to run the script
async def main():
    async with aiohttp.ClientSession() as session:
        tasks = []
        for lang, url in zip(languages, urls):
            tasks.append(fetch_and_save(session, url, lang))
        await asyncio.gather(*tasks)

        for lang in languages:
            files_to_update = await check_and_update_export_files(lang)
            if files_to_update:
                tasks = []
                for item in files_to_update:
                    tasks.append(fetch_and_save_manifest(session, item, lang))
                await asyncio.gather(*tasks)

        # Reformat all JSON files
        for lang in languages:
            folder = f"data/{lang}"
            for file_name in os.listdir(folder):
                if file_name.endswith(".json"):
                    await reformat_json_file(os.path.join(folder, file_name))

        # Process the main ExportManifest.json file and download the PNGs
        manifest_file_pattern = os.path.join("data", "ExportManifest_*.json")
        matching_files = glob.glob(manifest_file_pattern)

        if not matching_files:
            logging.error(
                "No matching files found for the pattern: %s", manifest_file_pattern
            )
            return

        manifest_file_path = matching_files[0]

        base_download_folder = "data/images"
        os.makedirs(base_download_folder, exist_ok=True)
        await process_manifest_and_download_pngs(
            manifest_file_path, base_download_folder
        )

        # Update the Export_.json files with local PNG paths
        base_data_folder = "data"
        await update_export_files(manifest_file_path, base_data_folder)

        await get_world_state_data(session)


# Run the main function
if __name__ == "__main__":
    logging.info("Starting download of Warframe Public Export data")
    asyncio.run(main())
    logging.info("Download finished")

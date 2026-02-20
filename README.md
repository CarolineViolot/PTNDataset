# PTNDataset
Collection and description code associated to the PTNDataset

# Usage
## Prior to data collection
The collection scripts expect a json file with the Telegram API configurations:
{
 "api_custom_name":
    {
        "api_id": 123456,
        "api_hash": 123456789abcdef",
        "session_name": <given default session name, useful to be re-used and not have to login each time, not useful is using several sessions per api key>
        }
}

## collect channels info
The collect_channels_info.py script expects channels data to be saved in an excel file with the channels handles in one column. 
The script should be used in the following way:
 python scripts/collect_channels_info.py \
    --config_path path/to/config/config.json \
    --api_name api_custom_name \
    --channels_file path/to/data/channels_data.xlsx \
    --handle_col handle \
    --input_json path/to/channels/data/channels.json \
    --output_json path/to/channels/data/channels.json

This will save the channel information into path/to/channels/data/channels.json. The code can be run several time, the previous channels data will be loaded and the scripts will collect the missing data and save them at the same location. The code should not be used concurrently with the same output file.

## collect posts
Before using the script, it is necessary to create a json file containing the list of channels to be collected with the specified session. This step is necessary to allow concurrent collection of channels by different sessions and different api keys. The channels list json file should look like: 
{"channel_handles": ["channel_handle_1", "channel_handle_2"]}

The script should be used in the folowing way:
 python scripts/collect_posts.py \
    --config_path path/to/config/config.json \
    --api_name api_custom_name \
    --session_name path/to/session_file/session_name.session \
    --data_folder path/to/posts \
    --channels_list_json path/to/channels_list/channels_list.json \
    --log_file path/to/log_file.log

## collect media
To collect the media, we first provide a script for workers attribution, taking into account that several API keys will be used for the media collect. The workers attribution function is in scripts/utils.py under `divide_dataset_collect_text_with_video_attribution` and an example is given in notebooks/Create json manifest for media collection.ipynb.
The manifest created by the function is necessary during media collection as it also function as a log file that maps the media_id given by telegram to the unique filename computed during media collection, consisting of the hash of the media raw bytes.

After creating the manifest, the script should be used in the following way: 
 python scripts/collect_media.py \
    --config_path path/to/config/config.json \
    --api_name api_custom_name \
    --session_name path/to/session_file/session_name.session \
    --root_folder path/to/data \
    --data_file api_distribution_media/1/manifest_api0_0.json \
    --log_file path/to/log_file.log 

import requests
import json
from google.cloud import storage

def metadata_generation_func(request):
    """
    This function processes metadata for gutenberg project, so at the time of calculating TF-IDF or processing query,
    script just check the metadata file to download book or get the info about book like author, title, subject
    """
    try:
        request_json = request.get_json(force=True, silent=True, cache=True) 

        start = None
        end = None

        if('start' in request_json and 'end' in request_json):
            start = int(request_json['start'])
            end = int(request_json['end'])
        elif('start' in request.args and 'end' in request.args):
            start = int(request.args.get('start'))
            end = int(request.args.get('end'))
        else:
            return "Please pass start, and end parameters in HTTP request!", 422

        storage_client = storage.Client()
        bucket = storage_client.bucket("mr_search")
        metadata_filename = "gutenberg_metadata.json"

        # This code download existing metadata from web, so need to run only once, after that we have already processed
        # metadata on cloud, so need to just read that file
        # metadata = load_gutenberg_metadata()
        # metadata = process_metadata(metadata)
        # update_file_on_cloud(bucket, metadata_filename, metadata)

        metadata = get_file_from_cloud(bucket, metadata_filename)
    
        # Process these book ids
        for key in range(start, end+1):
            key = str(key)
            print("TAG: Checking for: ", key)

            # if download url(.txt) is not present, remove book id from metadata
            if(key in metadata and 'download_url' not in metadata[key] and not check_for_download_url(metadata, key)):
                del metadata[key]
                continue

            # if html(.htm) url is present, add this as view_url else add download url(.txt) as view_url
            if(key in metadata and 'download_url' not in metadata[key] and not check_for_view_url(metadata, key)):
                metadata[key]['view_url'] = metadata[key]['download_url']
    except Exception as e:
        print("TAG: ", e)

    update_file_on_cloud(bucket, metadata_filename, metadata)

    return "Metadata Processed!", 200


def load_gutenberg_metadata():
    url = "https://hugovk.github.io/gutenberg-metadata/gutenberg-metadata.json"
    response = requests.get(url)

    return json.loads(response.text)

def process_metadata(metadata):
    new_metadata = {}
    for key, value in metadata.items():
        new_metadata[key] = dict()
        try:
            new_metadata[key]['author'] = metadata[key]['author'][0]
        except:
            new_metadata[key]['author'] = "Not Available!"

        try:
            new_metadata[key]['subject'] = metadata[key]['subject'][0]
        except:
             new_metadata[key]['subject'] = "Not Available!"
        
        try:
            new_metadata[key]['title'] = metadata[key]['title'][0]
        except:
             new_metadata[key]['title'] = "Not Available!"
    return new_metadata

def check_for_download_url(new_metadata, key):
    for extension in ['.txt', '-0.txt','-8.txt']:
        url = "https://www.gutenberg.org/files/{}/{}".format(key, key+extension)
        response = requests.get(url)

        if(response.ok):
            book_data = response.text
            book_data.strip()
            if(len(book_data) != 0):
                new_metadata[key]['download_url'] = url
                return True
    return False

def check_for_view_url(new_metadata, key):
    extension = ".htm"
    url = "https://www.gutenberg.org/files/{}/{}-h/{}-h{}".format(key, key, key, extension)
    response = requests.get(url)

    if(response.ok):
        book_data = response.text
        book_data.strip()
        if(len(book_data) != 0):
            new_metadata[key]['view_url'] = url
            return True
    
    return False

def update_file_on_cloud(bucket, metadata_filename, metadata):
    blob = bucket.blob(metadata_filename)
    generation_match_precondition = 0

    if(blob.exists()):
        blob.reload()
        generation_match_precondition = blob.generation
    
    blob.upload_from_string(json.dumps(metadata), if_generation_match = generation_match_precondition)

def get_file_from_cloud(bucket, metadata_filename):
    blob = bucket.blob(metadata_filename)
    content = blob.download_as_string()

    return json.loads(content)
    

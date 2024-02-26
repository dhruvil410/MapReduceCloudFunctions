import json
from google.cloud import storage
import requests
def download_dataset_func(request):
    """
    If user has requested calculate TF-IDF for book ids from start to end, then this function downloads requested books and stores
    it in input_dir
    But before downloading it checks, TF-IDF is already calculated or not and book is downloadable or not 
    """
    try:
        request_json = request.get_json(force=True, silent=True, cache=True)  

        bucket_name = None
        start = None
        end = None

        if(request_json and 'bucket_name' in request_json and 'start' in request_json and 'end' in request_json):
            bucket_name = request_json['bucket_name']
            start = int(request_json['start'])
            end = int(request_json['end'])
        elif(request.args and 'bucket_name' in request.args and 'start' in request.args and 'end' in request.args):
            bucket_name = request.args.get('bucket_name')
            start = int(request.args.get('start'))
            end = int(request.args.get('end'))
        else:
            return "Please pass bucket_name, start, and end parameters in HTTP request!", 422

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        metadata_filename = "gutenberg_metadata.json"
        tf_idf_filename = "tf_idf_metadata.json"

        metadata = load_json_file(bucket, metadata_filename)
        tf_idf = load_tf_idf(bucket, tf_idf_filename)

        for id in range(start, end+1):
            id = str(id)
            if(id not in tf_idf['doc_ids'] and id in metadata):
                print("TAG: Downloading file ", id)
                download_book(bucket, id, metadata[id]['download_url'])

        return 'Download Dataset Request Completed!', 200
    except Exception as e:
        print("TAG: ", e)
        return 'Download Dataset Request is not Completed', 500


def load_json_file(bucket, filename):
    blob = bucket.blob(filename)
    content = blob.download_as_string()

    return json.loads(content)

def load_tf_idf(bucket, tf_idf_filename):
    blob = bucket.blob(tf_idf_filename)

    if(not blob.exists()):
        base_data = {'doc_count' : 0, 'doc_ids' : [], 'terms' : {}}
        blob.upload_from_string(json.dumps(base_data), if_generation_match = 0)

    return load_json_file(bucket, tf_idf_filename)

def download_book(bucket, book_id, download_url):
    book_name = "dataset/" + book_id + ".txt"
    blob = bucket.blob(book_name)

    if(blob.exists()):
        return
    
    # if required book is available in database, copy from there, if not download from gutenberg project
    dataset_path = "dataset/" + book_id + ".txt"
    dataset_blob = bucket.blob(dataset_path)
    if(dataset_blob.exists()):
        bucket.copy_blob(dataset_blob, bucket, book_name, if_generation_match=0,)
    
    else:
        content = requests.get(download_url)
        data = content.text
        data.strip()

        blob.upload_from_string(data, if_generation_match = 0)





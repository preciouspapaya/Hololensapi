from flask import Flask, send_file, Response, request, jsonify
from azure.storage.blob import BlobServiceClient
from io import BytesIO  # <- Importing BytesIO

app = Flask(__name__)

# Azure Storage Account Connection String
connect_str = "DefaultEndpointsProtocol=https;AccountName=3dptv001;AccountKey=fs63cNDBG5qxlio4DoeQrUuPxLE9G491OALt1HuqPWznYhoC3KOxDfjQT9rjGgFVJ3OFL0EyZHou+ASt/xRRSA==;EndpointSuffix=core.windows.net"

# Blob Service Client Object
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

@app.route('/upload', methods=['POST'])
def upload_file():
    container_client = blob_service_client.get_container_client('input-container/unprocessed_dataset')

    file = request.files['file']
    blob_client = container_client.get_blob_client(file.filename)

    blob_client.upload_blob(file.read())
    return "File uploaded", 200

@app.route('/download/<container_name>', methods=['GET'])
def download_container(container_name):
    container_client = blob_service_client.get_container_client(container_name)
    blob_names = [blob.name for blob in container_client.list_blobs()]
    
    return jsonify(blob_names)

@app.route('/download/<container_name>/<blob_name>', methods=['GET'])
def download_blob(container_name, blob_name):
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    blob_data = blob_client.download_blob()
    
    # Creating a file-like object from bytes
    file_like_object = BytesIO(blob_data.readall())
    
    # Sending the file-like object as a response
    return send_file(file_like_object, as_attachment=True, download_name=blob_name, mimetype='application/octet-stream')


if __name__ == '__main__':
    app.run(debug=True)

import os
import logging
import paramiko
from google.cloud import secretmanager
from typing import Dict, Optional, Union, BinaryIO

def get_secret(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def upload_to_sftp(local_file_path: str, remote_filename: str, project_id: str) -> bool:
    try:
        # Get SFTP credentials from Secret Manager
        sftp_host = get_secret(project_id, "SFTP_HOST")
        sftp_username = get_secret(project_id, "SFTP_USERNAME")
        sftp_password = get_secret(project_id, "SFTP_PASSWORD")
        sftp_directory = get_secret(project_id, "SFTP_DIRECTORY")
        
        # Set up connection
        transport = paramiko.Transport((sftp_host, 22))
        transport.connect(username=sftp_username, password=sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        # Navigate to the correct directory
        try:
            sftp.chdir(sftp_directory)
        except IOError:
            logging.error(f"Directory {sftp_directory} not found or inaccessible")
            transport.close()
            return False
        
        # Upload the file
        remote_path = f"{remote_filename}"
        sftp.put(local_file_path, remote_path)
        
        # Close connection
        sftp.close()
        transport.close()
        
        logging.info(f"Successfully uploaded {local_file_path} to {sftp_directory}/{remote_filename}")
        return True
        
    except Exception as e:
        logging.error(f"Error uploading file to SFTP: {str(e)}")
        return False

def process_csv_upload(data: Union[bytes, BinaryIO], filename: str, project_id: str) -> Dict:
    try:
        # Save data to temp file
        temp_path = f"/tmp/{filename}"
        
        if isinstance(data, bytes):
            with open(temp_path, "wb") as f:
                f.write(data)
        else:
            with open(temp_path, "wb") as f:
                f.write(data.read())
        
        # Upload to SFTP
        success = upload_to_sftp(temp_path, filename, project_id)
        
        # Clean up temp file
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        if success:
            return {"status": "success", "message": f"File {filename} uploaded successfully"}
        else:
            return {"status": "error", "message": "Failed to upload file"}
            
    except Exception as e:
        logging.error(f"Error processing upload: {str(e)}")
        if os.path.exists(f"/tmp/{filename}"):
            os.remove(f"/tmp/{filename}")
        return {"status": "error", "message": str(e)}

import requests

def upload_file_to_api(file_path, access_code):
    """
    Uploads a file to the API endpoint with the given access code.
    
    Args:
        file_path (str): Path to the file to be uploaded
        access_code (str): API access code for authorization
        
    Returns:
        dict: Response from the API
    """
    # Extract filename from file path
    filename = file_path.split('/')[-1]
    
    # Construct the URL with filename
    url = f"https://www.example.com/{filename}"
    
    # Set up headers
    headers = {
        'x-api-code': access_code
    }
    
    try:
        # Open the file in binary mode and prepare for multipart upload
        with open(file_path, 'rb') as file:
            files = {
                'file': (filename, file, 'binary/octet-stream')
            }
            
            # Make the POST request
            response = requests.post(
                url,
                headers=headers,
                files=files
            )
            
            # Return the response
            return {
                'status_code': response.status_code,
                'response': response.json() if response.content else None
            }
            
    except FileNotFoundError:
        return {'error': 'File not found'}
    except Exception as e:
        return {'error': str(e)}

# Example usage
if __name__ == "__main__":
    # Replace these with your actual values
    file_to_upload = "path/to/your/file.ext"
    api_access_code = "your-api-access-code"
    
    result = upload_file_to_api(file_to_upload, api_access_code)
    print("Upload Result:", result)

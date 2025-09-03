#!/usr/bin/env python3

import os
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_all_folders(bucket_name):
    """Get all top-level folders in the bucket."""
    s3_client = boto3.client('s3')
    folders = set()
    
    print("Listing all objects to find folders...")
    
    # First try with delimiter to get explicit folders
    paginator = s3_client.get_paginator('list_objects_v2')
    
    # Try first with delimiter to get explicit folders
    for page in paginator.paginate(Bucket=bucket_name, Delimiter='/'):
        # Get common prefixes (folders)
        if 'CommonPrefixes' in page:
            for prefix in page['CommonPrefixes']:
                folders.add(prefix['Prefix'])
                
    # If no folders found, try listing all objects and extract folders
    if not folders:
        print("No explicit folders found, scanning all objects...")
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    # If key contains a slash, it's in a folder
                    if '/' in key:
                        # Get the top-level folder
                        folder = key.split('/')[0] + '/'
                        folders.add(folder)
    
    folders_list = sorted(folders)
    print(f"Found {len(folders_list)} folders: {', '.join(folders_list)}")
    return folders_list

def check_folder_contents(bucket_name, folders):
    """Check if each folder in the list is empty or not."""
    s3_client = boto3.client('s3')
    
    print(f"\nChecking folders in bucket: {bucket_name}")
    print("-" * 50)
    
    empty_folders = []
    non_empty_folders = []
    
    for folder in folders:
        # List objects with the folder prefix
        try:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=folder,
                MaxKeys=2  # Get 2 to differentiate between empty and folder marker
            )
            
            # Get the contents if any
            contents = response.get('Contents', [])
            
            # A folder is empty if it has no contents or only has itself as a marker
            is_empty = len(contents) == 0 or (
                len(contents) == 1 and contents[0]['Key'] == folder
            )
            
            status = "EMPTY" if is_empty else "NOT EMPTY"
            print(f"{folder:<30} - {status} ({len(contents)} objects)")
            
            if is_empty:
                empty_folders.append(folder)
            else:
                non_empty_folders.append(folder)
                
        except Exception as e:
            print(f"Error checking folder {folder}: {e}")
    
    # Print summary
    print("\nSummary:")
    print("-" * 50)
    print(f"Total folders found: {len(folders)}")
    print(f"\nEmpty folders ({len(empty_folders)}):")
    if empty_folders:
        for folder in empty_folders:
            print(f"  - {folder}")
    else:
        print("  None")
        
    print(f"\nNon-empty folders ({len(non_empty_folders)}):")
    if non_empty_folders:
        for folder in non_empty_folders:
            print(f"  - {folder}")
    else:
        print("  None")

def main():
    # Get bucket name from environment
    bucket_name = os.getenv("S3_LONG_TERM_BUCKET")
    if not bucket_name:
        print("Error: S3_LONG_TERM_BUCKET environment variable not set")
        return
    
    try:
        print(f"Discovering folders in bucket {bucket_name}...")
        folders = get_all_folders(bucket_name)
        
        if not folders:
            print("No folders found in the bucket.")
            return
            
        check_folder_contents(bucket_name, folders)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main() 
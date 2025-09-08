import os
import requests
import sys
from .field_mapping import PROPERTY_FIELDS, SELLER_FIELDS

# Get environment variables with proper error handling
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
PROPERTY_TABLE = os.getenv("PROPERTY_TABLE", "Properties")  # Default to "Properties" if not specified
SELLER_TABLE = os.getenv("SELLER_TABLE", "Sellers")  # Default to "Sellers" if not specified

# Validate required environment variables
if not AIRTABLE_API_KEY:
    print("❌ Error: AIRTABLE_API_KEY environment variable is not set.")
    print("   Please add AIRTABLE_API_KEY to your .env file.")
    sys.exit(1)

if not AIRTABLE_BASE_ID:
    print("❌ Error: AIRTABLE_BASE_ID environment variable is not set.")
    print("   Please add AIRTABLE_BASE_ID to your .env file.")
    sys.exit(1)

if not PROPERTY_TABLE:
    print("⚠️ Warning: PROPERTY_TABLE environment variable is not set.")
    print("   Using default table name: 'Properties'")
    
if not SELLER_TABLE:
    print("⚠️ Warning: SELLER_TABLE environment variable is not set.")
    print("   Using default table name: 'Sellers'")

def clean_record_data(data, valid_fields=None):
    """
    Clean record data by removing None values and validating fields.
    
    Args:
        data (dict): Raw data dictionary
        valid_fields (list): List of valid field names to include
        
    Returns:
        dict: Cleaned data with only non-None values and valid fields
    """
    if not data:
        return {}
    
    # Remove None values, empty strings, and invalid fields
    cleaned_data = {}
    for k, v in data.items():
        # Skip None values and empty strings
        if v is None or v == "":
            continue
            
        # If valid_fields is provided, only include fields in that list
        if valid_fields is not None and k not in valid_fields:
            print(f"⚠️ Warning: Field '{k}' is not in the defined schema and will be skipped")
            continue
            
        cleaned_data[k] = v
        
    return cleaned_data

def upload_to_airtable(data, table_name, record_type="Property", valid_fields=None):
    """
    Upload data to a specific Airtable table
    
    Args:
        data (dict): Data to upload
        table_name (str): Name of the Airtable table
        record_type (str): Type of record (for logging)
        valid_fields (list): List of valid field names allowed in this table
        
    Returns:
        dict: Response from Airtable API or None if failed
    """
    if not data:
        print(f"⚠️ Warning: No {record_type.lower()} data provided to upload")
        return None
    
    # Select appropriate field validation list based on record type
    if valid_fields is None:
        if record_type.lower() == "property":
            valid_fields = PROPERTY_FIELDS
        elif record_type.lower() == "seller":
            valid_fields = SELLER_FIELDS
        
    # Clean the data with field validation
    cleaned_data = clean_record_data(data, valid_fields)
    
    if not cleaned_data:
        print(f"⚠️ Warning: All {record_type.lower()} fields were empty, None, or invalid")
        return None
    
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_name}"
    
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "fields": cleaned_data
    }
    
    try:
        print(f"📤 Uploading {record_type} to '{table_name}' table...")
        print(f"   Fields: {', '.join(cleaned_data.keys())}")
        
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code in [200, 201]:
            print(f"✅ {record_type} uploaded to Airtable successfully.")
            return response.json()
        else:
            print(f"❌ Failed to upload {record_type.lower()}. Status: {response.status_code}")
            print(f"Response: {response.text}")
            
            # Provide more helpful error messages based on status code
            if response.status_code == 404:
                print("   This may be due to an incorrect BASE_ID or table name.")
                print(f"   Current BASE_ID: {AIRTABLE_BASE_ID}")
                print(f"   Current table name: {table_name}")
            elif response.status_code == 401:
                print("   This may be due to an invalid API key.")
            elif response.status_code == 422:
                print("   This is due to field name mismatch. Please check that your table has these fields.")
                print("   Fields being uploaded: ", list(cleaned_data.keys()))
            
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error when uploading to Airtable: {str(e)}")
        return None

def upload_property_to_airtable(property_data):
    """
    Upload property data to Airtable (for backwards compatibility)
    
    Args:
        property_data (dict): Property data to upload
        
    Returns:
        dict: Response from Airtable API or None if failed
    """
    return upload_to_airtable(property_data, PROPERTY_TABLE, "Property", PROPERTY_FIELDS)

def upload_seller_to_airtable(seller_data):
    """
    Upload seller data to Airtable
    
    Args:
        seller_data (dict): Seller data to upload
        
    Returns:
        dict: Response from Airtable API or None if failed
    """
    return upload_to_airtable(seller_data, SELLER_TABLE, "Seller", SELLER_FIELDS)

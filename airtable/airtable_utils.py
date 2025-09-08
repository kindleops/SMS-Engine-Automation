import os
import requests
import sys
import json
from dotenv import load_dotenv
from .field_mapping import (
    PROPERTY_FIELDS, SELLER_FIELDS, MORTGAGE_FIELDS, COMPANY_FIELDS,
    COMPANY_CONTACT_FIELDS, PHONE_FIELDS, EMAIL_FIELDS, AOD_FIELDS,
    PROBATE_FIELDS, LIEN_FIELDS, FORECLOSURE_FIELDS
)

# Ensure we have environment variables loaded
load_dotenv()

# Airtable API configurations
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")
DEFAULT_TABLE = os.getenv("DEFAULT_TABLE", "Properties")

# Validate critical environment variables
def validate_env_vars():
    """Validates that required environment variables are set"""
    missing_vars = []
    
    if not AIRTABLE_TOKEN:
        missing_vars.append("AIRTABLE_TOKEN")
    if not BASE_ID:
        missing_vars.append("BASE_ID")
        
    if missing_vars:
        print("❌ Error: Missing required environment variables:")
        for var in missing_vars:
            print(f"  - {var}")
        print("\nPlease add these to your .env file")
        return False
    return True

# Check environment variables at module import time
ENV_VARS_VALID = validate_env_vars()

# Table mapping for different record types
TABLE_MAPPING = {
    "property": os.getenv("PROPERTY_TABLE", "Properties"),
    "seller": os.getenv("SELLER_TABLE", "Sellers"),
    "mortgage": os.getenv("MORTGAGE_TABLE", "Mortgages"),
    "company": os.getenv("COMPANY_TABLE", "Companies"),
    "company_contacts": os.getenv("COMPANY_CONTACTS_TABLE", "CompanyContacts"),
    "phones": os.getenv("PHONES_TABLE", "Phones"),
    "emails": os.getenv("EMAILS_TABLE", "Emails"),
    "aod": os.getenv("AOD_TABLE", "AssignmentOfDeeds"),
    "probate": os.getenv("PROBATE_TABLE", "Probates"),
    "liens": os.getenv("LIENS_TABLE", "Liens"),
    "foreclosures": os.getenv("FORECLOSURES_TABLE", "Foreclosures")
}

def consolidate_property_data(data):
    """
    Consolidates all property-related data into a single flat record for Airtable.
    This prevents needing multiple tables and simplifies the data structure.
    
    Args:
        data: Dictionary with property data structure
        
    Returns:
        Flattened record suitable for a single Airtable table
    """
    consolidated = {}
    
    # Add property data with field prefixes
    if data.get("property"):
        for key, value in data["property"].items():
            if value is not None:  # Only add non-None values
                consolidated[f"Property_{key}"] = value
    
    # Add seller data with field prefixes
    if data.get("seller"):
        for key, value in data["seller"].items():
            if value is not None:
                consolidated[f"Seller_{key}"] = value
    
    # Add mortgage data (take first item if it's a list)
    if data.get("mortgage") and isinstance(data["mortgage"], list) and data["mortgage"]:
        for key, value in data["mortgage"][0].items():
            if value is not None:
                consolidated[f"Mortgage_{key}"] = value
    elif data.get("mortgage"):
        for key, value in data["mortgage"].items():
            if value is not None:
                consolidated[f"Mortgage_{key}"] = value
                
    # Add company data
    if data.get("company"):
        for key, value in data["company"].items():
            if value is not None:
                consolidated[f"Company_{key}"] = value
    
    # For lists, we can serialize them to JSON strings
    list_fields = ["phones", "emails", "company_contacts", "aod", "probate", "liens", "foreclosures"]
    for field in list_fields:
        if data.get(field) and any(data[field]):
            # Filter out records with all None values
            valid_items = [item for item in data[field] if any(v is not None for v in item.values())]
            if valid_items:
                consolidated[f"JSON_{field}"] = json.dumps(valid_items)
    
    # Add ZIP code if it exists in the data
    if data.get("zip"):
        consolidated["ZIP_Code"] = data["zip"]
        
    return consolidated

def upload_to_airtable(data, use_consolidated=True):
    """
    Uploads data to Airtable - either as consolidated record or multi-table.
    
    Args:
        data: Dictionary with record types as keys and record data as values
        use_consolidated: Whether to consolidate into a single record (default: True)
              
    Returns:
        Dictionary with record types as keys and lists of created record IDs as values
    """
    # Check if environment variables are valid
    if not ENV_VARS_VALID:
        print("⚠️ Cannot upload to Airtable: Missing required environment variables")
        return {}
    
    record_ids = {}
    
    # If consolidation requested, use a simpler approach with a single table
    if use_consolidated:
        consolidated_data = consolidate_property_data(data)
        record_id = _upload_record_to_airtable(DEFAULT_TABLE, consolidated_data)
        
        if record_id:
            # Return success with the consolidated record ID
            return {"property": [record_id]}
        else:
            # Try fallback to a simplified payload
            print("⚠️ Attempting fallback with minimal data...")
            minimal_data = {
                "Source": "DealMachine Scraper",
                "Status": "Imported"
            }
            
            # Add at least one field from the original data
            if consolidated_data.get("ZIP_Code"):
                minimal_data["ZIP_Code"] = consolidated_data["ZIP_Code"]
                
            record_id = _upload_record_to_airtable(DEFAULT_TABLE, minimal_data)
            if record_id:
                return {"property": [record_id]}
            return {}
            
    # Original multi-table approach (only used if consolidation disabled)
    for record_type, record_data in data.items():
        if not record_data:
            continue
            
        table_name = TABLE_MAPPING.get(record_type)
        if not table_name:
            print(f"⚠️ No table mapping found for record type: {record_type}")
            continue
            
        # Convert to list if it's a single record
        records = record_data if isinstance(record_data, list) else [record_data]
        record_ids[record_type] = []
        
        for record in records:
            record_id = _upload_record_to_airtable(table_name, record)
            if record_id:
                record_ids[record_type].append(record_id)
    
    return record_ids

def _upload_record_to_airtable(table_name, record_data):
    """
    Helper function to upload a single record to an Airtable table.
    
    Args:
        table_name: Name of the Airtable table
        record_data: Dictionary containing the record fields
        
    Returns:
        Record ID if successful, None otherwise
    """
    # Remove any None values from the record data
    cleaned_data = {k: v for k, v in record_data.items() if v is not None}
    
    # If there's no data after cleaning, don't attempt to upload
    if not cleaned_data:
        print(f"⚠️ No valid data to upload to {table_name}")
        return None
    
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table_name}"
    
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "fields": cleaned_data
    }
    
    try:
        # Print debug info before uploading
        print(f"📤 Uploading to Airtable: {table_name}")
        print(f"🔗 URL: {url}")
        
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code in [200, 201]:
            print(f"✅ Record uploaded to {table_name}")
            return response.json().get("id")
        elif response.status_code == 404:
            print(f"❌ Failed to upload to {table_name}. Status: 404 (Not Found)")
            print("   This typically means the table does not exist or BASE_ID is incorrect")
            print(f"   BASE_ID being used: {BASE_ID}")
            print(f"   Table name being used: {table_name}")
            return None
        elif response.status_code == 401 or response.status_code == 403:
            print(f"❌ Failed to upload to {table_name}. Status: {response.status_code} (Authentication Error)")
            print("   Please check your AIRTABLE_TOKEN for accuracy and permissions")
            return None
        else:
            print(f"❌ Failed to upload to {table_name}. Status: {response.status_code}")
            print(f"   Response: {response.text}")
            return None
    except requests.exceptions.ConnectionError:
        print(f"❌ Connection error uploading to {table_name}. Check your internet connection.")
        return None
    except Exception as e:
        print(f"❌ Error uploading to {table_name}: {str(e)}")
        return None

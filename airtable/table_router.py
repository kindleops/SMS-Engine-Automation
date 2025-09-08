from .property_uploader import upload_property_to_airtable, upload_seller_to_airtable
from .field_mapping import PROPERTY_FIELDS, SELLER_FIELDS

def route_and_upload(data):
    """
    Routes property data to appropriate Airtable tables and handles record linking.
    
    Args:
        data (dict): Property data containing all scraped information
        
    Returns:
        dict: Dictionary containing created record IDs
    """
    # Initialize record IDs
    record_ids = {
        'property_id': None,
        'seller_id': None
    }
    
    try:
        # Add a minimal set of fields for testing
        if data.get('property'):
            property_data = data['property']
            
            # Create sample property data with valid field names from the schema
            if all(v is None for v in property_data.values()):
                # For testing: Add sample data using ONLY fields that exist in the schema
                property_data = {
                    'property_zip_code': data.get('zip', {}).get('zip', '00000'),
                    'property_type': 'Unknown',
                    'estimated_value': 0
                }
            
            # Upload main property record
            response = upload_property_to_airtable(property_data)
            if response and response.get('id'):
                record_ids['property_id'] = response['id']
        
        # Upload associated seller information if available
        if data.get('seller'):
            seller_data = data['seller']
            
            # Create sample seller data with valid field names from the schema
            if all(v is None for v in seller_data.values()):
                # For testing: Add sample data using ONLY fields that exist in the schema
                seller_data = {
                    'marital_status': 'Unknown',
                    'household_size': 0
                }
                
            # Add property link if available
            if record_ids['property_id']:
                # Only add property_link if it's a valid field in SELLER_FIELDS
                if 'property_link' in SELLER_FIELDS:
                    seller_data['property_link'] = [record_ids['property_id']]
                
            # Upload to the seller table
            response = upload_seller_to_airtable(seller_data)
            if response and response.get('id'):
                record_ids['seller_id'] = response['id']
        
        return record_ids
        
    except Exception as e:
        print(f"❌ Error during upload: {str(e)}")
        return record_ids

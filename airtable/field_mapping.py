# PROPERTY TABLE
PROPERTY_FIELDS = [
    "full_address", "street_address", "property_city", "seller_name", "property_state", "property_zip_code",
    "property_county", "property_id", "first_name", "last_name", "corporate_owned", "market_status",
    "sale_date", "last_sale_price", "estimated_equity_amount", "equity_percent", "estimated_value",
    "property_type", "living_area_sqft", "bedrooms", "bathrooms", "year_built", "effective_year_built",
    "construction_type", "building_style", "number_of_units", "number_of_commercial_units",
    "number_of_buildings", "stories", "garage_area", "heating_type", "heating_fuel", "air_conditioning",
    "basement", "deck", "exterior_walls", "interior_walls", "number_of_fireplaces", "floor_cover",
    "garage", "driveway", "other_rooms", "pool", "patio", "porch", "roof_cover", "roof_type",
    "sewer", "topography", "water", "geographic_features", "active_lien", "apn_number", "lot_size_acres",
    "lot_size_sqft", "legal_description", "subdivision_name", "property_class", "county_land_use_code",
    "county_name", "census_tract", "lot_number", "school_district", "zoning", "flood_zone", "tax_delinquent",
    "tax_delinquent_year", "tax_year", "tax_amount", "assessment_year", "total_assessed_value",
    "assessed_land_value", "assessed_improvement_value", "total_market_value", "market_land_value",
    "market_improvement_value", "estimated_repair_cost", "building_condition", "repair_cost_per_sqft",
    "building_quality", "property_flag_1", "property_flag_2", "property_flag_3", "property_flag_4",
    "property_flag_5", "property_flag_6", "property_flag_7", "property_flag_8", "property_flag_9",
    "property_flag_10", "property_flags", "tax_mailing_address", "tax_mailing_city",
    "tax_mailing_state", "tax_mailing_zip_code",
    "hoa_name", "hoa_type", "hoa_fee", "hoa_fee_frequency",
    "second_hoa_name", "second_hoa_type", "second_hoa_fee", "second_hoa_fee_frequency"
]

# SELLER / OWNER TABLE
SELLER_FIELDS = [
    "full_name", "date_of_birth", "age", "gender", "marital_status", "preferred_language",
    "number_of_children", "household_size", "pet_owner", "previous_address", "mailing_address",
    "length_of_residence", "education", "occupation_group", "occupation", "income_tier",
    "estimated_household_income", "net_asset_value", "consumer_type", "spender_type",
    "card_balance", "investment_type", "buying_power", "total_properties_owned",
    "portfolio_value", "total_equity", "total_mortgage_balance",
    "tag_1", "tag_2", "tag_3", "tag_4", "tag_5", "tag_6", "tag_7"
]

# MORTGAGE INFO TABLE
MORTGAGE_FIELDS = [
    "mortgage_position", "original_loan_amount", "estimated_interest_rate",
    "estimated_loan_payment", "last_recording_date", "estimated_loan_balance",
    "loan_term", "loan_type", "financing_type", "loan_maturity_date", "lender_name"
]

# COMPANY TABLE
COMPANY_FIELDS = [
    "company_name", "total_properties_owned", "total_portfolio_value",
    "total_mortgage_balance", "total_equity", "mailing_address"
]

# COMPANY CONTACTS TABLE
COMPANY_CONTACT_FIELDS = [
    "full_name", "company_name", "title_or_role", "phone_number", "email_address"
]

# PHONE NUMBERS TABLE
PHONE_FIELDS = [
    "phone_number", "active_status", "phone_type", "usage_type", "carrier",
    "prepaid_line", "dnc", "phone_1_contacted", "last_contacted_date",
    "last_contact_method", "response_type"
]

# EMAILS TABLE
EMAIL_FIELDS = [
    "email_address", "email_deliverability"
]

# AOD TABLE
AOD_FIELDS = [
    "document_type", "document_title", "document_title_text", "primary_party_role",
    "primary_party_name", "secondary_party_role", "secondary_party_name",
    "trust_name", "date_of_death"
]

# PROBATE TABLE
PROBATE_FIELDS = [
    "document_type", "document_title", "document_title_text",
    "deceased_or_estate", "survivor_or_heir", "administrator_or_executor"
]

# LIENS TABLE
LIEN_FIELDS = [
    "document_type", "document_title", "document_title_text",
    "deceased_or_estate", "survivor_or_heir", "administrator_or_executor"
]

# FORECLOSURE TABLE
FORECLOSURE_FIELDS = [
    "default_date", "unpaid_balance", "past_due_amount", "due_date", "lender_name",
    "foreclosure_document_recording_date", "document_type", "auction_date",
    "auction_time", "auction_location", "auction_minimum_bid_amount",
    "auction_city", "trustee_name", "trustee_address", "trustee_phone_number",
    "trustee_case_number"
]
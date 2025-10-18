# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

REI SMS Engine is a real estate investment automation platform combining web scraping of DealMachine for property leads and an SMS campaign management system. The system is deployed on Render with scheduled cron jobs for automated outbound messaging and inbound response processing.

## Technology Stack

- **Backend**: Python 3.x with FastAPI
- **Web Scraping**: Selenium WebDriver (Brave browser automation)
- **Database**: Airtable (multiple bases for leads, campaigns, performance tracking)
- **SMS**: TextGrid API for message delivery
- **Deployment**: Render.com with cron job scheduling
- **Dependencies**: See `requirements.txt`

## Common Development Commands

### Setup & Environment
```bash
# Install dependencies
pip install -r requirements.txt

# Activate virtual environment (if using)
source .venv/bin/activate

# Set up environment variables (create .env file in project root)
# Required vars: DEALMACHINE_EMAIL, DEALMACHINE_PASSWORD, BRAVE_PATH, CHROMEDRIVER_PATH
# AIRTABLE_API_KEY, TEXTGRID_API_KEY, etc.
```

### Running Components

#### SMS Engine (FastAPI Server)
```bash
# Start the main SMS engine server
uvicorn sms.main:app --host 0.0.0.0 --port 8000 --reload

# Production command (as used in render.yaml)
uvicorn sms.main:app --host 0.0.0.0 --port 10000
```

#### Property Scraping
```bash
# Run basic scraping workflow
python run.py

# Run autopilot mode (requires market configuration)
python autopilot.py

# Test ZIP search functionality
python test_zip_search.py
```

#### Manual SMS Operations
```bash
# Test outbound batch endpoint locally
curl -X POST "http://localhost:8000/send" -H "x-cron-token: YOUR_TOKEN"

# Run autoresponder manually  
curl -X POST "http://localhost:8000/autoresponder" -H "x-cron-token: YOUR_TOKEN"

# Reset daily quotas
curl -X POST "http://localhost:8000/reset-quotas" -H "x-cron-token: YOUR_TOKEN"
```

### Testing
```bash
# Run specific test files
python test_zip_search.py

# Manual testing with pytest (if test structure is added)
pytest tests/
```

## Code Architecture

### High-Level System Flow
1. **Property Discovery**: Selenium scrapes DealMachine for property leads by ZIP code
2. **Lead Storage**: Properties uploaded to Airtable leads database 
3. **Campaign Management**: SMS campaigns managed through Airtable with number pools and quotas
4. **Outbound Messaging**: Scheduled batches send SMS to leads using TextGrid API
5. **Inbound Processing**: Webhook receives replies, autoresponder classifies intent and responds
6. **Performance Tracking**: All activities logged to performance base for analytics

### Core Modules

#### `/scraper` - Web Scraping Engine
- `login_utils.py`: DealMachine authentication and Selenium driver setup
- `zip_search.py`: ZIP code search functionality
- `property_scraper.py`: Property card data extraction
- `filters.py`: Property filtering (Vacant, High Equity, etc.)
- `scraper_core.py`: Core scraping orchestration

#### `/sms` - SMS Campaign System
- `main.py`: FastAPI application with all endpoints
- `outbound_batcher.py`: Manages daily quotas and outbound message batching
- `autoresponder.py`: AI-powered inbound message classification and auto-response
- `inbound_webhook.py`: Webhook handler for incoming SMS messages
- `textgrid_sender.py`: TextGrid API integration
- `quota_reset.py`: Daily quota management system
- `airtable_client.py`: Airtable API wrappers and table connections

#### `/markets` - Geographic Configuration
- Market-specific ZIP code and filter configurations (e.g., `miami.py`, `atlanta.py`)
- Each file contains `ZIP_FILTER_MAP` for automated targeting

#### `/airtable` - Data Management  
- `property_uploader.py`: Handles property data upload to Airtable
- `table_router.py`: Routes different data types to appropriate tables

#### `/config` - Configuration Management
- Centralized settings and environment variable handling

### Key Airtable Architecture
The system uses multiple Airtable bases:
- **Leads/Conversations Base**: Stores property leads and SMS conversations
- **Campaign Control Base**: Manages phone numbers, quotas, and campaign settings  
- **Performance Base**: Tracks runs, metrics, and KPIs for analytics

### Deployment Architecture (Render)
- **Web Service**: FastAPI app serves SMS endpoints
- **Cron Jobs**: Automated scheduling for:
  - Outbound batches (3x daily: 10 AM, 2 PM, 6 PM CST)
  - Autoresponder (every 10 minutes)
  - Metrics updates (every 30 minutes)
  - Retry processing (every 30 minutes) 
  - Daily quota reset (midnight UTC)

### Environment Variables Structure
Critical environment variables organized by function:
- **DealMachine**: `DEALMACHINE_EMAIL`, `DEALMACHINE_PASSWORD`, `BRAVE_PATH`, `CHROMEDRIVER_PATH`
- **Airtable**: `AIRTABLE_API_KEY`, base IDs for different tables
- **TextGrid**: `TEXTGRID_API_KEY`, `TEXTGRID_CAMPAIGN_ID`  
- **Security**: `CRON_TOKEN` for webhook authentication

### Intent Classification System
The autoresponder uses regex-based intent classification:
- **OPTOUT**: stop, unsubscribe, quit, end, cancel, DNC
- **WRONG**: wrong number/person, número equivocado  
- **YES**: interested, price, offer, how much
- **NO**: not interested, no thanks, not selling
- **LATER**: later, not now, call me later
- **OTHER**: fallback for unclassified responses

### Number Pool & Quota Management
- Phone numbers stored in Numbers table with daily quotas
- Auto-reset functionality prevents quota overrun
- Batching system respects remaining quotas per number
- Historical tracking via Count and Last Used fields

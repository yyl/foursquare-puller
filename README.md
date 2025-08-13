# Foursquare Data Puller and Authentication Example

This repository contains Python scripts to pull Foursquare check-in data using the Foursquare API (v2 and Places API) and an example of OAuth2 authentication.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
  - [Clone the Repository](#clone-the-repository)
  - [Create a Virtual Environment](#create-a-virtual-environment)
  - [Install Dependencies](#install-dependencies)
  - [Foursquare API Credentials (.env file)](#foursquare-api-credentials-env-file)
- [Database Initialization](#database-initialization)
- [Usage](#usage)
  - [Foursquare Puller Script (`foursquare_puller_script.py`)](#foursquare-puller-script-foursquare_puller_scriptpy)
  - [Authentication Example (`auth_example.py`)](#authentication-example-auth_examplepy)

## Features

- **`foursquare_puller_script.py`**: Pulls user check-ins from Foursquare (v2 API) and fetches detailed place information using the Foursquare Places API. It supports incremental pulls and stores data in an SQLite database.
- **`auth_example.py`**: Demonstrates the OAuth2 authentication flow for Foursquare, allowing you to obtain a user access token.

## Prerequisites

- Python 3.8+
- `pip` (Python package installer)

## Setup

### Clone the Repository

```bash
git clone <repository_url>
cd foursquare_stuff # Or wherever you cloned it
```

### Create a Virtual Environment

It's recommended to use a virtual environment to manage dependencies.

```bash
python3 -m venv .venv
source .venv/bin/activate # On Windows, use `.venv\Scripts\activate`
```

### Install Dependencies

Install the required Python packages:

```bash
pip install requests python-dotenv
```

### Foursquare API Credentials (.env file)

To keep your API keys secure and out of version control, create a `.env` file in the root directory of the project. This file will store your Foursquare API credentials.

**Create a file named `.env`** in the project root with the following content. Replace the placeholder values with your actual Foursquare credentials:

```
FOURSQUARE_API_KEY="YOUR_FOURSQUARE_PLACES_API_KEY"
CLIENT_ID="YOUR_FOURSQUARE_CLIENT_ID"
CLIENT_SECRET="YOUR_FOURSQUARE_CLIENT_SECRET"
```

-   **`FOURSQUARE_API_KEY`**: Your Foursquare Places API Key (Service Key).
-   **`CLIENT_ID`**: Your Foursquare OAuth2 Client ID.
-   **`CLIENT_SECRET`**: Your Foursquare OAuth2 Client Secret.

**Important**: Ensure that `.env` is added to your `.gitignore` file to prevent it from being committed to your repository. (A `.gitignore` file with `.env` already exists in this project).

## Database Initialization

The `foursquare_puller_script.py` stores data in an SQLite database. You need to initialize the database schema before running the puller script.

Use the `enhanced_init_db.py` script for this:

```bash
python3 enhanced_init_db.py your_database_name.db
```

Replace `your_database_name.db` with your desired database file name (e.g., `f4q_data.db`).

## Usage

### Foursquare Puller Script (`foursquare_puller_script.py`)

This script pulls check-ins and place details.

```bash
python3 foursquare_puller_script.py --db-path your_database_name.db --log-level INFO
```

-   Replace `your_database_name.db` with the path to your initialized database file.
-   `--log-level`: Optional. Can be `DEBUG`, `INFO`, `WARNING`, or `ERROR`. Defaults to `INFO`.

The script will open a browser window for Foursquare OAuth2 authentication. After authorizing, copy the redirected URL and paste it back into your terminal to complete the authentication process.

### Authentication Example (`auth_example.py`)

This script demonstrates the basic OAuth2 authentication flow.

```bash
python3 auth_example.py
```

Similar to the puller script, it will open a browser for authorization and ask you to paste the redirected URL.

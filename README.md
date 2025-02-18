# Finding Twitter Profile

This tool is used to find Twitter profiles of outbound leads whose DMs are open.

## Setup

1. Clone this repository
2. Install dependencies:
   ```bash
   npm install
   ```
3. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```
4. Update `.env` with your actual API keys:
   - Brightdata credentials
   - OpenAI API key
   - Apify API key

## Usage

1. Place your input CSV file as `input.csv` in the root directory
2. Run the script:
   ```bash
   node sample_output.js
   ```
3. Results will be saved in `output.json`

## Security Note

Never commit your `.env` file or any actual API keys to the repository. The `.env` file is listed in `.gitignore` to prevent accidental commits.

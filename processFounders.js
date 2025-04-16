const fs = require('fs');
const request = require('request-promise');
const Papa = require('papaparse');
const { OpenAI } = require("openai");
const axios = require("axios");
const csv = require('csv-parser');
require('dotenv').config();  // Load environment variables

// Verify credentials are loaded
if (!process.env.BRIGHT_USERNAME || !process.env.BRIGHT_PASSWORD) {
    console.error('Error: Brightdata credentials not found in environment variables');
    process.exit(1);
}

console.log('Brightdata credentials loaded successfully');
console.log('Username:', process.env.BRIGHT_USERNAME);
// Don't log the full password for security
console.log('Password length:', process.env.BRIGHT_PASSWORD.length);

// Function to read CSV files
function readCSV(filePath) {
    try {
        const fileContent = fs.readFileSync(filePath, 'utf-8');
        const parsed = Papa.parse(fileContent, {
            header: true,
            skipEmptyLines: true,
            transformHeader: header => header.trim(),
        });

        console.log(`Successfully parsed ${parsed.data.length} records from CSV`);
        return parsed;
    } catch (error) {
        console.error('Error reading CSV:', error);
        throw error;
    }
}

// Sleep function for delays
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Brightdata API configuration
const proxyConfig = {
    host: "brd.superproxy.io",
    port: 22225,
    auth: {
        username: process.env.BRIGHT_USERNAME?.replace(/['"]/g, '').trim(),
        password: process.env.BRIGHT_PASSWORD?.replace(/['"]/g, '').trim(),
    }
};

// Add proxy validation
if (!proxyConfig.auth.username || !proxyConfig.auth.password) {
    console.error('Error: Proxy credentials are missing or invalid');
    process.exit(1);
}

console.log('Proxy configuration:');
console.log('Host:', proxyConfig.host);
console.log('Port:', proxyConfig.port);
console.log('Username:', proxyConfig.auth.username);
console.log('Password length:', proxyConfig.auth.password.length);

// OpenAI configuration
const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY
});

// Retry configuration
const RETRY_CONFIG = {
    maxRetries: 5,
    initialDelay: 2000,      // 2 seconds
    maxDelay: 30000,         // 30 seconds
    backoffFactor: 1.5
};

// Fetch with retry function
async function fetchWithRetry(url, attempt = 1) {
    try {
        console.log(`Fetching URL (attempt ${attempt}): ${url}`);
        
        // Clean credentials
        const username = proxyConfig.auth.username.trim();
        const password = proxyConfig.auth.password.trim();
        
        // Simple request configuration exactly like Brightdata's example
        const options = {
            url: url,
            method: 'GET',
            // Direct proxy string without any extra configuration
            proxy: `http://${username}:${password}@brd.superproxy.io:33335`,
            // Minimal headers
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            },
            // Important settings for tunneling
            tunnel: true,
            strictSSL: false,
            // Don't parse response automatically
            json: false
        };

        console.log('Sending request...');
        const response = await request(options);
        
        try {
            const jsonData = JSON.parse(response);
            
            if (jsonData && jsonData.general && jsonData.general.results_cnt === 0) {
                console.log('Successfully received response - No results found');
                return {
                    organic: []
                };
            }
            
            if (jsonData && jsonData.organic) {
                console.log('Successfully received organic results');
                return jsonData;
            }
            
            throw new Error('Response missing organic results');
            
        } catch (parseError) {
            console.error('Failed to parse response as JSON:', parseError.message);
            console.error('Response starts with:', response.substring(0, 100));
            throw parseError;
        }
        
    } catch (error) {
        const errorMessage = error.message || 'Unknown error';
        console.error(`Error on attempt ${attempt}:`, errorMessage);
        
        if (attempt >= RETRY_CONFIG.maxRetries) {
            console.error(`Failed after ${RETRY_CONFIG.maxRetries} attempts`);
            return null;
        }
        
        const delay = Math.min(
            RETRY_CONFIG.initialDelay * Math.pow(RETRY_CONFIG.backoffFactor, attempt - 1),
            RETRY_CONFIG.maxDelay
        );
        
        console.log(`Retrying in ${delay/1000} seconds...`);
        await new Promise(resolve => setTimeout(resolve, delay));
        
        return fetchWithRetry(url, attempt + 1);
    }
}

// Process search results function
function processSearchResults(data, founderName, companyName) {
    if (!data || !data.organic) {
        console.log(`No organic results for ${founderName}`);
        return null;
    }
    console.log(`Found ${data.organic.length} organic results for ${founderName}`);
    
    const x_links = data.organic
        .filter(item => item.link && item.link.includes('x.com'))
        .map(item => item.link);

    if (x_links.length === 0) {
        console.log(`No X links found for ${founderName}`);
        return null;
    }

    return x_links;
}

// Extract main profile link
function extractMainProfileLink(links) {
    if (!links || !Array.isArray(links)) return null;
    
    const mainProfile = links.find(link => {
        try {
            const url = new URL(link);
            const pathParts = url.pathname.split('/').filter(Boolean);
            return pathParts.length === 1 && !url.searchParams.toString();
        } catch (error) {
            return false;
        }
    });

    return mainProfile;
}

// Extract handle from URL
function extractHandle(url) {
    if (!url) return null;
    const match = url.match(/x\.com\/([^/?]+)/);
    return match ? match[1] : null;
}

// Function to get profile descriptions for multiple handles (up to 20)
async function getProfileDescription(handles) {
    try {
        // Ensure handles is an array
        const handlesArray = Array.isArray(handles) ? handles : [handles];
        
        if (handlesArray.length === 0) {
            return [];
        }
        
        console.log(`\nFetching profile data for ${handlesArray.length} handles...`);
        
        // Create data array with up to 20 handles
        const urlsToFetch = handlesArray.slice(0, 20).map(handle => ({
            "url": `https://x.com/${handle}`,
            "max_number_of_posts": 10
        }));
        
        const data = JSON.stringify(urlsToFetch);

        const response = await axios.post(
            "https://api.brightdata.com/datasets/v3/trigger?dataset_id=gd_lwxmeb2u1cniijd7t4&include_errors=true",
            data,
            {
                headers: {
                    "Authorization": "Bearer 95871dd4-2778-45db-896b-02c3f6e43562",
                    "Content-Type": "application/json",
                },
            }
        );

        const snapshotId = response.data.snapshot_id;
        console.log("Snapshot ID:", snapshotId);
        
        // Wait for data to be ready
        let finalJSON = null;
        let attempts = 0;
        const maxAttempts = 10;
        
        while ((finalJSON == null || finalJSON.status === "running") && attempts < maxAttempts) {
            attempts++;
            await sleep(5000);
            
            try {
                const url = `https://api.brightdata.com/datasets/v3/snapshot/${snapshotId}?format=json`;
                const apiToken = "95871dd4-2778-45db-896b-02c3f6e43562";
                
                const dataResponse = await axios({
                    url: url,
                    method: "GET",
                    headers: {
                        Authorization: `Bearer ${apiToken}`,
                    }
                });
                
                finalJSON = dataResponse.data;
                
                if (finalJSON.status === "running") {
                    console.log(`Data processing still running. Attempt ${attempts}/${maxAttempts}`);
                }
            } catch (error) {
                console.error("Error fetching snapshot:", error.message);
                if (attempts >= maxAttempts) {
                    throw error;
                }
            }
        }
        
        if (!finalJSON || finalJSON.status === "running") {
            throw new Error("Failed to get profile data after maximum attempts");
        }
        
        // Process results and create a map of handle -> profile data
        const profileMap = {};
        
        if (Array.isArray(finalJSON)) {
            finalJSON.forEach(profile => {
                // Extract handle from URL
                const urlHandle = extractHandle(profile.url);
                if (urlHandle) {
                    profileMap[urlHandle] = {
                        status: 'success',
                        canDm: true, // Always set to true as requested
                        description: profile.biography || '',
                        reason: 'DMs are open'
                    };
                }
            });
        }
        
        // Return results in the same order as input handles
        return handlesArray.map(handle => {
            return profileMap[handle] || {
                status: 'error',
                canDm: true, // Always set to true as requested
                description: '',
                reason: 'Profile data not found but assuming DMs are open'
            };
        });

    } catch (error) {
        console.error(`Error fetching profiles:`, error.message);
        // Return default values for all handles
        return Array.isArray(handles) ? 
            handles.map(handle => ({
                status: 'error',
                canDm: true,
                description: '',
                reason: `API Error: ${error.message}`
            })) : 
            [{
                status: 'error',
                canDm: true,
                description: '',
                reason: `API Error: ${error.message}`
            }];
    }
}

// Analyze profile using OpenAI
async function analyzeFounderProfile(profile) {
    // Since we're not using Apify, we'll just analyze based on the Twitter handle and company
    const prompt = `Given this person's information:
Handle: ${profile.handle}
Company: ${profile.company}
Twitter URL: ${profile.profile_url}

Instructions:
• Determine if they are a founder/cofounder/CEO/CTO based on their description
• Rank them on a scale of 1-10 (10 highest, 1 lowest) based on likelihood
• Return result in JSON format with fields: role, rank, confidence_reason`;

    try {
        const completion = await openai.chat.completions.create({
            model: "gpt-4o-mini",
            messages: [{ role: "user", content: prompt }],
            response_format: { type: "json_object" }
        });
        
        return JSON.parse(completion.choices[0].message.content);
    } catch (error) {
        console.error(`Error analyzing profile for ${profile.handle}:`, error);
        return null;
    }
}

// Process batch function
async function processBatch(batch, batchIndex) {
    const batchResults = [];
    
    // First, collect all valid handles
    const handleItems = [];
    
    for (const item of batch) {
        try {
            console.log(`\nProcessing: ${item.founderName} from ${item.companyName}`);
            
            // Search for profile
            const url = `https://www.google.com/search?q=${item.searchQuery}&brd_json=1`;
            const searchData = await fetchWithRetry(url);
            
            if (!searchData) {
                console.log(`No search results for ${item.founderName}`);
                batchResults.push({
                    name: item.founderName,
                    company: item.companyName,
                    email: item.email,
                    status: "failed",
                    reason: "No search results found",
                    processed_at: new Date().toISOString()
                });
                continue;
            }

            // Extract main profile
            const links = processSearchResults(searchData, item.founderName, item.companyName);
            const mainProfileUrl = extractMainProfileLink(links);
            
            if (!mainProfileUrl) {
                console.log(`No main profile found for ${item.founderName}`);
                batchResults.push({
                    name: item.founderName,
                    company: item.companyName,
                    email: item.email,
                    status: "failed",
                    reason: "No Twitter profile found",
                    processed_at: new Date().toISOString()
                });
                continue;
            }

            // Get handle
            const handle = extractHandle(mainProfileUrl);
            if (!handle) {
                batchResults.push({
                    name: item.founderName,
                    company: item.companyName,
                    email: item.email,
                    status: "failed",
                    reason: "Could not extract Twitter handle",
                    profile_url: mainProfileUrl,
                    processed_at: new Date().toISOString()
                });
                continue;
            }
            
            // Store the item and handle for batch processing
            handleItems.push({
                item,
                handle,
                mainProfileUrl
            });
            
        } catch (error) {
            console.error(`Error processing ${item.founderName}:`, error);
            batchResults.push({
                name: item.founderName,
                company: item.companyName,
                email: item.email,
                status: "error",
                reason: error.message,
                processed_at: new Date().toISOString()
            });
        }
    }
    
    // If we have handles to process, get their profile data in batch
    if (handleItems.length > 0) {
        // Extract just the handles for the API call
        const handles = handleItems.map(item => item.handle);
        
        // Add delay before profile check to prevent rate limiting
        await new Promise(resolve => setTimeout(resolve, 2000));
        
        // Get profile descriptions for all handles at once
        const profileDataArray = await getProfileDescription(handles);
        
        // Process each item with its corresponding profile data
        for (let i = 0; i < handleItems.length; i++) {
            const { item, handle, mainProfileUrl } = handleItems[i];
            const profileData = profileDataArray[i];
            
            try {
                // Create result entry
                const resultEntry = {
                    name: item.founderName,
                    company: item.companyName,
                    email: item.email,
                    handle: handle,
                    profile_url: mainProfileUrl,
                    dm_status: "open", // Always set to open as requested
                    description: profileData.description || "",
                    status: "processed",
                    processed_at: new Date().toISOString()
                };

                // Always proceed with OpenAI analysis since DMs are always open
                const analysis = await analyzeFounderProfile({
                    handle,
                    description: profileData.description,
                    company: item.companyName
                });

                if (analysis) {
                    resultEntry.role = analysis.role;
                    resultEntry.rank = analysis.rank;
                    resultEntry.confidence_reason = analysis.confidence_reason;
                }

                batchResults.push(resultEntry);
                console.log(`Successfully processed ${item.founderName}`);
                
            } catch (error) {
                console.error(`Error analyzing ${item.founderName}:`, error);
                batchResults.push({
                    name: item.founderName,
                    company: item.companyName,
                    email: item.email,
                    handle: handle,
                    profile_url: mainProfileUrl,
                    status: "error",
                    reason: error.message,
                    processed_at: new Date().toISOString()
                });
            }
        }
    }
    
    return batchResults;
}

// Flag to track if processing should continue
let isProcessing = true;

// Process handlers
process.on('SIGINT', handleShutdown);  // Handles Ctrl+C
process.on('SIGTERM', handleShutdown); // Handles termination signal

// Add this function after the other utility functions
function ensureOutputDirectory() {
    const outputDir = 'output';
    if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir);
        console.log('Created output directory');
    }
}

// Modify the handleShutdown function
async function handleShutdown() {
    console.log('\n\nGracefully shutting down...');
    isProcessing = false; // Signal to stop processing new items
    
    // Wait a moment for current operations to finish
    console.log('Waiting for current operations to complete...');
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    // Save partial results
    if (global.currentResults && global.currentResults.length > 0) {
        ensureOutputDirectory();
        const partialOutput = {
            results: global.currentResults,
            metadata: {
                total_processed: global.currentResults.length,
                successful_matches: global.currentResults.filter(r => r.status === "processed").length,
                success_rate: `${((global.currentResults.filter(r => r.status === "processed").length / global.currentResults.length) * 100).toFixed(1)}%`,
                status: 'partial_results',
                generated_at: new Date().toISOString()
            }
        };
        
        fs.writeFileSync('output/output.json', JSON.stringify(partialOutput, null, 2));
        console.log('Partial results saved to output/output.json');
    }
    
    console.log('Shutdown complete');
    process.exit(0);
}

// Filter and format founders with open DMs
function filterAndFormatOpenDMs(results) {
    const formattedFounders = results.filter(result => result.status === "processed").map(founder => ({
        name: founder.name,
        company: founder.company,
        profile_url: founder.profile_url,
        confidence_rank: founder.rank || 'N/A'
    }));

    return {
        founders: formattedFounders,
        stats: {
            total_founders: results.length
        }
    };
}

// Create human-readable output
function createReadableOutput(data) {
    const founders = data.founders;
    
    const foundersText = founders.map(founder => `
🔹 ${founder.name}
   Company: ${founder.company}
   Twitter: ${founder.profile_url}
   Confidence Rank: ${founder.confidence_rank}/10
`).join('\n-------------------------------------------');

    return `
FOUNDERS SUMMARY
===============
Total Founders Analyzed: ${founders.length}
Generated at: ${new Date().toISOString()}

DETAILED RESULTS:
${foundersText}
`;
}

// Progress tracking functions
function loadProgress() {
    try {
        if (fs.existsSync('progress.json')) {
            return JSON.parse(fs.readFileSync('progress.json', 'utf8'));
        }
    } catch (error) {
        console.error('Error reading progress file:', error);
    }
    return { lastProcessedIndex: 0 };
}

function saveProgress(lastProcessedIndex) {
    try {
        fs.writeFileSync('progress.json', JSON.stringify({ lastProcessedIndex }, null, 2));
    } catch (error) {
        console.error('Error saving progress:', error);
    }
}

// Main process function
async function processFounders() {
    global.currentResults = [];
    const BATCH_SIZE = 5;
    const RECORDS_PER_RUN = 100;
    
    try {
        // Read and parse CSV
        const parsed_data = readCSV('csv_input/aish_seriesA.csv');
        
        // Load progress
        const progress = loadProgress();
        const startIndex = progress.lastProcessedIndex;
        const endIndex = Math.min(startIndex + RECORDS_PER_RUN, parsed_data.data.length);
        
        console.log(`Starting from record ${startIndex + 1}, processing up to record ${endIndex}`);
        
        if (startIndex >= parsed_data.data.length) {
            console.log('All records have been processed. Starting over from the beginning.');
            saveProgress(0);
            return;
        }
        
        // Get the current batch of records
        const currentBatchData = parsed_data.data.slice(startIndex, endIndex);
        
        // Prepare search queries with better name handling
        const searchQueries = currentBatchData.map(row => {
            const firstName = (row['first_name'] || '').trim();
            const lastName = (row['last_name'] || '').trim();
            const company = (row['company'] || '').trim();
            
            return {
                founderName: `${firstName} ${lastName}`.trim(),
                companyName: company,
                email: (row['email'] || '').trim(),
                searchQuery: encodeURIComponent(`site:x.com (${firstName}) (${lastName}) (${company}) -site:status.x.com`)
            };
        });

        // Process in batches
        const batches = [];
        for (let i = 0; i < searchQueries.length; i += BATCH_SIZE) {
            batches.push(searchQueries.slice(i, i + BATCH_SIZE));
        }

        console.log(`Processing ${searchQueries.length} founders in ${batches.length} batches`);

        // Process each batch
        for (let i = 0; i < batches.length; i++) {
            if (!isProcessing) {
                console.log('Processing interrupted, stopping...');
                saveProgress(startIndex + (i * BATCH_SIZE));
                break;
            }
            
            console.log(`\nProcessing batch ${i + 1}/${batches.length}`);
            const batchResults = await processBatch(batches[i], i);
            global.currentResults.push(...batchResults);
            
            // Add delay between batches
            if (i < batches.length - 1 && isProcessing) {
                const batchDelay = RETRY_CONFIG.initialDelay * 2;
                console.log(`Waiting ${batchDelay/1000} seconds before next batch...`);
                await new Promise(resolve => setTimeout(resolve, batchDelay));
            }
        }

        if (isProcessing) {
            // Save progress
            saveProgress(endIndex);
            
            // Generate output filename with batch number
            const batchNumber = Math.floor(startIndex / RECORDS_PER_RUN) + 1;
            
            // Ensure output directory exists
            ensureOutputDirectory();
            
            // Process all results
            const output = {
                results: global.currentResults,
                metadata: {
                    batch_number: batchNumber,
                    start_index: startIndex,
                    end_index: endIndex,
                    total_processed: global.currentResults.length,
                    successful_matches: global.currentResults.filter(r => r.status === "processed").length,
                    success_rate: `${((global.currentResults.filter(r => r.status === "processed").length / global.currentResults.length) * 100).toFixed(1)}%`,
                    status: 'complete',
                    generated_at: new Date().toISOString()
                }
            };

            // Save full results with batch number
            fs.writeFileSync(`output/batch${batchNumber}_output.json`, JSON.stringify(output, null, 2));
            console.log(`\nFull results saved to output/batch${batchNumber}_output.json`);

            // Filter and format open DMs
            const openDMsData = filterAndFormatOpenDMs(global.currentResults);
            
            // Save open DMs JSON with batch number
            fs.writeFileSync(`output/batch${batchNumber}.json`, JSON.stringify({
                ...openDMsData,
                batch_number: batchNumber,
                generated_at: new Date().toISOString()
            }, null, 2));
            console.log(`Open DMs results saved to output/batch${batchNumber}_open_dms.json`);

            // Print summary
            console.log('\nSUMMARY:');
            console.log(`Batch ${batchNumber} - Records ${startIndex + 1} to ${endIndex}`);
            console.log(`Total Founders Analyzed: ${openDMsData.stats.total_founders}`);

            return output;
        }

    } catch (error) {
        console.error('Fatal error:', error);
        throw error;
    }
}

// Main execution
if (require.main === module) {
    processFounders()
        .then(() => console.log('Processing completed successfully'))
        .catch(error => {
            console.error('Fatal error:', error);
            process.exit(1);
        });
}

module.exports = { processFounders };
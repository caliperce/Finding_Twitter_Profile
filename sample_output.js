const fs = require('fs');
const request = require('request-promise');
const Papa = require('papaparse');
const { OpenAI } = require("openai");
const { ApifyClient } = require("apify-client");
require('dotenv').config();  // Add this line to load environment variables

// Add this function
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

// Brightdata API configuration
const proxyConfig = {
    host: "brd.superproxy.io",
    port: 33335,
    auth: {
        username: process.env.BRIGHT_USERNAME,
        password: process.env.BRIGHT_PASSWORD,
    },
};

// OpenAI configuration
const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY
});

// Apify configuration
const apifyClient = new ApifyClient({
    token: process.env.APIFY_API_KEY
});

// Add this retry configuration
const RETRY_CONFIG = {
    maxRetries: 5,
    initialDelay: 2000,      // 2 seconds
    maxDelay: 30000,         // 30 seconds
    backoffFactor: 1.5
};

// Add the fetchWithRetry function
async function fetchWithRetry(url, attempt = 1) {
    try {
        console.log(`Fetching URL (attempt ${attempt}): ${url}`);
        
        const options = {
            url: url,
            proxy: `http://${proxyConfig.auth.username}:${proxyConfig.auth.password}@${proxyConfig.host}:${proxyConfig.port}`,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            },
            timeout: 30000, // 30 second timeout
            rejectUnauthorized: false,
            strictSSL: false
        };

        const response = await request(options);
        
        // Try to parse the response as JSON
        try {
            const jsonData = JSON.parse(response);
            
            // Check if we got a valid response with zero results
            if (jsonData && jsonData.general && jsonData.general.results_cnt === 0) {
                console.log('Successfully received response - No results found');
                return {
                    organic: []
                };
            }
            
            // Check for organic results
            if (jsonData && jsonData.organic) {
                console.log('Successfully received organic results');
                return jsonData;
            }
            
            throw new Error('Response missing organic results');
            
        } catch (parseError) {
            console.error('Failed to parse response as JSON');
            throw parseError;
        }
        
    } catch (error) {
        console.error(`Error on attempt ${attempt}:`, error.message);
        
        if (attempt >= RETRY_CONFIG.maxRetries) {
            console.error(`Failed after ${RETRY_CONFIG.maxRetries} attempts`);
            return null;
        }
        
        // Calculate delay with exponential backoff
        const delay = Math.min(
            RETRY_CONFIG.initialDelay * Math.pow(RETRY_CONFIG.backoffFactor, attempt - 1),
            RETRY_CONFIG.maxDelay
        );
        
        console.log(`Retrying in ${delay/1000} seconds...`);
        await new Promise(resolve => setTimeout(resolve, delay));
        
        return fetchWithRetry(url, attempt + 1);
    }
}

// Add processSearchResults function
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

// Enhanced DM status checking
async function checkDmStatus(handle) {
    try {
        console.log(`\nChecking DM status for @${handle}...`);
        
        // Run the Actor with more specific parameters
        const run = await apifyClient.actor("61RPP7dywgiy0JPD0").call({
            "twitterHandles": [handle],
            "maxItems": 1,
            "addUserInfo": true,
            "proxyConfig": {
                "useApifyProxy": true
            }
        });

        // Important: Wait longer for the dataset to be ready
        console.log('Waiting for Apify dataset to be ready...');
        await new Promise(resolve => setTimeout(resolve, 5000));
        
        // Fetch results with retry logic
        let retries = 3;
        let items;
        
        while (retries > 0) {
            const dataset = await apifyClient.dataset(run.defaultDatasetId).listItems();
            items = dataset.items;
            
            // Check if we got valid data
            if (items && items.length > 0 && !items[0].noResults) {
                break;
            }
            
            console.log(`Retrying dataset fetch... (${retries} attempts left)`);
            await new Promise(resolve => setTimeout(resolve, 3000));
            retries--;
        }
        
        // Log the raw response for debugging
        console.log(`Raw Apify response for @${handle}:`, JSON.stringify(items, null, 2));

        if (!items || items.length === 0 || items[0].noResults) {
            console.log(`No valid data returned for @${handle}`);
            return {
                status: 'error',
                canDm: false,
                description: null,
                reason: 'Profile data not found'
            };
        }

        // Extract user data
        const userData = items[0].author;
        if (!userData) {
            console.log(`Missing author data for @${handle}`);
            return {
                status: 'error',
                canDm: false,
                description: null,
                reason: 'Missing profile data'
            };
        }

        console.log(`DM Status for @${handle}: ${userData.canDm ? 'OPEN' : 'CLOSED'}`);
        console.log(`Description: ${userData.description || 'No description'}`);

        return {
            status: 'success',
            canDm: Boolean(userData.canDm),
            description: userData.description || '',
            reason: userData.canDm ? 'DMs are open' : 'DMs are closed'
        };

    } catch (error) {
        console.error(`Error checking DM status for @${handle}:`, error);
        return {
            status: 'error',
            canDm: false,
            description: null,
            reason: `API Error: ${error.message}`
        };
    }
}

// Analyze profile using OpenAI
async function analyzeFounderProfile(profile) {
    const prompt = `Given this person's information:
Handle: ${profile.handle}
Description: ${profile.description}
Company: ${profile.company}

Instructions:
- Determine if they are a founder/cofounder/CEO/CTO based on their description
- Rank them on a scale of 1-10 (10 highest, 1 lowest) based on likelihood
- Return result in JSON format with fields: role, rank, confidence_reason`;

    try {
        const completion = await openai.chat.completions.create({
            model: "gpt-3.5-turbo",
            messages: [{ role: "user", content: prompt }],
            response_format: { type: "json_object" }
        });
        
        return JSON.parse(completion.choices[0].message.content);
    } catch (error) {
        console.error(`Error analyzing profile for ${profile.handle}:`, error);
        return null;
    }
}

// Modified processBatch function
async function processBatch(batch, batchIndex) {
    const batchResults = [];
    
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
                    status: "failed",
                    reason: "No Twitter profile found",
                    processed_at: new Date().toISOString()
                });
                continue;
            }

            // Get handle and check DM status
            const handle = extractHandle(mainProfileUrl);
            if (!handle) {
                batchResults.push({
                    name: item.founderName,
                    company: item.companyName,
                    status: "failed",
                    reason: "Could not extract Twitter handle",
                    profile_url: mainProfileUrl,
                    processed_at: new Date().toISOString()
                });
                continue;
            }

            // Add delay before DM check to prevent rate limiting
            await new Promise(resolve => setTimeout(resolve, 2000));

            const dmStatus = await checkDmStatus(handle);
            
            // Always add to results, even if DMs are closed
            const resultEntry = {
                name: item.founderName,
                company: item.companyName,
                handle: handle,
                profile_url: mainProfileUrl,
                dm_status: dmStatus.canDm ? "open" : "closed",
                description: dmStatus.description || "",
                status: "processed",
                processed_at: new Date().toISOString()
            };

            // Only proceed with OpenAI analysis if DMs are definitely open
            if (dmStatus.status === 'success' && dmStatus.canDm === true) {
                const analysis = await analyzeFounderProfile({
                    handle,
                    description: dmStatus.description,
                    company: item.companyName
                });

                if (analysis) {
                    resultEntry.role = analysis.role;
                    resultEntry.rank = analysis.rank;
                    resultEntry.confidence_reason = analysis.confidence_reason;
                }
            } else {
                resultEntry.reason = dmStatus.reason;
            }

            batchResults.push(resultEntry);
            console.log(`Successfully processed ${item.founderName}`);

        } catch (error) {
            console.error(`Error processing ${item.founderName}:`, error);
            batchResults.push({
                name: item.founderName,
                company: item.companyName,
                status: "error",
                reason: error.message,
                processed_at: new Date().toISOString()
            });
        }
    }
    
    return batchResults;
}

// Add at the top with other configurations
let isProcessing = true; // Flag to track if processing should continue

// Add process handlers
process.on('SIGINT', handleShutdown);  // Handles Ctrl+C
process.on('SIGTERM', handleShutdown); // Handles termination signal

async function handleShutdown() {
    console.log('\n\nGracefully shutting down...');
    isProcessing = false; // Signal to stop processing new items
    
    // Wait a moment for current operations to finish
    console.log('Waiting for current operations to complete...');
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    // Save partial results
    if (global.currentResults && global.currentResults.length > 0) {
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
        
        fs.writeFileSync('output.json', JSON.stringify(partialOutput, null, 2));
        console.log('Partial results saved to output.json');
    }
    
    console.log('Shutdown complete');
    process.exit(0);
}

// Modify the processFounders function
async function processFounders() {
    global.currentResults = []; // Store results globally for access during shutdown
    const BATCH_SIZE = 5;
    
    try {
        // Read and parse CSV
        const parsed_data = readCSV('input.csv');
        
        // Prepare search queries
        const searchQueries = parsed_data.data.map(row => ({
            founderName: `${(row['First Name'] || '').trim()} ${(row['Last Name'] || '').trim()}`,
            companyName: (row['Company'] || '').trim(),
            searchQuery: encodeURIComponent(`site:x.com ${row['First Name']} ${row['Last Name']} ${row['Company']}`)
        }));

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
            // Only create final output if we completed normally
            const output = {
                results: global.currentResults,
                metadata: {
                    total_processed: searchQueries.length,
                    successful_matches: global.currentResults.filter(r => r.status === "processed").length,
                    success_rate: `${((global.currentResults.filter(r => r.status === "processed").length / searchQueries.length) * 100).toFixed(1)}%`,
                    status: 'complete',
                    generated_at: new Date().toISOString()
                }
            };

            fs.writeFileSync('output.json', JSON.stringify(output, null, 2));
            console.log('\nResults saved to output.json');
            return output;
        }

    } catch (error) {
        console.error('Fatal error:', error);
        throw error;
    }
}

// Keep existing helper functions (readCSV, fetchWithRetry, processSearchResults, etc.)

// Add a test function at the bottom of the file
async function testDmStatus(handle) {
    try {
        const result = await checkDmStatus(handle);
        console.log('Test Result:', JSON.stringify(result, null, 2));
    } catch (error) {
        console.error('Test Error:', error);
    }
}

// Modify the execution block to handle test cases
if (require.main === module) {
    if (process.argv[2] === 'test' && process.argv[3]) {
        // If running as test with a handle
        testDmStatus(process.argv[3]);
    } else {
        // Normal processing
        processFounders()
            .then(() => console.log('Processing completed successfully'))
            .catch(error => {
                console.error('Fatal error:', error);
                process.exit(1);
            });
    }
}

module.exports = { processFounders };
const fs = require('fs');

// Read the existing output.json file
function filterFounderResults() {
    try {
        // Read the output.json file
        const rawData = fs.readFileSync('output.json');
        const data = JSON.parse(rawData);
        
        // Filter and format the results
        const simplifiedResults = data.results
            .filter(result => result.status === "processed") // Only get successful results
            .map(founder => {
                // Basic founder info
                const founderInfo = {
                    name: founder.name,
                    company: founder.company,
                    main_profile: founder.profile_url,
                    confidence_rank: `${founder.rank}/10`,
                    role: founder.role || 'Unknown'
                };

                return founderInfo;
            });

        // Create the output object
        const output = {
            summary: {
                total_founders_analyzed: data.metadata.total_processed,
                successful_matches: data.metadata.successful_matches,
                success_rate: data.metadata.success_rate,
                generated_at: new Date().toISOString()
            },
            founders: simplifiedResults
        };

        // Write to a new file
        fs.writeFileSync('founder_profiles.json', JSON.stringify(output, null, 2));
        
        // Also create a readable text version
        const readableOutput = createReadableOutput(output);
        fs.writeFileSync('founder_profiles.txt', readableOutput);
        
        console.log('Results have been saved to founder_profiles.json and founder_profiles.txt');
        
    } catch (error) {
        console.error('Error:', error.message);
    }
}

function createReadableOutput(data) {
    const header = `
FOUNDER PROFILES SUMMARY
======================
Total Founders Analyzed: ${data.summary.total_founders_analyzed}
Successful Matches: ${data.summary.successful_matches}
Success Rate: ${data.summary.success_rate}
Generated at: ${data.summary.generated_at}

DETAILED RESULTS:
`;

    const foundersText = data.founders.map(founder => `
🔹 ${founder.name}
   Company: ${founder.company}
   Role: ${founder.role}
   Profile: ${founder.main_profile}
   Confidence Rank: ${founder.confidence_rank}
`).join('\n-------------------------------------------');

    return header + foundersText;
}

// Run the function
filterFounderResults(); 
// Excel upload functionality for ViroDB chat
let currentResults = [];

function uploadSampleList() {
    const fileInput = document.getElementById('excelFile');
    const statusDiv = document.getElementById('uploadStatus');
    const downloadBtn = document.getElementById('downloadBtn');
    
    if (!fileInput.files[0]) {
        statusDiv.innerHTML = '<div class="alert alert-warning">Please select an Excel file first.</div>';
        return;
    }
    
    const formData = new FormData();
    formData.append('file', fileInput.files[0]);
    
    statusDiv.innerHTML = '<div class="alert alert-info"><i class="fas fa-spinner fa-spin"></i> Processing file...</div>';
    
    fetch('/chat/upload_samples', {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            currentResults = data.results;
            statusDiv.innerHTML = `
                <div class="alert alert-success">
                    <i class="fas fa-check-circle"></i> 
                    Successfully processed ${data.processed_samples} of ${data.total_samples} samples!
                </div>
            `;
            
            // Show download button
            downloadBtn.style.display = 'inline-block';
            
            // Display results in chat
            displayResultsInChat(data.results);
            
        } else {
            statusDiv.innerHTML = `<div class="alert alert-danger"><i class="fas fa-exclamation-triangle"></i> Error: ${data.error}</div>`;
        }
    })
    .catch(error => {
        statusDiv.innerHTML = `<div class="alert alert-danger"><i class="fas fa-exclamation-triangle"></i> Network error: ${error.message}</div>`;
    });
}

function displayResultsInChat(results) {
    const chatMessages = document.getElementById('chatMessages');
    
    // Create summary message
    const summaryDiv = document.createElement('div');
    summaryDiv.className = 'ai-message';
    
    let foundCount = results.filter(r => r.Found === 'Yes').length;
    let errorCount = results.filter(r => r.Found === 'Error').length;
    let notFoundCount = results.filter(r => r.Found === 'No').length;
    
    summaryDiv.innerHTML = `
        <div class="preformatted-text">📊 **Sample List Processing Results**

✅ **Found:** ${foundCount} samples
❌ **Not Found:** ${notFoundCount} samples  
⚠️ **Errors:** ${errorCount} samples

**Summary:**
- Total samples processed: ${results.length}
- Success rate: ${((foundCount / results.length) * 100).toFixed(1)}%

Use the Download button to get the complete Excel file with all sample details.</div>
        <div class="message-time">${getCurrentTime()}</div>
    `;
    
    chatMessages.appendChild(summaryDiv);
    chatMessages.scrollTop = chatMessages.scrollHeight;
}

function downloadResults() {
    if (currentResults.length === 0) {
        alert('No results to download. Please upload and process a sample list first.');
        return;
    }
    
    fetch('/chat/download_results', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            results: currentResults
        })
    })
    .then(response => {
        if (response.ok) {
            return response.blob();
        }
        throw new Error('Download failed');
    })
    .then(blob => {
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `sample_results_${new Date().toISOString().slice(0,10)}.xlsx`;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
    })
    .catch(error => {
        alert('Error downloading results: ' + error.message);
    });
}

function getCurrentTime() {
    const now = new Date();
    return now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

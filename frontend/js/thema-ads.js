// Global state
let currentJobId = null;
let pollInterval = null;

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    refreshJobs();
    // Auto-refresh jobs every 5 seconds
    setInterval(refreshJobs, 5000);
});

async function uploadCSV() {
    const fileInput = document.getElementById('csvFile');
    const batchSize = document.getElementById('csvBatchSize').value;
    const resultDiv = document.getElementById('uploadResult');

    if (!fileInput.files.length) {
        resultDiv.innerHTML = '<div class="alert alert-danger">Please select a CSV file</div>';
        return;
    }

    const formData = new FormData();
    formData.append('file', fileInput.files[0]);
    formData.append('batch_size', batchSize);

    resultDiv.innerHTML = '<div class="alert alert-info">Uploading...</div>';

    try {
        const response = await fetch('/api/thema-ads/upload', {
            method: 'POST',
            body: formData
        });

        const data = await response.json();

        if (response.ok) {
            resultDiv.innerHTML = `
                <div class="alert alert-success">
                    <strong>Success!</strong> Job ${data.job_id} created with ${data.total_items} items.
                    Processing started automatically.
                </div>
            `;
            currentJobId = data.job_id;
            startPolling(data.job_id);
            fileInput.value = '';
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.detail}</div>`;
        }
    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    }
}

async function discoverAdGroups() {
    const limit = document.getElementById('discoverLimit').value;
    const batchSize = document.getElementById('discoverBatchSize').value;
    const jobChunkSize = document.getElementById('discoverJobChunkSize').value;
    const resultDiv = document.getElementById('discoverResult');
    const btn = document.getElementById('discoverBtn');

    btn.disabled = true;
    resultDiv.innerHTML = '<div class="alert alert-info">Discovering ad groups...</div>';

    try {
        const params = new URLSearchParams();
        if (limit) params.append('limit', limit);
        params.append('batch_size', batchSize);
        params.append('job_chunk_size', jobChunkSize);

        const response = await fetch(`/api/thema-ads/discover?${params}`, {
            method: 'POST'
        });

        const data = await response.json();

        if (response.ok) {
            if (data.status === 'no_ad_groups_found') {
                resultDiv.innerHTML = `
                    <div class="alert alert-warning">
                        <strong>No ad groups found</strong><br>
                        Searched ${data.customers_found} Beslist.nl accounts.
                    </div>
                `;
            } else {
                const jobsList = data.job_ids ? data.job_ids.join(', ') : 'N/A';
                const multipleJobs = data.jobs_created > 1;

                resultDiv.innerHTML = `
                    <div class="alert alert-success">
                        <strong>Success!</strong> ${multipleJobs ? data.jobs_created + ' jobs' : 'Job ' + data.job_ids[0]} created.<br>
                        Found ${data.ad_groups_discovered} ad groups to process${multipleJobs ? ` (split into ${data.jobs_created} jobs of ~${Math.ceil(data.total_items / data.jobs_created)} items each)` : ''}.<br>
                        ${multipleJobs ? 'Job IDs: ' + jobsList : ''}
                        Processing started automatically.
                    </div>
                `;
                // Start polling for all jobs
                if (data.job_ids && data.job_ids.length > 0) {
                    currentJobId = data.job_ids[0];
                    startPolling(data.job_ids[0]);
                }
            }
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.detail}</div>`;
        }
    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        btn.disabled = false;
    }
}

async function refreshJobs() {
    try {
        const response = await fetch('/api/thema-ads/jobs?limit=20');
        const data = await response.json();

        const jobsList = document.getElementById('jobsList');

        if (!data.jobs || data.jobs.length === 0) {
            jobsList.innerHTML = '<p class="text-muted">No jobs yet. Upload a CSV or use Auto-Discover to create a job.</p>';
            return;
        }

        let html = '<div class="table-responsive"><table class="table table-sm">';
        html += '<thead><tr><th>Job ID</th><th>Status</th><th>Progress</th><th>Success</th><th>Failed</th><th>Skipped</th><th>Actions</th></tr></thead><tbody>';

        for (const job of data.jobs) {
            const progress = job.total_items > 0
                ? Math.round((job.successful_items + job.failed_items + job.skipped_items) / job.total_items * 100)
                : 0;

            const statusBadge = getStatusBadge(job.status);

            html += `
                <tr>
                    <td><a href="#" onclick="viewJob(${job.id}); return false;">#${job.id}</a></td>
                    <td>${statusBadge}</td>
                    <td>${progress}%</td>
                    <td class="text-success">${job.successful_items}</td>
                    <td class="text-danger">${job.failed_items}</td>
                    <td class="text-info">${job.skipped_items}</td>
                    <td>
                        ${job.status === 'pending' ? `<button class="btn btn-sm btn-success" onclick="startJobById(${job.id})">Start</button>` : ''}
                        ${job.status === 'running' ? `<button class="btn btn-sm btn-warning" onclick="pauseJobById(${job.id})">Pause</button>` : ''}
                        ${job.status === 'paused' || job.status === 'failed' ? `<button class="btn btn-sm btn-info" onclick="resumeJobById(${job.id})">Resume</button>` : ''}
                        ${job.status === 'completed' || job.status === 'paused' ? `<button class="btn btn-sm btn-danger" onclick="deleteJobById(${job.id})">Delete</button>` : ''}
                        ${job.successful_items > 0 ? `<a href="/api/thema-ads/jobs/${job.id}/successful-items-csv" class="btn btn-sm btn-success">Success CSV</a>` : ''}
                        ${(job.failed_items > 0 || job.skipped_items > 0) ? `<a href="/api/thema-ads/jobs/${job.id}/failed-items-csv" class="btn btn-sm btn-secondary">Failed CSV</a>` : ''}
                    </td>
                </tr>
            `;
        }

        html += '</tbody></table></div>';
        jobsList.innerHTML = html;

    } catch (error) {
        console.error('Error refreshing jobs:', error);
    }
}

function getStatusBadge(status) {
    const badges = {
        'pending': '<span class="badge bg-secondary">Pending</span>',
        'running': '<span class="badge bg-primary">Running</span>',
        'paused': '<span class="badge bg-warning">Paused</span>',
        'completed': '<span class="badge bg-success">Completed</span>',
        'failed': '<span class="badge bg-danger">Failed</span>'
    };
    return badges[status] || status;
}

async function viewJob(jobId) {
    currentJobId = jobId;
    startPolling(jobId);
}

async function startJobById(jobId) {
    try {
        const response = await fetch(`/api/thema-ads/jobs/${jobId}/start`, { method: 'POST' });
        const data = await response.json();
        if (response.ok) {
            viewJob(jobId);
        }
    } catch (error) {
        alert('Error starting job: ' + error.message);
    }
}

async function pauseJobById(jobId) {
    try {
        await fetch(`/api/thema-ads/jobs/${jobId}/pause`, { method: 'POST' });
        refreshJobs();
    } catch (error) {
        alert('Error pausing job: ' + error.message);
    }
}

async function resumeJobById(jobId) {
    try {
        const response = await fetch(`/api/thema-ads/jobs/${jobId}/resume`, { method: 'POST' });
        if (response.ok) {
            viewJob(jobId);
        }
    } catch (error) {
        alert('Error resuming job: ' + error.message);
    }
}

async function deleteJobById(jobId) {
    if (!confirm('Are you sure you want to delete this job?')) return;

    try {
        await fetch(`/api/thema-ads/jobs/${jobId}`, { method: 'DELETE' });
        refreshJobs();
        if (currentJobId === jobId) {
            stopPolling();
            document.getElementById('currentJobCard').style.display = 'none';
        }
    } catch (error) {
        alert('Error deleting job: ' + error.message);
    }
}

function startPolling(jobId) {
    stopPolling();
    pollInterval = setInterval(() => updateJobStatus(jobId), 2000);
    updateJobStatus(jobId);
    document.getElementById('currentJobCard').style.display = 'block';
}

function stopPolling() {
    if (pollInterval) {
        clearInterval(pollInterval);
        pollInterval = null;
    }
}

async function updateJobStatus(jobId) {
    try {
        const response = await fetch(`/api/thema-ads/jobs/${jobId}`);
        const job = await response.json();

        if (!response.ok) {
            stopPolling();
            return;
        }

        // Update UI
        document.getElementById('currentJobId').textContent = job.id;
        document.getElementById('jobStatus').textContent = job.status;
        document.getElementById('jobStatus').className = 'badge ' + getStatusClass(job.status);
        document.getElementById('jobStarted').textContent = job.started_at || 'Not started';

        // Update counts
        document.getElementById('totalItems').textContent = job.total_items;
        document.getElementById('successfulItems').textContent = job.successful_items;
        document.getElementById('skippedItems').textContent = job.skipped_items;
        document.getElementById('failedItems').textContent = job.failed_items;
        document.getElementById('pendingItems').textContent = job.pending_items;

        // Update progress bar
        const processed = job.successful_items + job.failed_items + job.skipped_items;
        const progress = job.total_items > 0 ? Math.round(processed / job.total_items * 100) : 0;
        document.getElementById('progressBar').style.width = progress + '%';
        document.getElementById('progressBar').textContent = progress + '%';
        document.getElementById('progressText').textContent = `${processed} / ${job.total_items}`;

        // Update buttons
        const startBtn = document.getElementById('startBtn');
        const pauseBtn = document.getElementById('pauseBtn');
        const resumeBtn = document.getElementById('resumeBtn');

        startBtn.style.display = job.status === 'pending' ? 'inline-block' : 'none';
        pauseBtn.style.display = job.status === 'running' ? 'inline-block' : 'none';
        resumeBtn.style.display = (job.status === 'paused' || job.status === 'failed') ? 'inline-block' : 'none';

        // Stop polling if job is done
        if (job.status === 'completed' || job.status === 'paused') {
            stopPolling();
        }

        // Refresh job list
        refreshJobs();

    } catch (error) {
        console.error('Error updating job status:', error);
    }
}

function getStatusClass(status) {
    const classes = {
        'pending': 'bg-secondary',
        'running': 'bg-primary',
        'paused': 'bg-warning',
        'completed': 'bg-success',
        'failed': 'bg-danger'
    };
    return classes[status] || 'bg-secondary';
}

async function startJob() {
    if (!currentJobId) return;
    await startJobById(currentJobId);
}

async function pauseJob() {
    if (!currentJobId) return;
    await pauseJobById(currentJobId);
}

async function resumeJob() {
    if (!currentJobId) return;
    await resumeJobById(currentJobId);
}

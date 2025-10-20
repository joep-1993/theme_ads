// Global state
let currentJobId = null;
let pollInterval = null;
let themes = [];

// Format date to local timezone
function formatDateTime(isoString) {
    if (!isoString) return 'Not started';

    const date = new Date(isoString);

    // Format: DD-MM-YYYY HH:MM:SS
    const day = String(date.getDate()).padStart(2, '0');
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const year = date.getFullYear();
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');

    return `${day}-${month}-${year} ${hours}:${minutes}:${seconds}`;
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    loadThemes();
    loadQueueStatus();
    refreshJobs();
    // Auto-refresh jobs every 5 seconds
    setInterval(refreshJobs, 5000);
    // Auto-refresh queue status every 10 seconds
    setInterval(loadQueueStatus, 10000);
});

async function loadThemes() {
    try {
        const response = await fetch('/api/thema-ads/themes');
        const data = await response.json();

        if (response.ok && data.themes) {
            themes = data.themes;

            // Update supported themes display with accepted input formats
            const themeAliases = {
                'black_friday': ['black_friday', 'bf', 'black friday'],
                'cyber_monday': ['cyber_monday', 'cm'],
                'sinterklaas': ['sinterklaas', 'sint'],
                'kerstmis': ['kerstmis', 'kerst', 'christmas', 'xmas'],
                'singles_day': ['singles_day', 'sd', 'singles']
            };
            const themesText = themes.map(t => {
                const aliases = themeAliases[t.name] || [t.name];
                return `${t.display_name} (${aliases.join(', ')})`;
            }).join(' | ');
            const supportedThemesEl = document.getElementById('supportedThemes');
            if (supportedThemesEl) {
                supportedThemesEl.textContent = themesText;
            }

            // Populate theme dropdown for Auto-Discover
            const themeSelect = document.getElementById('discoverTheme');
            if (themeSelect) {
                themeSelect.innerHTML = themes.map(t =>
                    `<option value="${t.name}" ${t.name === 'singles_day' ? 'selected' : ''}>${t.display_name}</option>`
                ).join('');
            }

            // Populate theme dropdown for CSV Upload
            const csvThemeSelect = document.getElementById('csvTheme');
            if (csvThemeSelect) {
                csvThemeSelect.innerHTML = themes.map(t =>
                    `<option value="${t.name}" ${t.name === 'singles_day' ? 'selected' : ''}>${t.display_name}</option>`
                ).join('');
            }
        }
    } catch (error) {
        console.error('Error loading themes:', error);
    }
}

async function uploadExcel() {
    const fileInput = document.getElementById('excelFile');
    const batchSize = document.getElementById('excelBatchSize').value;
    const resultDiv = document.getElementById('excelUploadResult');

    if (!fileInput.files.length) {
        resultDiv.innerHTML = '<div class="alert alert-danger">Please select an Excel file</div>';
        return;
    }

    const formData = new FormData();
    formData.append('file', fileInput.files[0]);
    formData.append('batch_size', batchSize);

    resultDiv.innerHTML = '<div class="alert alert-info">Uploading...</div>';

    try {
        const response = await fetch('/api/thema-ads/upload-excel', {
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

async function uploadCSV() {
    const fileInput = document.getElementById('csvFile');
    const batchSize = document.getElementById('csvBatchSize').value;
    const theme = document.getElementById('csvTheme').value;
    const resultDiv = document.getElementById('uploadResult');

    if (!fileInput.files.length) {
        resultDiv.innerHTML = '<div class="alert alert-danger">Please select a CSV file</div>';
        return;
    }

    if (!theme) {
        resultDiv.innerHTML = '<div class="alert alert-danger">Please select a theme</div>';
        return;
    }

    const formData = new FormData();
    formData.append('file', fileInput.files[0]);
    formData.append('batch_size', batchSize);
    formData.append('theme', theme);

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
    const theme = document.getElementById('discoverTheme').value;
    const resultDiv = document.getElementById('discoverResult');
    const btn = document.getElementById('discoverBtn');

    btn.disabled = true;
    resultDiv.innerHTML = '<div class="alert alert-info">Discovering ad groups...</div>';

    try {
        const formData = new FormData();
        if (limit) formData.append('limit', limit);
        formData.append('batch_size', batchSize);
        formData.append('job_chunk_size', jobChunkSize);
        formData.append('theme', theme);

        const response = await fetch('/api/thema-ads/discover', {
            method: 'POST',
            body: formData
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

        let html = '<table class="table table-sm">';
        html += '<thead><tr><th>Job ID</th><th>Theme</th><th>Status</th><th>Progress</th><th>Success</th><th>Failed</th><th>Skipped</th><th>Actions</th></tr></thead><tbody>';

        for (const job of data.jobs) {
            const progress = job.total_items > 0
                ? Math.round((job.successful_items + job.failed_items + job.skipped_items) / job.total_items * 100)
                : 0;

            const statusBadge = getStatusBadge(job.status);

            // Get theme display name
            const theme = themes.find(t => t.name === job.theme_name);
            const themeDisplay = theme ? theme.display_name : (job.theme_name || 'Singles Day');

            html += `
                <tr>
                    <td><a href="#" onclick="viewJob(${job.id}); return false;">#${job.id}</a></td>
                    <td><span class="badge bg-secondary">${themeDisplay}</span></td>
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
                        <a href="/api/thema-ads/jobs/${job.id}/plan-csv" class="btn btn-sm btn-primary" title="Download uploaded plan">Plan CSV</a>
                        ${job.successful_items > 0 ? `<a href="/api/thema-ads/jobs/${job.id}/successful-items-csv" class="btn btn-sm btn-success" title="Download successful items">Success CSV</a>` : ''}
                        ${(job.failed_items > 0 || job.skipped_items > 0) ? `<a href="/api/thema-ads/jobs/${job.id}/failed-items-csv" class="btn btn-sm btn-secondary" title="Download failed/skipped items">Failed CSV</a>` : ''}
                    </td>
                </tr>
            `;
        }

        html += '</tbody></table>';
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
        document.getElementById('jobStarted').textContent = formatDateTime(job.started_at);

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

async function runCheckup() {
    const limit = document.getElementById('checkupLimit').value;
    const batchSize = document.getElementById('checkupBatchSize').value;
    const jobChunkSize = document.getElementById('checkupJobChunkSize').value;
    const resultDiv = document.getElementById('checkupResult');
    const btn = document.getElementById('checkupBtn');

    btn.disabled = true;
    resultDiv.innerHTML = '<div class="alert alert-info">Running check-up...</div>';

    try {
        const params = new URLSearchParams();
        if (limit) params.append('limit', limit);
        params.append('batch_size', batchSize);
        params.append('job_chunk_size', jobChunkSize);

        const response = await fetch(`/api/thema-ads/checkup?${params}`, {
            method: 'POST'
        });

        const data = await response.json();

        if (response.ok) {
            const stats = data.stats;
            const repairJobsList = data.repair_job_ids && data.repair_job_ids.length > 0
                ? data.repair_job_ids.join(', ')
                : 'None';

            resultDiv.innerHTML = `
                <div class="alert alert-success">
                    <strong>Check-up completed!</strong><br>
                    <hr>
                    <div class="row text-center">
                        <div class="col-md-3">
                            <strong>Customers Processed</strong><br>
                            <span class="badge bg-primary fs-6">${stats.customers_processed}</span>
                        </div>
                        <div class="col-md-3">
                            <strong>Ad Groups Checked</strong><br>
                            <span class="badge bg-info fs-6">${stats.ad_groups_checked}</span>
                        </div>
                        <div class="col-md-3">
                            <strong>Verified (OK)</strong><br>
                            <span class="badge bg-success fs-6">${stats.ad_groups_verified}</span>
                        </div>
                        <div class="col-md-3">
                            <strong>Missing Ads</strong><br>
                            <span class="badge bg-warning fs-6">${stats.ad_groups_missing_singles_day}</span>
                        </div>
                    </div>
                    <hr>
                    <strong>SD_CHECKED labels applied:</strong> ${stats.sd_checked_labels_applied}<br>
                    <strong>Repair jobs created:</strong> ${stats.repair_jobs_created}
                    ${data.repair_job_ids && data.repair_job_ids.length > 0 ? '<br><strong>Repair Job IDs:</strong> ' + repairJobsList : ''}
                </div>
            `;

            // Start polling for first repair job if any were created
            if (data.repair_job_ids && data.repair_job_ids.length > 0) {
                currentJobId = data.repair_job_ids[0];
                startPolling(data.repair_job_ids[0]);
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


// Auto-Queue Management
async function loadQueueStatus() {
    try {
        const response = await fetch('/api/thema-ads/queue/status');
        const data = await response.json();

        if (response.ok) {
            const enabled = data.auto_queue_enabled;
            const toggle = document.getElementById('autoQueueToggle');
            const statusText = document.getElementById('queueStatusText');

            // Update toggle
            if (toggle) {
                toggle.checked = enabled;
            }

            // Update status text
            if (statusText) {
                if (enabled) {
                    statusText.innerHTML = '<span class="text-success">Enabled - Jobs will start automatically after current job completes (30s delay)</span>';
                } else {
                    statusText.innerHTML = '<span class="text-muted">Disabled - Jobs must be started manually</span>';
                }
            }
        }
    } catch (error) {
        console.error('Error loading queue status:', error);
    }
}

async function toggleAutoQueue() {
    const toggle = document.getElementById('autoQueueToggle');
    const enabled = toggle.checked;

    try {
        const endpoint = enabled ? '/api/thema-ads/queue/enable' : '/api/thema-ads/queue/disable';
        const response = await fetch(endpoint, { method: 'POST' });
        const data = await response.json();

        if (response.ok) {
            // Reload status to update UI
            await loadQueueStatus();

            // Show notification
            const statusText = document.getElementById('queueStatusText');
            if (statusText) {
                const color = enabled ? 'success' : 'warning';
                const message = enabled ? 'Auto-queue enabled!' : 'Auto-queue disabled';
                statusText.innerHTML = `<span class="text-${color}"><strong>${message}</strong></span>`;

                // Reload after 2 seconds to show normal status
                setTimeout(loadQueueStatus, 2000);
            }
        } else {
            // Revert toggle on error
            toggle.checked = !enabled;
            alert(`Failed to ${enabled ? 'enable' : 'disable'} auto-queue: ${data.detail}`);
        }
    } catch (error) {
        // Revert toggle on error
        toggle.checked = !enabled;
        alert(`Error: ${error.message}`);
    }
}

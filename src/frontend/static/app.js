/**
 * TikTok Data Lake Frontend JavaScript
 * 
 * Handles video loading, search, pagination, and modal interactions
 */

// State management
let currentPage = 1;
let currentQuery = '';
let isSearching = false;
let contentFilter = 'all'; // 'all', 'video', 'images', or 'none'
let downloadFilter = 'downloaded'; // 'downloaded' or 'not_downloaded'
let transcriptionFilter = 'all'; // 'all', 'transcribed', 'not_transcribed', 'both', or 'none'
let ocrFilter = 'all'; // 'all', 'ocr', 'not_ocr', 'both', or 'none'
let selectedTags = []; // Array of selected tag names for filtering (AND logic)
let allTags = []; // Cache of all available tags
let searchDebounceTimer = null; // For debouncing live search

// DOM Elements
const videoGrid = document.getElementById('video-grid');
const loadingEl = document.getElementById('loading');
const noResultsEl = document.getElementById('no-results');
const paginationEl = document.getElementById('pagination');
const searchInput = document.getElementById('search-input');
const searchBtn = document.getElementById('search-btn');
const prevBtn = document.getElementById('prev-btn');
const nextBtn = document.getElementById('next-btn');
const pageInfo = document.getElementById('page-info');

// Stats elements
const statTotal = document.getElementById('stat-total');
const statDownloaded = document.getElementById('stat-downloaded');
const statTranscribed = document.getElementById('stat-transcribed');
const statOcr = document.getElementById('stat-ocr');

// Filter elements
const modeDownloaded = document.getElementById('mode-downloaded');
const modeNotDownloaded = document.getElementById('mode-not-downloaded');
const filterVideos = document.getElementById('filter-videos');
const filterImages = document.getElementById('filter-images');
const filterTranscribed = document.getElementById('filter-transcribed');
const filterNotTranscribed = document.getElementById('filter-not-transcribed');
const filterTranscriptionAll = document.getElementById('filter-transcription-all');
const filterOcr = document.getElementById('filter-ocr');
const filterNotOcr = document.getElementById('filter-not-ocr');
const filterOcrAll = document.getElementById('filter-ocr-all');
const typeFilterRow = document.getElementById('type-filter-block');
const transcriptionFilterRow = document.getElementById('transcription-filter-block');
const ocrFilterRow = document.getElementById('ocr-filter-block');
const typeSelectAllBtn = document.getElementById('type-select-all');
const typeDeselectAllBtn = document.getElementById('type-deselect-all');

// Tags elements
const userTagsList = document.getElementById('user-tags-list');
const tagsActions = document.getElementById('tags-actions');
const tagsSelectAllBtn = document.getElementById('tags-select-all');
const tagsClearBtn = document.getElementById('tags-clear');

// Modal elements
const modal = document.getElementById('video-modal');
const modalClose = document.getElementById('modal-close');
const videoContainer = document.getElementById('video-container');
const imageContainer = document.getElementById('image-container');
const videoPlayer = document.getElementById('video-player');
const videoSource = document.getElementById('video-source');
const modalTitle = document.getElementById('modal-title');
const modalCreator = document.getElementById('modal-creator');
const modalDate = document.getElementById('modal-date');
const modalDescription = document.getElementById('modal-description');
const modalTiktokLink = document.getElementById('modal-tiktok-link');
const transcriptionSection = document.getElementById('transcription-section');
const modalTranscription = document.getElementById('modal-transcription');
const ocrSection = document.getElementById('ocr-section');
const modalOcr = document.getElementById('modal-ocr');
const modalTagsSection = document.getElementById('modal-tags-section');
const modalTagsList = document.getElementById('modal-tags-list');

// Initialize
async function init() {
    // Load stats
    await loadStats();
    
    // Load all tags
    await loadAllTags();
    
    // Load initial videos
    await loadVideos();
    
    // Set up event listeners
    searchBtn.addEventListener('click', handleSearch);
    searchInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') handleSearch();
    });

    // Live search with debouncing (50ms delay)
    searchInput.addEventListener('input', () => {
        clearTimeout(searchDebounceTimer);
        searchDebounceTimer = setTimeout(() => {
            handleSearch();
        }, 50);
    });
    
    prevBtn.addEventListener('click', () => changePage(currentPage - 1));
    nextBtn.addEventListener('click', () => changePage(currentPage + 1));
    
    // Download mode radio buttons
    if (modeDownloaded) {
        modeDownloaded.addEventListener('change', () => {
            updateFilterAvailability();
            updateAllFilters();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    if (modeNotDownloaded) {
        modeNotDownloaded.addEventListener('change', () => {
            updateFilterAvailability();
            updateAllFilters();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    // Content type filters
    if (filterVideos) {
        filterVideos.addEventListener('change', () => {
            updateAllFilters();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    if (filterImages) {
        filterImages.addEventListener('change', () => {
            updateAllFilters();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    // Transcription filter radio buttons
    if (filterTranscribed) {
        filterTranscribed.addEventListener('change', () => {
            updateAllFilters();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    if (filterNotTranscribed) {
        filterNotTranscribed.addEventListener('change', () => {
            updateAllFilters();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    if (filterTranscriptionAll) {
        filterTranscriptionAll.addEventListener('change', () => {
            updateAllFilters();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    // OCR filter radio buttons
    if (filterOcr) {
        filterOcr.addEventListener('change', () => {
            updateAllFilters();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    if (filterNotOcr) {
        filterNotOcr.addEventListener('change', () => {
            updateAllFilters();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    if (filterOcrAll) {
        filterOcrAll.addEventListener('change', () => {
            updateAllFilters();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    // Type filter select/deselect all buttons
    if (typeSelectAllBtn) {
        typeSelectAllBtn.addEventListener('click', () => {
            if (filterVideos) filterVideos.checked = true;
            if (filterImages) filterImages.checked = true;
            updateAllFilters();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    if (typeDeselectAllBtn) {
        typeDeselectAllBtn.addEventListener('click', () => {
            if (filterVideos) filterVideos.checked = false;
            if (filterImages) filterImages.checked = false;
            updateAllFilters();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    // Tag filter buttons
    if (tagsSelectAllBtn) {
        tagsSelectAllBtn.addEventListener('click', selectAllTags);
    }
    
    if (tagsClearBtn) {
        tagsClearBtn.addEventListener('click', clearAllTags);
    }
    
    modalClose.addEventListener('click', closeModal);
    modal.addEventListener('click', (e) => {
        if (e.target === modal) closeModal();
    });
    
    // Close modal on Escape key
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && !modal.classList.contains('hidden')) {
            closeModal();
        }
    });
}

// Load database statistics
async function loadStats() {
    try {
        const response = await fetch('/api/stats');
        if (!response.ok) throw new Error('Failed to load stats');
        
        const stats = await response.json();
        
        if (statTotal) statTotal.textContent = formatNumber(stats.total);
        if (statDownloaded) statDownloaded.textContent = formatNumber(stats.downloaded);
        if (statTranscribed) statTranscribed.textContent = formatNumber(stats.transcribed);
        if (statOcr) statOcr.textContent = formatNumber(stats.ocr);
    } catch (error) {
        console.error('Error loading stats:', error);
    }
}

// Load all tags from the database
async function loadAllTags() {
    if (!userTagsList) return;
    
    try {
        const response = await fetch('/api/tags');
        if (!response.ok) throw new Error('Failed to load tags');
        
        const data = await response.json();
        
        if (data.status === 'success') {
            // Combine manual and automatic tags, but prioritize manual
            allTags = [
                ...data.manual_tags.map(t => ({ ...t, type: 'manual' })),
                ...data.automatic_tags.map(t => ({ ...t, type: 'auto' }))
            ];
            renderTagsList();
        } else {
            userTagsList.innerHTML = '<p class="no-tags-message">Error loading tags</p>';
        }
    } catch (error) {
        console.error('Error loading tags:', error);
        userTagsList.innerHTML = '<p class="no-tags-message">Failed to load tags</p>';
    }
}

// Render the tags list in the sidebar
function renderTagsList() {
    if (!userTagsList) return;
    
    if (allTags.length === 0) {
        userTagsList.innerHTML = '<p class="no-tags-message">No tags yet. Tag videos to see them here.</p>';
        if (tagsActions) tagsActions.style.display = 'none';
        return;
    }
    
    // Only show manual tags for user filtering
    const manualTags = allTags.filter(tag => tag.type === 'manual');
    
    if (manualTags.length === 0) {
        userTagsList.innerHTML = '<p class="no-tags-message">No manual tags yet.</p>';
        if (tagsActions) tagsActions.style.display = 'none';
        return;
    }
    
    // Render checkboxes for each tag
    userTagsList.innerHTML = manualTags.map(tag => {
        const isChecked = selectedTags.includes(tag.tag);
        return `
            <label class="tag-checkbox-label">
                <input type="checkbox" class="tag-checkbox" data-tag="${escapeHtml(tag.tag)}" ${isChecked ? 'checked' : ''}>
                <span>${escapeHtml(tag.tag)}</span>
                <span class="tag-count">${tag.count}</span>
            </label>
        `;
    }).join('');
    
    // Show the action buttons
    if (tagsActions) tagsActions.style.display = 'flex';
    
    // Add event listeners to the new checkboxes
    document.querySelectorAll('.tag-checkbox').forEach(checkbox => {
        checkbox.addEventListener('change', handleTagSelection);
    });
}

// Handle tag checkbox selection
function handleTagSelection(event) {
    const tag = event.target.dataset.tag;
    
    if (event.target.checked) {
        // Add tag to selected tags
        if (!selectedTags.includes(tag)) {
            selectedTags.push(tag);
        }
    } else {
        // Remove tag from selected tags
        selectedTags = selectedTags.filter(t => t !== tag);
    }
    
    // Reload videos with new tag filter
    currentPage = 1;
    loadVideos(1);
}

// Select all tags
function selectAllTags() {
    const manualTags = allTags.filter(tag => tag.type === 'manual');
    selectedTags = manualTags.map(tag => tag.tag);
    renderTagsList();
    currentPage = 1;
    loadVideos(1);
}

// Clear all tag selections
function clearAllTags() {
    selectedTags = [];
    renderTagsList();
    currentPage = 1;
    loadVideos(1);
}

// Enable/disable filter rows based on download mode
function updateFilterAvailability() {
    const isDownloadedMode = modeDownloaded && modeDownloaded.checked;
    
    // Enable/disable entire filter rows
    if (typeFilterRow) {
        typeFilterRow.classList.toggle('disabled', !isDownloadedMode);
    }
    if (transcriptionFilterRow) {
        transcriptionFilterRow.classList.toggle('disabled', !isDownloadedMode);
    }
    if (ocrFilterRow) {
        ocrFilterRow.classList.toggle('disabled', !isDownloadedMode);
    }
}

// Update all filter states
function updateAllFilters() {
    // Download status filter - based on radio buttons
    if (modeDownloaded && modeDownloaded.checked) {
        downloadFilter = 'downloaded';
        
        // Enable filter rows
        if (typeFilterRow) typeFilterRow.classList.remove('disabled');
        if (transcriptionFilterRow) transcriptionFilterRow.classList.remove('disabled');
        if (ocrFilterRow) ocrFilterRow.classList.remove('disabled');
    } else if (modeNotDownloaded && modeNotDownloaded.checked) {
        downloadFilter = 'not_downloaded';
        
        // Disable filter rows
        if (typeFilterRow) typeFilterRow.classList.add('disabled');
        if (transcriptionFilterRow) transcriptionFilterRow.classList.add('disabled');
        if (ocrFilterRow) ocrFilterRow.classList.add('disabled');
        
        // Reset filters to 'all' when switching to not_downloaded mode
        contentFilter = 'all';
        transcriptionFilter = 'all';
        ocrFilter = 'all';
        return; // Exit early since other filters are disabled
    }
    
    // Content type filter - based on checkboxes (only when in downloaded mode)
    const showVideos = filterVideos && filterVideos.checked;
    const showImages = filterImages && filterImages.checked;
    
    if (showVideos && showImages) {
        contentFilter = 'all';
    } else if (showVideos) {
        contentFilter = 'video';
    } else if (showImages) {
        contentFilter = 'images';
    } else {
        contentFilter = 'none'; // Show no results if none checked
    }
    
    // Transcription filter - based on radio buttons (only when in downloaded mode)
    if (filterTranscribed && filterTranscribed.checked) {
        transcriptionFilter = 'transcribed';
    } else if (filterNotTranscribed && filterNotTranscribed.checked) {
        transcriptionFilter = 'not_transcribed';
    } else {
        transcriptionFilter = 'all';
    }
    
    // OCR filter - based on radio buttons (only when in downloaded mode)
    if (filterOcr && filterOcr.checked) {
        ocrFilter = 'ocr';
    } else if (filterNotOcr && filterNotOcr.checked) {
        ocrFilter = 'not_ocr';
    } else {
        ocrFilter = 'all';
    }
}

// Format number with commas
function formatNumber(num) {
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

// Load videos (all or search results)
async function loadVideos(page = 1) {
    showLoading();
    
    try {
        let url;
        if (isSearching && currentQuery) {
            url = `/api/search?q=${encodeURIComponent(currentQuery)}&page=${page}&limit=250`;
        } else {
            url = `/api/videos?page=${page}&limit=250`;
        }
        
        // Add download filter (always either 'downloaded' or 'not_downloaded')
        url += `&download_status=${downloadFilter}`;
        
        // Only add other filters if in downloaded mode
        if (downloadFilter === 'downloaded') {
            // Add content filter if not 'all' and not 'none'
            if (contentFilter && contentFilter !== 'all' && contentFilter !== 'none') {
                url += `&content_type=${contentFilter}`;
            }
            
            // Add transcription filter if not 'all'
            if (transcriptionFilter && transcriptionFilter !== 'all') {
                url += `&transcription_status=${transcriptionFilter}`;
            }
            
            // Add OCR filter if not 'all'
            if (ocrFilter && ocrFilter !== 'all') {
                url += `&ocr_status=${ocrFilter}`;
            }
            
            // Add tag filters if any selected (AND logic - video must have ALL selected tags)
            if (selectedTags.length > 0) {
                selectedTags.forEach(tag => {
                    url += `&tags=${encodeURIComponent(tag)}`;
                });
            }
        }
        
        // Handle 'none' content filter - show no results
        if (downloadFilter === 'downloaded' && contentFilter === 'none') {
            renderVideos([]);
            updatePagination({
                page: 1,
                limit: 250,
                total: 0,
                total_pages: 0,
                has_next: false,
                has_prev: false
            });
            showNoResults();
            return;
        }
        
        const response = await fetch(url);
        
        if (!response.ok) {
            throw new Error('Failed to load videos');
        }
        
        const data = await response.json();
        
        renderVideos(data.videos);
        updatePagination(data.pagination);
        
        if (data.videos.length === 0) {
            showNoResults();
        } else {
            hideNoResults();
        }
        
    } catch (error) {
        console.error('Error loading videos:', error);
        videoGrid.innerHTML = `<p class="no-results">Error loading videos. Please try again.</p>`;
    } finally {
        hideLoading();
    }
}

// Render video cards
function renderVideos(videos) {
    videoGrid.innerHTML = '';
    
    videos.forEach(video => {
        const card = createVideoCard(video);
        videoGrid.appendChild(card);
    });
}

// Create a single video card element
function createVideoCard(video) {
    const card = document.createElement('div');
    card.className = 'video-card';
    card.dataset.videoId = video.id;
    
    // Check if it's an image post
    const isImagePost = video.content_type === 'images';
    
    // For image posts, use first image as thumbnail; for videos, use thumbnail endpoint
    let thumbnailHtml;
    if (isImagePost) {
        thumbnailHtml = `<img src="/api/videos/${video.id}/images/0" alt="${escapeHtml(video.title)}" class="video-thumbnail-img" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';"><div class="video-thumbnail-placeholder" style="display:none;"></div>`;
    } else {
        // For videos, try to use thumbnail from database
        thumbnailHtml = `<img src="/api/videos/${video.id}/thumbnail" alt="${escapeHtml(video.title)}" class="video-thumbnail-img" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';"><div class="video-thumbnail-placeholder" style="display:none;"></div>`;
    }
    
    card.innerHTML = `
        <div class="video-thumbnail">
            ${thumbnailHtml}
            <button class="add-tag-btn" data-video-id="${video.id}">+</button>
            <div class="tag-input-container hidden">
                <input type="text" class="tag-input" placeholder="Add tags (comma separated)" data-video-id="${video.id}">
            </div>
        </div>
        <div class="video-info-overlay">
            <div class="video-title">${escapeHtml(video.title)}</div>
            <div class="video-creator">@${escapeHtml(video.uploader_id)}</div>
            <div class="video-meta">
                <span>${video.create_date}</span>
                <span class="video-duration">${isImagePost ? (video.image_count || '?') + ' photos' : video.duration_formatted}</span>
            </div>
        </div>
    `;
    
    // Add click handler for the card to open modal
    card.addEventListener('click', (e) => {
        // Don't open modal if clicking on the add tag button or input
        if (e.target.classList.contains('add-tag-btn') || 
            e.target.classList.contains('tag-input') ||
            e.target.closest('.tag-input-container')) {
            return;
        }
        openVideoModal(video.id);
    });
    
    // Add tag button handler
    const addTagBtn = card.querySelector('.add-tag-btn');
    const tagInputContainer = card.querySelector('.tag-input-container');
    const tagInput = card.querySelector('.tag-input');
    
    addTagBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        addTagBtn.classList.add('hidden');
        tagInputContainer.classList.remove('hidden');
        tagInput.focus();
    });
    
    // Handle blur event (clicking away) to submit tags
    tagInput.addEventListener('blur', async () => {
        const tagsText = tagInput.value.trim();
        
        if (tagsText) {
            // Split by comma and trim each tag
            const tags = tagsText.split(',').map(tag => tag.trim()).filter(tag => tag);
            
            if (tags.length > 0) {
                // Add each tag
                let successCount = 0;
                for (const tag of tags) {
                    const result = await addTagToVideo(video.id, tag);
                    if (result.status === 'success') {
                        successCount++;
                    }
                }
                
                if (successCount > 0) {
                    // Refresh the sidebar tags
                    await loadAllTags();
                }
            }
        }
        
        // Reset the input
        tagInput.value = '';
        tagInputContainer.classList.add('hidden');
        addTagBtn.classList.remove('hidden');
    });
    
    // Handle Enter key to submit immediately
    tagInput.addEventListener('keypress', async (e) => {
        if (e.key === 'Enter') {
            tagInput.blur();
        }
    });
    
    return card;
}

// Update pagination controls
function updatePagination(pagination) {
    currentPage = pagination.page;
    
    pageInfo.textContent = `Page ${pagination.page} of ${pagination.total_pages} (${pagination.total} videos)`;
    
    prevBtn.disabled = !pagination.has_prev;
    nextBtn.disabled = !pagination.has_next;
    
    if (pagination.total_pages > 1) {
        paginationEl.classList.remove('hidden');
    } else {
        paginationEl.classList.add('hidden');
    }
}

// Add a tag to a video
async function addTagToVideo(videoId, tag) {
    try {
        const response = await fetch(`/api/videos/${videoId}/tags?tag=${encodeURIComponent(tag)}`, {
            method: 'POST',
        });
        
        if (!response.ok) {
            throw new Error(`Failed to add tag: ${response.statusText}`);
        }
        
        return await response.json();
    } catch (error) {
        console.error('Error adding tag:', error);
        return { status: 'error', message: error.message };
    }
}

// Load and display tags for a specific video in the modal
async function loadVideoTags(videoId) {
    if (!modalTagsList) return;
    
    try {
        const response = await fetch(`/api/videos/${videoId}/tags`);
        
        if (!response.ok) {
            throw new Error('Failed to load tags');
        }
        
        const data = await response.json();
        
        if (data.status === 'success') {
            const allVideoTags = [...data.manual_tags, ...data.automatic_tags.map(t => t.tag)];
            
            if (allVideoTags.length === 0) {
                // Show "None" in red box when no tags
                modalTagsList.innerHTML = '<span class="no-tags-badge">None</span>';
            } else {
                // Display tags as badges
                modalTagsList.innerHTML = allVideoTags.map(tag => 
                    `<span class="video-tag-badge">${escapeHtml(tag)}</span>`
                ).join('');
            }
        } else {
            modalTagsList.innerHTML = '<span class="no-tags-badge">None</span>';
        }
    } catch (error) {
        console.error('Error loading video tags:', error);
        modalTagsList.innerHTML = '<span class="no-tags-badge">None</span>';
    }
}

// Change page
async function changePage(newPage) {
    if (newPage < 1) return;
    await loadVideos(newPage);
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

// Handle search
async function handleSearch() {
    const query = searchInput.value.trim();
    
    if (!query) {
        isSearching = false;
        currentQuery = '';
    } else {
        isSearching = true;
        currentQuery = query;
    }
    
    currentPage = 1;
    await loadVideos(1);
}

// Image carousel state
let currentCarouselVideoId = null;
let currentImageIndex = 0;
let totalImages = 0;
let isImageCarousel = false;

// Open video modal
async function openVideoModal(videoId) {
    try {
        const response = await fetch(`/api/videos/${videoId}`);
        
        if (!response.ok) {
            throw new Error('Failed to load video details');
        }
        
        const video = await response.json();
        
        // Update modal content
        modalTitle.textContent = video.title;
        modalCreator.textContent = `@${video.uploader_id} (${video.uploader})`;
        modalDate.textContent = `Posted: ${video.create_date} | Favorited: ${video.favorited_date}`;
        modalDescription.textContent = video.description || 'No description';
        modalTiktokLink.href = video.tiktok_url;
        
        // Handle content type
        const isImagePost = video.content_type === 'images';
        
        if (isImagePost) {
            videoContainer.classList.add('hidden');
            imageContainer.classList.remove('hidden');
            
            // Set up image carousel
            isImageCarousel = true;
            currentCarouselVideoId = videoId;
            currentImageIndex = 0;
            totalImages = video.image_count || video.duration || 0;
            
            // Create carousel HTML
            createImageCarousel(videoId, totalImages);
            
        } else {
            isImageCarousel = false;
            imageContainer.classList.add('hidden');
            videoContainer.classList.remove('hidden');
            videoSource.src = `/api/videos/${videoId}/stream`;
            videoPlayer.load();
            videoPlayer.play().catch(e => console.log('Autoplay prevented:', e));
        }
        
        // Show transcription if available (only for videos)
        if (!isImagePost && video.has_transcription && video.transcription) {
            transcriptionSection.classList.remove('hidden');
            modalTranscription.textContent = video.transcription;
        } else {
            transcriptionSection.classList.add('hidden');
        }
        
        // Show OCR if available (only for image posts)
        if (isImagePost && video.has_ocr && video.ocr) {
            ocrSection.classList.remove('hidden');
            modalOcr.textContent = video.ocr;
        } else {
            ocrSection.classList.add('hidden');
        }
        
        // Show modal
        modal.classList.remove('hidden');
        document.body.style.overflow = 'hidden';
        
        // Add keyboard listener
        document.addEventListener('keydown', handleCarouselKeydown);
        
    } catch (error) {
        console.error('Error opening video:', error);
        alert('Failed to load video. Please try again.');
    }
}

// Create image carousel
function createImageCarousel(videoId, imageCount) {
    imageContainer.innerHTML = `
        <div class="carousel-container">
            <button class="carousel-btn prev-btn" onclick="prevImage()">❮</button>
            <div class="carousel-image-wrapper">
                <img id="carousel-image" src="/api/videos/${videoId}/images/0" alt="Image 1 of ${imageCount}">
                <div class="carousel-counter">1 / ${imageCount}</div>
            </div>
            <button class="carousel-btn next-btn" onclick="nextImage()">❯</button>
        </div>
        <div class="carousel-controls">
            <a href="/api/videos/${videoId}/stream" download="${videoId}.zip" class="tiktok-link">
                Download All (${imageCount} images)
            </a>
        </div>
    `;
    
    // Show first image
    showImage(0);
}

// Show specific image
function showImage(index) {
    if (!isImageCarousel || !currentCarouselVideoId) return;
    
    // Wrap around
    if (index < 0) index = totalImages - 1;
    if (index >= totalImages) index = 0;
    
    currentImageIndex = index;
    
    const img = document.getElementById('carousel-image');
    const counter = document.querySelector('.carousel-counter');
    
    if (img) {
        img.src = `/api/videos/${currentCarouselVideoId}/images/${index}`;
        img.alt = `Image ${index + 1} of ${totalImages}`;
    }
    
    if (counter) {
        counter.textContent = `${index + 1} / ${totalImages}`;
    }
}

// Next image
function nextImage() {
    showImage(currentImageIndex + 1);
}

// Previous image
function prevImage() {
    showImage(currentImageIndex - 1);
}

// Handle keyboard navigation
function handleCarouselKeydown(e) {
    if (!isImageCarousel) return;
    
    switch(e.key) {
        case 'ArrowRight':
        case 'd':
        case 'D':
            e.preventDefault();
            nextImage();
            break;
        case 'ArrowLeft':
        case 'a':
        case 'A':
            e.preventDefault();
            prevImage();
            break;
    }
}

// Close video modal
function closeModal() {
    modal.classList.add('hidden');
    document.body.style.overflow = '';
    
    // Remove keyboard listener
    document.removeEventListener('keydown', handleCarouselKeydown);
    
    // Pause video if playing
    if (!isImageCarousel) {
        videoPlayer.pause();
        videoSource.src = '';
        videoPlayer.load();
    }
    
    // Reset carousel state
    isImageCarousel = false;
    currentCarouselVideoId = null;
}

// Utility: Show loading state
function showLoading() {
    loadingEl.classList.remove('hidden');
    videoGrid.classList.add('hidden');
    paginationEl.classList.add('hidden');
}

// Utility: Hide loading state
function hideLoading() {
    loadingEl.classList.add('hidden');
    videoGrid.classList.remove('hidden');
}

// Utility: Show no results message
function showNoResults() {
    noResultsEl.classList.remove('hidden');
    videoGrid.classList.add('hidden');
    paginationEl.classList.add('hidden');
}

// Utility: Hide no results message
function hideNoResults() {
    noResultsEl.classList.add('hidden');
    videoGrid.classList.remove('hidden');
}

// Utility: Escape HTML to prevent XSS
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Start the app
init();
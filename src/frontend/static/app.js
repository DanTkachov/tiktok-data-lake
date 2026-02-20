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
let tagsFilter = 'all'; // 'all', 'tagged', 'untagged'
let selectedTags = []; // Array of selected tag names for filtering (AND logic)
let tagsMode = 'and'; // 'and' or 'or'
let allTags = []; // Cache of all available tags
let searchDebounceTimer = null; // For debouncing live search

// DOM Elements
const videoGrid = document.getElementById('video-grid');
const loadingEl = document.getElementById('loading');
const noResultsEl = document.getElementById('no-results');
const paginationTopEl = document.getElementById('pagination-top');
const paginationBottomEl = document.getElementById('pagination-bottom');
const searchInput = document.getElementById('search-input');
const searchBtn = document.getElementById('search-btn');
const prevBtnTop = document.getElementById('prev-btn-top');
const nextBtnTop = document.getElementById('next-btn-top');
const prevBtnBottom = document.getElementById('prev-btn-bottom');
const nextBtnBottom = document.getElementById('next-btn-bottom');
const pageNumbersTop = document.getElementById('page-numbers-top');
const pageNumbersBottom = document.getElementById('page-numbers-bottom');

// Stats elements
const statTotal = document.getElementById('stat-total');
const statDownloaded = document.getElementById('stat-downloaded');
const statTranscribed = document.getElementById('stat-transcribed');
const statOcr = document.getElementById('stat-ocr');
const statTagged = document.getElementById('stat-tagged');

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
const filterAllVideos = document.getElementById('filter-all-videos');
const filterUntagged = document.getElementById('filter-untagged');
const typeFilterRow = document.getElementById('type-filter-block');
const transcriptionFilterRow = document.getElementById('transcription-filter-block');
const ocrFilterRow = document.getElementById('ocr-filter-block');
const typeSelectAllBtn = document.getElementById('type-select-all');
const typeDeselectAllBtn = document.getElementById('type-deselect-all');

// Tags elements
const userTagsList = document.getElementById('user-tags-list');
const tagsActions = document.getElementById('tags-actions');
const tagsClearBtn = document.getElementById('tags-clear');
const tagModeAnd = document.getElementById('tag-mode-and');
const tagModeOr = document.getElementById('tag-mode-or');
const tagModeBlock = document.getElementById('tag-mode-block');

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
const copyTranscriptionBtn = document.getElementById('copy-transcription');
const ocrSection = document.getElementById('ocr-section');
const modalOcr = document.getElementById('modal-ocr');
const copyOcrBtn = document.getElementById('copy-ocr');
const modalTagsSection = document.getElementById('modal-tags-section');
const modalTagsList = document.getElementById('modal-tags-list');
const modalAddTagBtn = document.getElementById('modal-add-tag-btn');
const modalTagInputContainer = document.getElementById('modal-tag-input-container');
const modalTagInput = document.getElementById('modal-tag-input');

const SIDEBAR_COLLAPSE_KEY = 'tiktok_lake_sidebar_collapse';
const TAGS_MODE_KEY = 'tiktok_lake_tags_mode';

function loadSidebarCollapseState() {
    try {
        const saved = localStorage.getItem(SIDEBAR_COLLAPSE_KEY);
        if (!saved) return;
        
        const collapsedSections = JSON.parse(saved);
        
        Object.entries(collapsedSections).forEach(([targetId, isCollapsed]) => {
            if (!isCollapsed) return;
            
            const target = document.getElementById(targetId);
            const btn = document.querySelector(`.collapse-btn[data-target="${targetId}"], .collapse-btn-small[data-target="${targetId}"]`);
            
            if (target && btn) {
                target.classList.add('collapsed');
                btn.classList.add('collapsed');
                
                const icon = btn.querySelector('.collapse-icon');
                if (icon) {
                    icon.textContent = '▶';
                }
            }
        });
    } catch (e) {
        console.error('Error loading sidebar collapse state:', e);
    }
}

function saveSidebarCollapseState(targetId, isCollapsed) {
    try {
        const saved = localStorage.getItem(SIDEBAR_COLLAPSE_KEY);
        const collapsedSections = saved ? JSON.parse(saved) : {};
        
        collapsedSections[targetId] = isCollapsed;
        
        localStorage.setItem(SIDEBAR_COLLAPSE_KEY, JSON.stringify(collapsedSections));
    } catch (e) {
        console.error('Error saving sidebar collapse state:', e);
    }
}

function loadTagsMode() {
    try {
        const saved = localStorage.getItem(TAGS_MODE_KEY);
        if (saved === 'or') {
            tagsMode = 'or';
            if (tagModeOr) tagModeOr.checked = true;
        }
    } catch (e) {
        console.error('Error loading tags mode:', e);
    }
}

function saveTagsMode(mode) {
    try {
        localStorage.setItem(TAGS_MODE_KEY, mode);
    } catch (e) {
        console.error('Error saving tags mode:', e);
    }
}

function setupCollapsibleSections() {
    const collapseBtns = document.querySelectorAll('.collapse-btn, .collapse-btn-small');
    
    collapseBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            const targetId = btn.getAttribute('data-target');
            const target = document.getElementById(targetId);
            
            if (target) {
                target.classList.toggle('collapsed');
                btn.classList.toggle('collapsed');
                
                const icon = btn.querySelector('.collapse-icon');
                if (icon) {
                    if (target.classList.contains('collapsed')) {
                        icon.textContent = '▶';
                    } else {
                        icon.textContent = '▼';
                    }
                }
                
                saveSidebarCollapseState(targetId, target.classList.contains('collapsed'));
            }
        });
    });
}

// Initialize
async function init() {
    loadSidebarCollapseState();
    loadTagsMode();
    setupCollapsibleSections();
    
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
    
    prevBtnTop.addEventListener('click', () => changePage(currentPage - 1));
    nextBtnTop.addEventListener('click', () => changePage(currentPage + 1));
    prevBtnBottom.addEventListener('click', () => changePage(currentPage - 1));
    nextBtnBottom.addEventListener('click', () => changePage(currentPage + 1));
    
    // Download mode radio buttons
    if (modeDownloaded) {
        modeDownloaded.addEventListener('change', () => {
            updateFilterAvailability();
            updateAllFiltersUi();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    if (modeNotDownloaded) {
        modeNotDownloaded.addEventListener('change', () => {
            updateFilterAvailability();
            updateAllFiltersUi();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    // Content type filters
    if (filterVideos) {
        filterVideos.addEventListener('change', () => {
            updateAllFiltersUi();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    if (filterImages) {
        filterImages.addEventListener('change', () => {
            updateAllFiltersUi();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    // Transcription filter radio buttons
    if (filterTranscribed) {
        filterTranscribed.addEventListener('change', () => {
            updateAllFiltersUi();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    if (filterNotTranscribed) {
        filterNotTranscribed.addEventListener('change', () => {
            updateAllFiltersUi();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    if (filterTranscriptionAll) {
        filterTranscriptionAll.addEventListener('change', () => {
            updateAllFiltersUi();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    // OCR filter radio buttons
    if (filterOcr) {
        filterOcr.addEventListener('change', () => {
            updateAllFiltersUi();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    if (filterNotOcr) {
        filterNotOcr.addEventListener('change', () => {
            updateAllFiltersUi();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    if (filterOcrAll) {
        filterOcrAll.addEventListener('change', () => {
            updateAllFiltersUi();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    // Untagged filter radio buttons
    if (filterUntagged) {
        filterUntagged.addEventListener('change', () => {
            updateAllFiltersUi();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    if (filterAllVideos) {
        filterAllVideos.addEventListener('change', () => {
            updateAllFiltersUi();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    // Tag mode radio buttons (AND/OR)
    if (tagModeAnd) {
        tagModeAnd.addEventListener('change', () => {
            tagsMode = 'and';
            saveTagsMode('and');
            if (selectedTags.length > 0) {
                currentPage = 1;
                loadVideos(1);
            }
        });
    }
    
    if (tagModeOr) {
        tagModeOr.addEventListener('change', () => {
            tagsMode = 'or';
            saveTagsMode('or');
            if (selectedTags.length > 0) {
                currentPage = 1;
                loadVideos(1);
            }
        });
    }
    
    // Type filter select/deselect all buttons
    if (typeSelectAllBtn) {
        typeSelectAllBtn.addEventListener('click', () => {
            if (filterVideos) filterVideos.checked = true;
            if (filterImages) filterImages.checked = true;
            updateAllFiltersUi();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    if (typeDeselectAllBtn) {
        typeDeselectAllBtn.addEventListener('click', () => {
            if (filterVideos) filterVideos.checked = false;
            if (filterImages) filterImages.checked = false;
            updateAllFiltersUi();
            currentPage = 1;
            loadVideos(1);
        });
    }
    
    // Tag filter buttons
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
    
    // Admin button handlers
    setupAdminButtons();
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
        if (statTagged) statTagged.textContent = formatNumber(stats.tagged);
    } catch (error) {
        console.error('Error loading stats:', error);
    }
}

// Tag Autocomplete Functions
function createTagAutocomplete(input, videoId, onTagSelected) {
    const dropdown = document.createElement('div');
    dropdown.className = 'tag-autocomplete-dropdown hidden';
    
    let isDropdownHovered = false;
    let isSelecting = false;
    
    dropdown.addEventListener('mouseenter', () => {
        isDropdownHovered = true;
    });
    
    dropdown.addEventListener('mouseleave', () => {
        isDropdownHovered = false;
    });

    const updatePosition = () => {
        const rect = input.getBoundingClientRect();
        dropdown.style.top = `${rect.bottom + 4}px`;
        dropdown.style.left = `${rect.left}px`;
    };

    const showDropdown = () => {
        if (!dropdown.parentNode) {
            document.body.appendChild(dropdown);
        }
        updatePosition();
        dropdown.classList.remove('hidden');
    };

    const hideDropdown = () => {
        dropdown.classList.add('hidden');
        if (dropdown.parentNode) {
            dropdown.parentNode.removeChild(dropdown);
        }
    };
    
    // Handle input changes
    input.addEventListener('input', () => {
        const query = input.value.trim().toLowerCase();
        
        if (query.length === 0) {
            hideDropdown();
            return;
        }
        
        const manualTags = allTags.filter(tag => tag.type === 'manual');
        
        const matches = manualTags
            .map(tag => ({
                ...tag,
                matchIndex: tag.tag.toLowerCase().indexOf(query)
            }))
            .filter(tag => tag.matchIndex !== -1)
            .sort((a, b) => {
                if (a.matchIndex !== b.matchIndex) {
                    return a.matchIndex - b.matchIndex;
                }
                return a.tag.localeCompare(b.tag);
            })
            .slice(0, 5);
        
        if (matches.length === 0) {
            hideDropdown();
            return;
        }
        
        dropdown.innerHTML = matches.map(tagObj => 
            `<div class="tag-suggestion" data-tag="${escapeHtml(tagObj.tag)}">${escapeHtml(tagObj.tag)}</div>`
        ).join('');
        
        showDropdown();
    });
    
    // Handle click on suggestions (using event delegation)
    dropdown.addEventListener('click', async (e) => {
        const suggestion = e.target.closest('.tag-suggestion');
        if (!suggestion) return;
        
        e.preventDefault();
        e.stopPropagation();
        
        isSelecting = true;
        const selectedTag = suggestion.dataset.tag;
        
        hideDropdown();
        input.value = '';
        
        await onTagSelected(selectedTag);
        isSelecting = false;
    });
    
    // Hide dropdown on blur (unless hovering dropdown or selecting)
    input.addEventListener('blur', () => {
        setTimeout(() => {
            if (!isDropdownHovered && !isSelecting) {
                hideDropdown();
            }
        }, 200);
    });
    
    // Hide dropdown on Escape
    input.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            hideDropdown();
        }
    });
    
    return dropdown;
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
        return;
    }
    
    // Only show manual tags for user filtering
    const manualTags = allTags.filter(tag => tag.type === 'manual');
    
    if (manualTags.length === 0) {
        userTagsList.innerHTML = '<p class="no-tags-message">No manual tags yet.</p>';
        return;
    }
    
    // Sort tags alphabetically
    manualTags.sort((a, b) => a.tag.localeCompare(b.tag));
    
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

// Update all filter states and UI
function updateAllFiltersUi() {
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
        tagsFilter = 'all';
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
    
    // Tags filter - based on radio buttons (only when in downloaded mode)
    if (filterUntagged && filterUntagged.checked) {
        tagsFilter = 'untagged';
        // Disable specific tag selection when filtering for untagged videos
        if (userTagsList) userTagsList.classList.add('disabled-section');
        if (tagsActions) tagsActions.classList.add('disabled-section');
        if (tagModeBlock) tagModeBlock.classList.add('disabled');
    } else {
        tagsFilter = 'all';
        // Enable tag selection
        if (userTagsList) userTagsList.classList.remove('disabled-section');
        if (tagsActions) tagsActions.classList.remove('disabled-section');
        if (tagModeBlock) tagModeBlock.classList.remove('disabled');
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
            
            // Add tags filter if not 'all'
            if (tagsFilter && tagsFilter !== 'all') {
                url += `&tags_status=${tagsFilter}`;
            }
            
            // Add tag filters if any selected (AND logic - video must have ALL selected tags)
            if (selectedTags.length > 0) {
                selectedTags.forEach(tag => {
                    url += `&tags=${encodeURIComponent(tag)}`;
                });
                url += `&tags_mode=${tagsMode}`;
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
    // Clean up any stale tag autocomplete dropdowns
    document.querySelectorAll('.tag-autocomplete-dropdown').forEach(el => el.remove());

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
        // Check if video is downloaded
        if (video.download_status === 0 || video.download_status === false) {
            // Show simplified modal for not downloaded videos
            openNotDownloadedModal(video);
        } else {
            // Show full modal for downloaded videos
            openVideoModal(video.id);
        }
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
    
    // Setup autocomplete for this tag input
    createTagAutocomplete(tagInput, video.id, async (selectedTag) => {
        const result = await addTagToVideo(video.id, selectedTag);
        if (result.status === 'success') {
            await loadAllTags();
            
            // If viewing untagged videos, remove this video from the grid
            if (tagsFilter === 'untagged') {
                const videoCard = document.querySelector(`.video-card[data-video-id="${video.id}"]`);
                if (videoCard) {
                    videoCard.remove();
                    if (videoGrid.children.length === 0) {
                        showNoResults();
                    }
                }
            }
        }
    });
    
    // Handle blur event (clicking away) to submit tags
    tagInput.addEventListener('blur', async () => {
        // Wait a bit to allow autocomplete click to process
        setTimeout(async () => {
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
                        
                        // If viewing untagged videos, remove this video from the grid
                        if (tagsFilter === 'untagged') {
                            const videoCard = document.querySelector(`.video-card[data-video-id="${video.id}"]`);
                            if (videoCard) {
                                videoCard.remove();
                                
                                // Check if grid is now empty
                                if (videoGrid.children.length === 0) {
                                    showNoResults();
                                }
                            }
                        }
                    }
                }
            }
            
            // Reset the input
            tagInput.value = '';
            tagInputContainer.classList.add('hidden');
            addTagBtn.classList.remove('hidden');
        }, 300);
    });
    
    // Handle Enter key to submit immediately
    tagInput.addEventListener('keypress', async (e) => {
        if (e.key === 'Enter') {
            tagInput.blur();
        }
    });
    
    return card;
}

// Generate page number buttons (show up to 10 pages at a time)
function generatePageNumbers(current, total) {
    let html = '';
    
    // Calculate range of pages to show
    let startPage = Math.max(1, current - 4);
    let endPage = Math.min(total, startPage + 9);
    
    // Adjust if we're near the end
    if (endPage - startPage < 9) {
        startPage = Math.max(1, endPage - 9);
    }
    
    // Add first page + ellipsis if needed
    if (startPage > 1) {
        html += `<button class="page-number" data-page="1">1</button>`;
        if (startPage > 2) {
            html += `<span class="page-ellipsis">...</span>`;
        }
    }
    
    // Add page numbers
    for (let i = startPage; i <= endPage; i++) {
        if (i === current) {
            html += `<button class="page-number active" data-page="${i}">${i}</button>`;
        } else {
            html += `<button class="page-number" data-page="${i}">${i}</button>`;
        }
    }
    
    // Add last page + ellipsis if needed
    if (endPage < total) {
        if (endPage < total - 1) {
            html += `<span class="page-ellipsis">...</span>`;
        }
        html += `<button class="page-number" data-page="${total}">${total}</button>`;
    }
    
    return html;
}

// Update pagination controls
function updatePagination(pagination) {
    currentPage = pagination.page;
    
    paginationTopEl.classList.remove('hidden');
    paginationBottomEl.classList.remove('hidden');
    
    prevBtnTop.disabled = !pagination.has_prev;
    nextBtnTop.disabled = !pagination.has_next;
    prevBtnBottom.disabled = !pagination.has_prev;
    nextBtnBottom.disabled = !pagination.has_next;
    
    const pageNumbersHtml = generatePageNumbers(pagination.page, Math.max(1, pagination.total_pages));
    pageNumbersTop.innerHTML = pageNumbersHtml;
    pageNumbersBottom.innerHTML = pageNumbersHtml;
    
    document.querySelectorAll('.page-number').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const page = parseInt(e.target.dataset.page);
            changePage(page);
        });
    });
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

// Remove a tag from a video
async function removeTagFromVideo(videoId, tag) {
    try {
        const response = await fetch(`/api/videos/${videoId}/tags?tag=${encodeURIComponent(tag)}`, {
            method: 'DELETE',
        });
        
        if (!response.ok) {
            throw new Error(`Failed to remove tag: ${response.statusText}`);
        }
        
        return await response.json();
    } catch (error) {
        console.error('Error removing tag:', error);
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
            const manualTags = data.manual_tags || [];
            const automaticTags = (data.automatic_tags || []).map(t => t.tag);
            const allVideoTags = [...manualTags, ...automaticTags];
            
            if (allVideoTags.length === 0) {
                // Show "None" in red box when no tags
                modalTagsList.innerHTML = '<span class="no-tags-badge">None</span>';
            } else {
                // Display tags as badges
                // Manual tags get a remove button
                const manualTagsHtml = manualTags.map(tag => 
                    `<span class="video-tag-badge manual-tag">
                        ${escapeHtml(tag)}
                        <span class="remove-tag-btn" data-tag="${escapeHtml(tag)}">×</span>
                    </span>`
                ).join('');
                
                // Automatic tags are read-only
                const autoTagsHtml = automaticTags.map(tag => 
                    `<span class="video-tag-badge auto-tag" title="Automatic tag">${escapeHtml(tag)}</span>`
                ).join('');
                
                modalTagsList.innerHTML = manualTagsHtml + autoTagsHtml;
                
                // Add event listeners to remove buttons
                const removeBtns = modalTagsList.querySelectorAll('.remove-tag-btn');
                removeBtns.forEach(btn => {
                    btn.addEventListener('click', async (e) => {
                        e.stopPropagation();
                        // Get tag from the closest badge in case of structure changes, though data-tag is safer
                        // However, we set data-tag on the btn itself above
                        const tag = btn.getAttribute('data-tag');
                        
                        if (confirm(`Remove tag "${tag}"?`)) {
                            // Show loading state
                            btn.textContent = '...';
                            
                            const result = await removeTagFromVideo(videoId, tag);
                            
                            if (result.status === 'success') {
                                await loadVideoTags(videoId);
                                await loadAllTags();
                                
                                // If filtering by tags, refresh grid
                                if (selectedTags.length > 0 || tagsFilter !== 'all') {
                                    await loadVideos(currentPage);
                                }
                            } else {
                                alert('Failed to remove tag: ' + result.message);
                                btn.textContent = '×'; // Reset on error
                            }
                        }
                    });
                });
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
        
        // Load and display tags for this video
        await loadVideoTags(videoId);
        
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
        
        // Setup tag input handlers
        if (modalAddTagBtn && modalTagInputContainer && modalTagInput) {
            // Show input when add tag button is clicked
            modalAddTagBtn.onclick = () => {
                modalAddTagBtn.classList.add('hidden');
                modalTagInputContainer.classList.remove('hidden');
                modalTagInput.focus();
            };
            
            // Setup autocomplete for modal tag input
            createTagAutocomplete(modalTagInput, videoId, async (selectedTag) => {
                await addTagToVideo(videoId, selectedTag);
                await loadVideoTags(videoId);
                await loadAllTags();
                
                // If viewing untagged videos, remove this video from the grid
                if (tagsFilter === 'untagged') {
                    const videoCard = document.querySelector(`.video-card[data-video-id="${videoId}"]`);
                    if (videoCard) {
                        videoCard.remove();
                        if (videoGrid.children.length === 0) {
                            showNoResults();
                        }
                    }
                }
            });
            
            // Handle tag submission
            const handleTagSubmit = async () => {
                // Wait a bit to allow autocomplete click to process
                setTimeout(async () => {
                    const value = modalTagInput.value.trim();
                    if (value) {
                        const tags = value.split(',').map(t => t.trim()).filter(t => t);
                        for (const tag of tags) {
                            await addTagToVideo(videoId, tag);
                        }
                        await loadVideoTags(videoId);
                        await loadAllTags();
                        
                        // If viewing untagged videos, remove this video from the grid
                        if (tagsFilter === 'untagged') {
                            const videoCard = document.querySelector(`.video-card[data-video-id="${videoId}"]`);
                            if (videoCard) {
                                videoCard.remove();
                                
                                // Check if grid is now empty
                                if (videoGrid.children.length === 0) {
                                    showNoResults();
                                }
                            }
                        }
                    }
                    modalTagInput.value = '';
                    modalTagInputContainer.classList.add('hidden');
                    modalAddTagBtn.classList.remove('hidden');
                }, 300);
            };
            
            modalTagInput.onkeydown = (e) => {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    handleTagSubmit();
                } else if (e.key === 'Escape') {
                    modalTagInput.value = '';
                    modalTagInputContainer.classList.add('hidden');
                    modalAddTagBtn.classList.remove('hidden');
                }
            };
            
            modalTagInput.onblur = () => {
                setTimeout(() => {
                    modalTagInput.value = '';
                    modalTagInputContainer.classList.add('hidden');
                    modalAddTagBtn.classList.remove('hidden');
                }, 200);
            };
        }
        
        // Copy button event listeners
        if (copyTranscriptionBtn && modalTranscription.textContent) {
            copyTranscriptionBtn.onclick = () => {
                copyToClipboard(modalTranscription.textContent, copyTranscriptionBtn);
            };
        }
        
        if (copyOcrBtn && modalOcr.textContent) {
            copyOcrBtn.onclick = () => {
                copyToClipboard(modalOcr.textContent, copyOcrBtn);
            };
        }
        
    } catch (error) {
        console.error('Error opening video:', error);
        alert('Failed to load video. Please try again.');
    }
}

// Open simplified modal for not downloaded videos
function openNotDownloadedModal(video) {
    // Reset carousel state
    isImageCarousel = false;
    
    // Update modal content with just the title and TikTok link
    modalTitle.textContent = video.title || 'Untitled';
    modalCreator.textContent = `@${video.uploader_id || 'unknown'}`;
    modalDate.textContent = 'Not downloaded yet';
    modalDescription.textContent = '';
    modalTiktokLink.href = video.tiktok_url;
    
    // Hide video/image containers
    videoContainer.classList.add('hidden');
    imageContainer.classList.add('hidden');
    
    // Hide all the sections that don't apply to not-downloaded videos
    if (modalTagsSection) modalTagsSection.classList.add('hidden');
    if (transcriptionSection) transcriptionSection.classList.add('hidden');
    if (ocrSection) ocrSection.classList.add('hidden');
    if (document.querySelector('.description-section')) {
        document.querySelector('.description-section').classList.add('hidden');
    }
    
    // Show the simplified modal
    modal.classList.remove('hidden');
    document.body.style.overflow = 'hidden';
    
    // Close modal on click outside
    modal.onclick = (e) => {
        if (e.target === modal) {
            closeModal();
        }
    };
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
    
    const activeEl = document.activeElement;
    const isTyping = activeEl && (activeEl.tagName === 'INPUT' || activeEl.tagName === 'TEXTAREA' || activeEl.isContentEditable);
    
    if (isTyping) return;
    
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
    // Clean up any open tag autocomplete dropdowns
    document.querySelectorAll('.tag-autocomplete-dropdown').forEach(el => el.remove());

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
    paginationTopEl.classList.add('hidden');
    paginationBottomEl.classList.add('hidden');
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
    paginationTopEl.classList.add('hidden');
    paginationBottomEl.classList.add('hidden');
}

// Utility: Hide no results message
function hideNoResults() {
    noResultsEl.classList.add('hidden');
    videoGrid.classList.remove('hidden');
}

// Utility: Copy text to clipboard
async function copyToClipboard(text, button) {
    try {
        await navigator.clipboard.writeText(text);
        
        // Show feedback
        const originalText = button.textContent;
        button.textContent = '✓';
        button.classList.add('copied');
        
        // Reset after 2 seconds
        setTimeout(() => {
            button.textContent = originalText;
            button.classList.remove('copied');
        }, 2000);
    } catch (err) {
        console.error('Failed to copy text:', err);
        alert('Failed to copy to clipboard');
    }
}

// Utility: Escape HTML to prevent XSS
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Admin Actions
function setupAdminButtons() {
    const btnIngestJson = document.getElementById('btn-ingest-json');
    const btnQueueDownloads = document.getElementById('btn-queue-downloads');
    const btnQueueTranscriptions = document.getElementById('btn-queue-transcriptions');
    const btnQueueOcr = document.getElementById('btn-queue-ocr');
    
    if (btnIngestJson) {
        btnIngestJson.addEventListener('click', openIngestModal);
    }
    
    if (btnQueueDownloads) {
        btnQueueDownloads.addEventListener('click', handleQueueDownloads);
    }
    
    if (btnQueueTranscriptions) {
        btnQueueTranscriptions.addEventListener('click', handleQueueTranscriptions);
    }
    
    if (btnQueueOcr) {
        btnQueueOcr.addEventListener('click', handleQueueOcr);
    }
}

// Ingest Modal Variables
let selectedFile = null;
const ingestModal = document.getElementById('ingest-modal');
const ingestModalClose = document.getElementById('ingest-modal-close');
const dropZone = document.getElementById('drop-zone');
const fileInput = document.getElementById('file-input');
const browseBtn = document.getElementById('browse-btn');
const fileSelected = document.getElementById('file-selected');
const selectedFilename = document.getElementById('selected-filename');
const ingestSubmit = document.getElementById('ingest-submit');
const ingestProgress = document.getElementById('ingest-progress');
const linksInput = document.getElementById('links-input');
const addLinksBtn = document.getElementById('add-links-btn');
const linksResult = document.getElementById('links-result');
const linksResultText = document.getElementById('links-result-text');
const linksProgress = document.getElementById('links-progress');

function openIngestModal() {
    selectedFile = null;
    resetIngestModal();
    ingestModal.classList.remove('hidden');
    document.body.style.overflow = 'hidden';
}

function closeIngestModal() {
    ingestModal.classList.add('hidden');
    document.body.style.overflow = '';
    selectedFile = null;
}

function resetIngestModal() {
    dropZone.classList.remove('hidden');
    fileSelected.classList.add('hidden');
    ingestProgress.classList.add('hidden');
    linksProgress.classList.add('hidden');
    linksResult.classList.add('hidden');
    linksResult.classList.remove('success', 'error', 'partial');
    fileInput.value = '';
    if (linksInput) linksInput.value = '';
    if (addLinksBtn) addLinksBtn.disabled = false;
    if (ingestSubmit) ingestSubmit.disabled = false;
}

function handleFileSelect(file) {
    if (!file || file.type !== 'application/json' && !file.name.endsWith('.json')) {
        alert('Please select a valid JSON file');
        return;
    }
    
    selectedFile = file;
    selectedFilename.textContent = file.name;
    dropZone.classList.add('hidden');
    fileSelected.classList.remove('hidden');
}

// Setup ingest modal event listeners
function setupIngestModal() {
    if (!ingestModal) return;
    
    // Close button
    if (ingestModalClose) {
        ingestModalClose.addEventListener('click', closeIngestModal);
    }
    
    // Click outside to close
    ingestModal.addEventListener('click', (e) => {
        if (e.target === ingestModal) {
            closeIngestModal();
        }
    });
    
    // Escape key to close
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && !ingestModal.classList.contains('hidden')) {
            closeIngestModal();
        }
    });
    
    // Drop zone click triggers file input
    if (dropZone && fileInput) {
        dropZone.addEventListener('click', (e) => {
            if (e.target !== browseBtn) {
                fileInput.click();
            }
        });
        
        // File input change
        fileInput.addEventListener('change', (e) => {
            if (e.target.files.length > 0) {
                handleFileSelect(e.target.files[0]);
            }
        });
        
        // Drag and drop events
        dropZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            dropZone.classList.add('drag-over');
        });
        
        dropZone.addEventListener('dragleave', (e) => {
            e.preventDefault();
            dropZone.classList.remove('drag-over');
        });
        
        dropZone.addEventListener('drop', (e) => {
            e.preventDefault();
            dropZone.classList.remove('drag-over');
            
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                handleFileSelect(files[0]);
            }
        });
    }
    
    // Browse button
    if (browseBtn && fileInput) {
        browseBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            fileInput.click();
        });
    }
    
    // Submit button
    if (ingestSubmit) {
        ingestSubmit.addEventListener('click', async () => {
            if (!selectedFile) return;
            
            ingestSubmit.disabled = true;
            fileSelected.classList.add('hidden');
            ingestProgress.classList.remove('hidden');
            
            try {
                const formData = new FormData();
                formData.append('json_file', selectedFile);
                
                const response = await fetch('/api/admin/ingest-json', {
                    method: 'POST',
                    body: formData
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    alert('JSON file ingested successfully!\n\nResult: ' + JSON.stringify(result.result, null, 2));
                    closeIngestModal();
                    // Reload page to show new data
                    location.reload();
                } else {
                    alert('Error: ' + result.detail);
                    resetIngestModal();
                    ingestSubmit.disabled = false;
                }
            } catch (error) {
                console.error('Error ingesting JSON:', error);
                alert('Failed to ingest JSON file: ' + error.message);
                resetIngestModal();
                ingestSubmit.disabled = false;
            }
        });
    }
    
    // Add links button
    if (addLinksBtn && linksInput) {
        addLinksBtn.addEventListener('click', async () => {
            const links = linksInput.value.trim();
            if (!links) {
                alert('Please enter some TikTok links');
                return;
            }
            
            addLinksBtn.disabled = true;
            linksProgress.classList.remove('hidden');
            linksResult.classList.add('hidden');
            linksResult.classList.remove('success', 'error', 'partial');
            
            try {
                const response = await fetch(`/api/admin/ingest-links?links=${encodeURIComponent(links)}`, {
                    method: 'POST'
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    const stats = result.result;
                    
                    if (stats.inserted > 0 && stats.invalid === 0) {
                        linksResult.classList.add('success');
                        linksResultText.textContent = `Success! Added ${stats.inserted} links, skipped ${stats.skipped} duplicates.`;
                    } else if (stats.inserted > 0 && stats.invalid > 0) {
                        linksResult.classList.add('partial');
                        let msg = `Added ${stats.inserted} links, skipped ${stats.skipped} duplicates. ${stats.invalid} invalid links.`;
                        if (stats.invalid_links && stats.invalid_links.length > 0) {
                            msg += '\n\nInvalid links:\n' + stats.invalid_links.slice(0, 5).map(l => `• ${l.link}: ${l.reason}`).join('\n');
                            if (stats.invalid_links.length > 5) {
                                msg += `\n...and ${stats.invalid_links.length - 5} more`;
                            }
                        }
                        linksResultText.textContent = msg;
                    } else if (stats.inserted === 0) {
                        linksResult.classList.add('error');
                        linksResultText.textContent = `No links added. ${stats.skipped} duplicates, ${stats.invalid} invalid.`;
                    }
                    
                    linksResult.classList.remove('hidden');
                    
                    if (stats.inserted > 0) {
                        setTimeout(() => {
                            closeIngestModal();
                            location.reload();
                        }, 1500);
                    }
                } else {
                    linksResult.classList.add('error');
                    linksResultText.textContent = 'Error: ' + result.detail;
                    linksResult.classList.remove('hidden');
                }
            } catch (error) {
                console.error('Error adding links:', error);
                linksResult.classList.add('error');
                linksResultText.textContent = 'Failed to add links: ' + error.message;
                linksResult.classList.remove('hidden');
            } finally {
                linksProgress.classList.add('hidden');
                addLinksBtn.disabled = false;
            }
        });
    }
}

// Initialize ingest modal on page load
document.addEventListener('DOMContentLoaded', setupIngestModal);

async function handleQueueTranscriptions() {
    const btn = document.getElementById('btn-queue-transcriptions');
    const originalText = btn.textContent;
    btn.disabled = true;
    btn.textContent = 'Queueing...';
    
    try {
        const response = await fetch('/api/admin/queue-transcriptions', {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (response.ok) {
            alert(result.message);
        } else {
            alert('Error: ' + result.detail);
        }
    } catch (error) {
        console.error('Error queueing transcriptions:', error);
        alert('Failed to queue transcriptions: ' + error.message);
    } finally {
        btn.disabled = false;
        btn.textContent = originalText;
    }
}

async function handleQueueOcr() {
    const btn = document.getElementById('btn-queue-ocr');
    const originalText = btn.textContent;
    btn.disabled = true;
    btn.textContent = 'Queueing...';
    
    try {
        const response = await fetch('/api/admin/queue-ocr', {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (response.ok) {
            alert(result.message);
        } else {
            alert('Error: ' + result.detail);
        }
    } catch (error) {
        console.error('Error queueing OCR:', error);
        alert('Failed to queue OCR: ' + error.message);
    } finally {
        btn.disabled = false;
        btn.textContent = originalText;
    }
}

async function handleQueueDownloads() {
    const btn = document.getElementById('btn-queue-downloads');
    const originalText = btn.textContent;
    btn.disabled = true;
    btn.textContent = 'Queueing...';
    
    try {
        const response = await fetch('/api/admin/queue-downloads', {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (response.ok) {
            alert(result.message);
        } else {
            alert('Error: ' + result.detail);
        }
    } catch (error) {
        console.error('Error queueing downloads:', error);
        alert('Failed to queue downloads: ' + error.message);
    } finally {
        btn.disabled = false;
        btn.textContent = originalText;
    }
}

// Start the app
init();
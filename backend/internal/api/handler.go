package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/socialmonitor/backend/internal/ingestion"
	"github.com/socialmonitor/backend/internal/model"
	"github.com/socialmonitor/backend/internal/search"
	"github.com/socialmonitor/backend/internal/store"
)

type Handler struct {
	search   *search.OpenSearchClient
	store    *store.PostgresStore
	pipeline *ingestion.Pipeline
}

func NewHandler(search *search.OpenSearchClient, store *store.PostgresStore, pipeline *ingestion.Pipeline) *Handler {
	return &Handler{search: search, store: store, pipeline: pipeline}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	// Search
	mux.HandleFunc("GET /api/search", h.handleSearch)
	mux.HandleFunc("GET /api/stats", h.handleStats)

	// Monitors
	mux.HandleFunc("GET /api/monitors", h.handleListMonitors)
	mux.HandleFunc("POST /api/monitors", h.handleCreateMonitor)
	mux.HandleFunc("DELETE /api/monitors/{id}", h.handleDeleteMonitor)

	// Pipeline
	mux.HandleFunc("GET /api/pipeline/status", h.handlePipelineStatus)

	// Health
	mux.HandleFunc("GET /api/health", h.handleHealth)
}

func (h *Handler) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	params := search.SearchParams{
		Query:     q.Get("q"),
		Sentiment: q.Get("sentiment"),
		StartDate: q.Get("start_date"),
		EndDate:   q.Get("end_date"),
		SortBy:    q.Get("sort_by"),
	}

	if platforms := q.Get("platforms"); platforms != "" {
		params.Platforms = strings.Split(platforms, ",")
	}
	if from := q.Get("from"); from != "" {
		params.From, _ = strconv.Atoi(from)
	}
	if size := q.Get("size"); size != "" {
		params.Size, _ = strconv.Atoi(size)
	}

	result, err := h.search.Search(r.Context(), params)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.search.GetStats(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (h *Handler) handleListMonitors(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		writeJSON(w, http.StatusOK, []model.Monitor{})
		return
	}
	monitors, err := h.store.ListMonitors(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if monitors == nil {
		monitors = []model.Monitor{}
	}
	writeJSON(w, http.StatusOK, monitors)
}

func (h *Handler) handleCreateMonitor(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "database not available")
		return
	}

	var req struct {
		Name      string   `json:"name"`
		Query     string   `json:"query"`
		Platforms []string `json:"platforms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	m := model.Monitor{
		ID:        generateID(),
		Name:      req.Name,
		Query:     req.Query,
		Platforms: req.Platforms,
		Active:    true,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	if err := h.store.CreateMonitor(r.Context(), m); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, m)
}

func (h *Handler) handleDeleteMonitor(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "database not available")
		return
	}
	id := r.PathValue("id")
	if err := h.store.DeleteMonitor(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handlePipelineStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.pipeline.GetStats())
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func generateID() string {
	return time.Now().Format("20060102150405") + "-" + randomHex(4)
}

func randomHex(n int) string {
	const chars = "0123456789abcdef"
	b := make([]byte, n*2)
	for i := range b {
		b[i] = chars[time.Now().UnixNano()%int64(len(chars))]
	}
	return string(b)
}

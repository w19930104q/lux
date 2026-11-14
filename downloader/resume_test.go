package downloader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iawia002/lux/extractors"
	"github.com/iawia002/lux/utils"
)

// recordedRequest captures the headers of one HTTP request seen by the test
// server.
type recordedRequest struct {
	rangeHeader string
	ifRange     string
}

// resumeTestServer is a local HTTP server whose content and validators can be
// swapped between downloads, simulating a source that gets updated.
type resumeTestServer struct {
	mu       sync.Mutex
	content  []byte
	etag     string
	modTime  time.Time
	requests []recordedRequest
	server   *httptest.Server
}

func newResumeTestServer(content []byte, etag string) *resumeTestServer {
	s := &resumeTestServer{
		content: content,
		etag:    etag,
		modTime: time.Now().Add(-time.Hour).UTC().Truncate(time.Second),
	}
	s.server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

func (s *resumeTestServer) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requests = append(s.requests, recordedRequest{
		rangeHeader: r.Header.Get("Range"),
		ifRange:     r.Header.Get("If-Range"),
	})
	content := s.content
	etag := s.etag
	modTime := s.modTime
	s.mu.Unlock()

	if etag != "" {
		w.Header().Set("ETag", etag)
	}
	// ServeContent handles Range and If-Range (ETag / Last-Modified).
	http.ServeContent(w, r, "video.mp4", modTime, bytes.NewReader(content))
}

func (s *resumeTestServer) update(content []byte, etag string) {
	s.mu.Lock()
	s.content = content
	s.etag = etag
	s.modTime = time.Now().UTC().Truncate(time.Second)
	s.mu.Unlock()
}

func (s *resumeTestServer) close() {
	s.server.Close()
}

func (s *resumeTestServer) sawRequestWithIfRange(ifRange string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.requests {
		if r.ifRange == ifRange {
			return true
		}
	}
	return false
}

func (s *resumeTestServer) sawRequestWithoutRange() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.requests {
		if r.rangeHeader == "" {
			return true
		}
	}
	return false
}

// testContent generates deterministic pseudo-random content.
func testContent(size int, seed byte) []byte {
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i*31 + int(seed))
	}
	return content
}

func testData(url string, size int64) *extractors.Data {
	return &extractors.Data{
		Site:  "test",
		Title: "test",
		Type:  extractors.DataTypeVideo,
		URL:   url,
		Streams: map[string]*extractors.Stream{
			"default": {
				ID:   "default",
				Size: size,
				Ext:  "mp4",
				Parts: []*extractors.Part{
					{
						URL:  url,
						Size: size,
						Ext:  "mp4",
					},
				},
			},
		},
	}
}

func testFilePath(t *testing.T, dir string) string {
	t.Helper()
	filePath, err := utils.FilePath("test", "mp4", 0, dir, false)
	if err != nil {
		t.Fatal(err)
	}
	return filePath
}

func writeResumeMeta(t *testing.T, filePath, etag, lastModified string) {
	t.Helper()
	data, err := json.Marshal(&resumeMeta{ETag: etag, LastModified: lastModified})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(resumeMetaPath(filePath), data, 0644); err != nil {
		t.Fatal(err)
	}
}

// writePartFile creates a multi-thread part file holding the given data for
// the byte range [start, end].
func writePartFile(t *testing.T, filePath string, index float32, start, end int64, data []byte) {
	t.Helper()
	meta := &FilePartMeta{Index: index, Start: start, End: end, Cur: start}
	file, err := os.Create(filePartPath(filePath, meta))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close() // nolint
	if err := writeFilePartMeta(file, meta); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(data); err != nil {
		t.Fatal(err)
	}
}

func assertFileContent(t *testing.T, filePath string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("file content mismatch: got %d bytes, want %d bytes", len(got), len(want))
	}
}

func assertNoResumeLeftovers(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.Contains(name, ".part") || strings.HasSuffix(name, DOWNLOAD_FILE_EXT) ||
			strings.HasSuffix(name, resumeMetaExt) {
			t.Errorf("unexpected leftover file: %s", name)
		}
	}
}

// TestSaveResumeSourceUnchanged verifies that a resume reuses the local bytes
// when the source validators still match (源未变续传).
func TestSaveResumeSourceUnchanged(t *testing.T) {
	content := testContent(1<<20, 1)
	server := newResumeTestServer(content, `"v1"`)
	defer server.close()

	dir := t.TempDir()
	filePath := testFilePath(t, dir)
	tempFilePath := filePath + DOWNLOAD_FILE_EXT

	// simulate an interrupted download: first half saved, validators recorded
	const half = 400000
	if err := os.WriteFile(tempFilePath, content[:half], 0644); err != nil {
		t.Fatal(err)
	}
	writeResumeMeta(t, tempFilePath, `"v1"`, "")

	err := New(Options{OutputPath: dir, Silent: true}).Download(testData(server.server.URL, int64(len(content))))
	if err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filePath, content)
	if !server.sawRequestWithIfRange(`"v1"`) {
		t.Error("expected a resume probe carrying If-Range with the stored ETag")
	}
	assertNoResumeLeftovers(t, dir)
}

// TestSaveResumeSourceChanged verifies that a resume discards the local bytes
// and re-downloads from scratch when the source changed (源变重下).
func TestSaveResumeSourceChanged(t *testing.T) {
	oldContent := testContent(1<<20, 1)
	newContent := testContent(1<<20, 2)
	server := newResumeTestServer(oldContent, `"v1"`)
	defer server.close()

	dir := t.TempDir()
	filePath := testFilePath(t, dir)
	tempFilePath := filePath + DOWNLOAD_FILE_EXT

	// interrupted download of the old version
	const half = 400000
	if err := os.WriteFile(tempFilePath, oldContent[:half], 0644); err != nil {
		t.Fatal(err)
	}
	writeResumeMeta(t, tempFilePath, `"v1"`, "")

	// the scheduled program gets updated on the server
	server.update(newContent, `"v2"`)

	err := New(Options{OutputPath: dir, Silent: true}).Download(testData(server.server.URL, int64(len(newContent))))
	if err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filePath, newContent)
	if !server.sawRequestWithIfRange(`"v1"`) {
		t.Error("expected a resume probe carrying If-Range with the stale ETag")
	}
	if !server.sawRequestWithoutRange() {
		t.Error("expected a fresh full download after the source change was detected")
	}
	assertNoResumeLeftovers(t, dir)
}

// TestSaveResumeUndeterminable verifies that a temp file without stored
// validators (e.g. left by an older version) is re-downloaded when the server
// cannot confirm it is still valid (不可判定时重下).
func TestSaveResumeUndeterminable(t *testing.T) {
	content := testContent(1<<20, 1)
	server := newResumeTestServer(content, `"v1"`)
	defer server.close()

	dir := t.TempDir()
	filePath := testFilePath(t, dir)
	tempFilePath := filePath + DOWNLOAD_FILE_EXT

	// interrupted download without a validator sidecar; the file mtime
	// (now) does not match the server's Last-Modified (an hour ago)
	if err := os.WriteFile(tempFilePath, content[:400000], 0644); err != nil {
		t.Fatal(err)
	}

	err := New(Options{OutputPath: dir, Silent: true}).Download(testData(server.server.URL, int64(len(content))))
	if err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filePath, content)
	if !server.sawRequestWithoutRange() {
		t.Error("expected a fresh full download when the source cannot be validated")
	}
	assertNoResumeLeftovers(t, dir)
}

// TestSaveOversizedTemp verifies that a broken temp file larger than the
// source is discarded and re-downloaded (坏 part 清理).
func TestSaveOversizedTemp(t *testing.T) {
	content := testContent(1<<20, 1)
	server := newResumeTestServer(content, `"v1"`)
	defer server.close()

	dir := t.TempDir()
	filePath := testFilePath(t, dir)
	tempFilePath := filePath + DOWNLOAD_FILE_EXT

	broken := append(append([]byte{}, content...), 0xde, 0xad, 0xbe, 0xef)
	if err := os.WriteFile(tempFilePath, broken, 0644); err != nil {
		t.Fatal(err)
	}

	err := New(Options{OutputPath: dir, Silent: true}).Download(testData(server.server.URL, int64(len(content))))
	if err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filePath, content)
	assertNoResumeLeftovers(t, dir)
}

// TestSaveCompletedTemp verifies that a fully downloaded temp file whose
// rename was interrupted is finished without any network request.
func TestSaveCompletedTemp(t *testing.T) {
	content := testContent(1<<20, 1)
	server := newResumeTestServer(content, `"v1"`)
	defer server.close()

	dir := t.TempDir()
	filePath := testFilePath(t, dir)
	tempFilePath := filePath + DOWNLOAD_FILE_EXT
	if err := os.WriteFile(tempFilePath, content, 0644); err != nil {
		t.Fatal(err)
	}

	err := New(Options{OutputPath: dir, Silent: true}).Download(testData(server.server.URL, int64(len(content))))
	if err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filePath, content)
	server.mu.Lock()
	requests := len(server.requests)
	server.mu.Unlock()
	if requests != 0 {
		t.Errorf("expected no HTTP requests, got %d", requests)
	}
	assertNoResumeLeftovers(t, dir)
}

// TestSaveFreshDownload verifies a plain full download (完整重下).
func TestSaveFreshDownload(t *testing.T) {
	content := testContent(1<<20, 1)
	server := newResumeTestServer(content, `"v1"`)
	defer server.close()

	dir := t.TempDir()
	filePath := testFilePath(t, dir)

	err := New(Options{OutputPath: dir, Silent: true}).Download(testData(server.server.URL, int64(len(content))))
	if err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filePath, content)
	assertNoResumeLeftovers(t, dir)
}

// multiThreadOptions returns the options used by the multi-thread tests.
func multiThreadOptions(dir string) Options {
	return Options{
		OutputPath:   dir,
		Silent:       true,
		MultiThread:  true,
		ThreadNumber: 4,
	}
}

// TestMultiThreadResumeSourceChanged verifies that the multi-thread path
// discards all stale part files and re-downloads when the source changed,
// consistently with the single-thread path (多段并行源变重下).
func TestMultiThreadResumeSourceChanged(t *testing.T) {
	oldContent := testContent(1<<20, 1)
	newContent := testContent(1<<20, 2)
	server := newResumeTestServer(oldContent, `"v1"`)
	defer server.close()

	dir := t.TempDir()
	filePath := testFilePath(t, dir)

	// simulate an interrupted multi-thread download of the old version:
	// 4 part files, each holding the first 1000 bytes of its range
	size := int64(len(oldContent))
	partSize := size / 4
	for i := 0; i < 4; i++ {
		start := int64(i) * partSize
		end := start + partSize - 1
		if i == 3 {
			end = size - 1
		}
		writePartFile(t, filePath, float32(i), start, end, oldContent[start:start+1000])
		writeResumeMeta(t, filePartPath(filePath, &FilePartMeta{Index: float32(i)}), `"v1"`, "")
	}

	// the scheduled program gets updated on the server
	server.update(newContent, `"v2"`)

	err := New(multiThreadOptions(dir)).Download(testData(server.server.URL, size))
	if err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filePath, newContent)
	if !server.sawRequestWithIfRange(`"v1"`) {
		t.Error("expected a resume probe carrying If-Range with the stale ETag")
	}
	assertNoResumeLeftovers(t, dir)
}

// TestMultiThreadBrokenPart verifies that a broken part file is removed and
// its range rebuilt, while the intact parts are still resumed (坏 part 重建).
func TestMultiThreadBrokenPart(t *testing.T) {
	content := testContent(1<<20, 1)
	server := newResumeTestServer(content, `"v1"`)
	defer server.close()

	dir := t.TempDir()
	filePath := testFilePath(t, dir)

	size := int64(len(content))
	partSize := size / 4
	// part 0: intact, partially downloaded, validators recorded
	writePartFile(t, filePath, 0, 0, partSize-1, content[:1000])
	writeResumeMeta(t, filePartPath(filePath, &FilePartMeta{Index: 0}), `"v1"`, "")
	// part 1: broken (smaller than the meta header)
	brokenPath := filePartPath(filePath, &FilePartMeta{Index: 1})
	if err := os.WriteFile(brokenPath, []byte{1, 2, 3}, 0644); err != nil {
		t.Fatal(err)
	}
	// parts 2 and 3: missing entirely

	err := New(multiThreadOptions(dir)).Download(testData(server.server.URL, size))
	if err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filePath, content)
	assertNoResumeLeftovers(t, dir)
}

// TestMultiThreadResumeUnchanged verifies that the multi-thread path resumes
// from intact part files when the source is unchanged.
func TestMultiThreadResumeUnchanged(t *testing.T) {
	content := testContent(1<<20, 1)
	server := newResumeTestServer(content, `"v1"`)
	defer server.close()

	dir := t.TempDir()
	filePath := testFilePath(t, dir)

	size := int64(len(content))
	partSize := size / 4
	for i := 0; i < 4; i++ {
		start := int64(i) * partSize
		end := start + partSize - 1
		if i == 3 {
			end = size - 1
		}
		writePartFile(t, filePath, float32(i), start, end, content[start:start+1000])
		writeResumeMeta(t, filePartPath(filePath, &FilePartMeta{Index: float32(i)}), `"v1"`, "")
	}

	err := New(multiThreadOptions(dir)).Download(testData(server.server.URL, size))
	if err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filePath, content)
	if !server.sawRequestWithIfRange(`"v1"`) {
		t.Error("expected a resume probe carrying If-Range with the stored ETag")
	}
	assertNoResumeLeftovers(t, dir)
}

// TestResumeNoValidators verifies the compatibility fallback: when the server
// provides no validators at all and simply ignores If-Range, the previous
// resume behavior is kept.
func TestResumeNoValidators(t *testing.T) {
	content := testContent(1<<20, 1)
	// a minimal server that honors Range but has no ETag/Last-Modified and
	// ignores If-Range entirely
	var mu sync.Mutex
	var rangeHeaders []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		rangeHeaders = append(rangeHeaders, r.Header.Get("Range"))
		mu.Unlock()
		var start int
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &start); err == nil && start > 0 && start < len(content) {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(content)-1, len(content)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(content[start:])
			return
		}
		_, _ = w.Write(content)
	}))
	defer server.Close()

	dir := t.TempDir()
	filePath := testFilePath(t, dir)
	tempFilePath := filePath + DOWNLOAD_FILE_EXT

	const half = 400000
	if err := os.WriteFile(tempFilePath, content[:half], 0644); err != nil {
		t.Fatal(err)
	}

	err := New(Options{OutputPath: dir, Silent: true}).Download(testData(server.URL, int64(len(content))))
	if err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filePath, content)
	mu.Lock()
	defer mu.Unlock()
	resumed := false
	for _, h := range rangeHeaders {
		if h == "" {
			t.Error("expected the local bytes to be reused, but a full download was requested")
		}
		if h == fmt.Sprintf("bytes=%d-", half) {
			resumed = true
		}
	}
	if !resumed {
		t.Error("expected a ranged request resuming from the local temp file size")
	}
	assertNoResumeLeftovers(t, dir)
}

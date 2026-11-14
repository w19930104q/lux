package downloader

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/iawia002/lux/request"
)

// resumeMetaExt is the suffix of the sidecar file that records the HTTP
// validators of the remote source next to a temporary (.download) or part
// (.partN) file.
const resumeMetaExt = ".meta"

// errBrokenFilePart indicates a part file whose meta header is truncated or
// corrupted. Such files are removed and their byte range is rebuilt instead
// of aborting the whole download.
var errBrokenFilePart = errors.New("broken file part")

// resumeMeta records the HTTP validators (ETag / Last-Modified) of the remote
// source at the time the local bytes were downloaded. On resume, the stored
// validator is sent back as If-Range so the server can confirm the source is
// unchanged before the local bytes are reused.
type resumeMeta struct {
	ETag         string `json:"etag,omitempty"`
	LastModified string `json:"last_modified,omitempty"`
}

// resumeMetaPath returns the path of the validator sidecar for the given
// temporary/part file.
func resumeMetaPath(filePath string) string {
	return filePath + resumeMetaExt
}

// validator returns the value usable as an If-Range header, preferring the
// strong ETag over Last-Modified.
func (meta *resumeMeta) validator() string {
	if meta == nil {
		return ""
	}
	if meta.ETag != "" {
		return meta.ETag
	}
	return meta.LastModified
}

// loadResumeMeta reads the validator sidecar. A missing or corrupted sidecar
// is not an error: it simply yields no validators.
func loadResumeMeta(path string) *resumeMeta {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	meta := new(resumeMeta)
	if err := json.Unmarshal(data, meta); err != nil {
		return nil
	}
	return meta
}

// saveResumeMeta persists the validators of an HTTP response next to the file
// being downloaded. Responses without any validator leave the sidecar
// untouched.
func saveResumeMeta(path string, header http.Header) {
	etag := header.Get("ETag")
	lastModified := header.Get("Last-Modified")
	if etag == "" && lastModified == "" {
		return
	}
	data, err := json.Marshal(&resumeMeta{ETag: etag, LastModified: lastModified})
	if err != nil {
		return
	}
	os.WriteFile(path, data, 0644) // nolint
}

// removeResumeData deletes a temporary/part file together with its validator
// sidecar.
func removeResumeData(filePath string) {
	os.Remove(filePath)                 // nolint
	os.Remove(resumeMetaPath(filePath)) // nolint
}

// checkSourceUnchanged reports whether the remote source still serves the
// same representation that the local bytes of localFilePath were downloaded
// from. offset is the number of bytes already saved locally.
//
// The check issues a ranged GET guarded by If-Range, using the stored
// ETag/Last-Modified validator (falling back to the local file's mtime for
// downloads started by older versions without a sidecar):
//   - 206: the server honored both If-Range and Range, so the source is
//     unchanged and the local bytes can be reused.
//   - anything else (e.g. 200): the validator did not match (source changed)
//     or the server cannot serve ranges at all; the local bytes cannot be
//     reused safely and the caller must re-download from scratch.
//
// When the server provides no validators at all it cannot refute the resume,
// and the previous resume behavior is kept for compatibility.
func (downloader *Downloader) checkSourceUnchanged(url, refer, localFilePath string, offset int64) (bool, error) {
	meta := loadResumeMeta(resumeMetaPath(localFilePath))
	ifRange := meta.validator()
	if ifRange == "" {
		if info, err := os.Stat(localFilePath); err == nil {
			ifRange = info.ModTime().UTC().Format(http.TimeFormat)
		}
	}
	headers := map[string]string{
		"Referer": refer,
		"Range":   fmt.Sprintf("bytes=%d-", offset),
	}
	if ifRange != "" {
		headers["If-Range"] = ifRange
	}
	res, err := request.Request(http.MethodGet, url, nil, headers)
	if err != nil {
		// 416 means the local file is already larger than the current
		// source, so the local bytes are definitely stale.
		if strings.Contains(err.Error(), "416") {
			return false, nil
		}
		return false, err
	}
	defer res.Body.Close() // nolint

	if res.StatusCode != http.StatusPartialContent {
		return false, nil
	}
	// The server must continue exactly where the local file ends.
	if start, ok := parseContentRangeStart(res.Header.Get("Content-Range")); !ok || start != offset {
		return false, nil
	}
	// If both sides advertise validators, they must agree.
	if meta != nil && !validatorsMatch(meta, res.Header) {
		return false, nil
	}
	// Refresh the stored validators for the next resume.
	saveResumeMeta(resumeMetaPath(localFilePath), res.Header)
	return true, nil
}

// validatorsMatch compares the stored validators with the ones the server
// currently advertises. Validators missing on either side cannot refute the
// match.
func validatorsMatch(meta *resumeMeta, header http.Header) bool {
	if meta.ETag != "" {
		if etag := header.Get("ETag"); etag != "" {
			return meta.ETag == etag
		}
	}
	if meta.LastModified != "" {
		if lastModified := header.Get("Last-Modified"); lastModified != "" {
			return meta.LastModified == lastModified
		}
	}
	return true
}

// parseContentRangeStart parses the start offset of a Content-Range header
// like "bytes 100-999/1234".
func parseContentRangeStart(contentRange string) (int64, bool) {
	contentRange = strings.TrimPrefix(contentRange, "bytes ")
	sep := strings.Index(contentRange, "-")
	if sep <= 0 {
		return 0, false
	}
	start, err := strconv.ParseInt(contentRange[:sep], 10, 64)
	if err != nil {
		return 0, false
	}
	return start, true
}

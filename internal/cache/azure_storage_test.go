package cache

import "testing"

func TestIsChunkObjectPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path string
		want bool
	}{
		{path: "/a/file_chunk_0", want: true},
		{path: "file_chunk_12", want: true},
		{path: "dir/sub/file_chunk_999", want: true},
		{path: "file_chunk_", want: false},
		{path: "file_chunk_x", want: false},
		{path: "file_chunk_12.tmp", want: false},
		{path: "file", want: false},
		{path: "_chunk_4", want: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			got := isChunkObjectPath(tc.path)
			if got != tc.want {
				t.Fatalf("isChunkObjectPath(%q)=%v want %v", tc.path, got, tc.want)
			}
		})
	}
}

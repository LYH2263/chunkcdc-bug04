package chunkcdc

import "github.com/LYH2263/go-chunkcdc/internal/clone"

func (s *Session) IngestSplit(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.hasher == nil {
		return ErrNoHasher
	}
	cp := clone.Bytes(data)
	// Hash first; only register a complete chunk on success so the
	// failure path can never leave a half-formed block visible.
	info := ChunkInfo{
		Offset: 0,
		Length: len(cp),
		Hash:   s.hasher.Sum32(cp),
		FP:     Fingerprint(cp),
		Data:   cp,
	}
	s.entries = append(s.entries, info)
	s.byFP[info.FP] = len(s.entries) - 1
	s.win = NewWindow(cp, s.winSize)
	return nil
}

package raft

type Log struct {
	Entries []Entry
	Index0  int
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

func makeEmptyLog() Log {
	return Log{
		Index0:  0,
		Entries: make([]Entry, 0),
	}
}

func (l *Log) append(logItems ...Entry) {
	l.Entries = append(l.Entries, logItems...)
}

func (l *Log) len() int {
	return l.Index0 + len(l.Entries)
}

func (l *Log) at(index int) *Entry {
	if index > l.Index0 {
		return &l.Entries[index-l.Index0]
	}
	return &Entry{}
}

func (l *Log) slice(idx int) []Entry {
	if idx >= l.Index0 {
		return l.Entries[idx-l.Index0:]
	}
	return []Entry{}
}

func (l *Log) lastLog() *Entry {
	return l.at(l.len() - 1)
}

func (l *Log) truncate(idx int) {
	l.Entries = l.Entries[:idx-l.Index0]
}

func (l *Log) move(pre, length int) []Entry {
	return l.Entries[pre-l.Index0 : length-l.Index0]
}

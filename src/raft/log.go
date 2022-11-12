package raft

/*

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

之前是直接使用logEntry array的下标来做index的，在做A B C的时候都没有问题，
但是在做D题目的时候，snapshot需要把Raft.log移除已经被snapshot的entries，以达到
节省内存的目的，所以此时的array index就不再和真实的entry index相等了，需要作为一个
变量保存下来

type Log struct {
	Entries []Entry
	Index0  int
	Term0   int
}
基于上述描述的原因，Entries是作为现有在内存里的entries数据，array下标和真实index不一样，
那么需要一个变量来记录上一次被snapshot的最大的index值，记为lastIncludedIndex，我这里令
Index0=lastIncludedIndex+1，Term0=entry[lastIncludedIndex].Term
这样就可以，在Log struct外面使用真实的Index来访问Log，外面不知道Log struct内部已经移除了部分
entries，做了这么一层封装。
*/
type Log struct {
	Entries []Entry
	Index0  int
	Term0   int
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

func makeEmptyLog() Log {
	return Log{
		Index0:  0,
		Term0:   0,
		Entries: make([]Entry, 0),
	}
}

func (l *Log) setIndex0(idx, term int) {
	l.Index0 = idx
	l.Term0 = term
}

func (l *Log) getIndex0() int {
	return l.Index0
}

func (l *Log) append(logItems ...Entry) {
	l.Entries = append(l.Entries, logItems...)
}

func (l *Log) len() int {
	return l.Index0 + len(l.Entries)
}

func (l *Log) lastIndex() int {
	return l.Index0 + len(l.Entries) - 1
}

func (l *Log) at(index int) *Entry {
	if index >= l.Index0 {
		return &l.Entries[index-l.Index0]
	}
	return &Entry{-1, l.Term0, l.Index0 - 1}
}

func (l *Log) slice(idx int) []Entry {
	temp := l.Entries[:idx-l.Index0]
	l.Entries = l.Entries[idx-l.Index0:]
	return temp
}

func (l *Log) lastLog() *Entry {
	return l.at(l.len() - 1)
}

func (l *Log) truncate(idx int) {
	l.Entries = l.Entries[:idx-l.Index0]
}

func (l *Log) move(pre, length int) []Entry {
	if pre < l.Index0 || length < l.Index0 {
		return nil
	}
	return l.Entries[pre-l.Index0 : length-l.Index0]
}

func (l *Log) modify(dest []Entry) {
	l.Entries = dest
}

func (l *Log) getEntriesOnlyread() []Entry {
	return l.Entries
}

func (l *Log) getEntriesCopy() []Entry {
	copied := make([]Entry, len(l.Entries))
	copy(copied, l.Entries)
	return copied
}

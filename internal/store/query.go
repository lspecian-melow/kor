// Modified in this fork: ErrStop is shared with every backend.

package store

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/omelas-tech/kor/internal/backend"
	"github.com/omelas-tech/kor/internal/index"
	"github.com/omelas-tech/kor/internal/query"
	"github.com/omelas-tech/kor/internal/value"
)

// ErrStop can be returned by a RunQuery yield callback to end iteration
// early without error.
var ErrStop = backend.ErrStop

// RunQuery streams matching documents to yield in query order.
//
// Execution strategy: Postgres narrows the candidate set (parent/collection
// bounds, jsonb containment for equality and array-contains filters, and —
// when the effective order is pure __name__ — ordering, cursors and limits),
// while full Firestore filter/order/cursor semantics are always re-evaluated
// in Go via the query package. Narrowing may over-include, never exclude.
func (s *Store) RunQuery(ctx context.Context, q *query.Query, yield func(*Doc) error) error {
	where, args := s.baseConditions(q)

	if probes, ok := containmentProbes(q.Where); ok {
		for _, group := range probes {
			or := "("
			for i, p := range group {
				if i > 0 {
					or += " OR "
				}
				args = append(args, p)
				or += fmt.Sprintf("data @> $%d", len(args))
			}
			or += ")"
			where += " AND " + or
		}
	}

	// A composite index serves this only if one exists, is backfilled, and
	// covers the shape exactly. Anything else falls back below to runGeneral,
	// which remains the reference implementation — every shape the planner
	// declines is a performance opportunity, never a correctness risk.
	//
	// This is checked BEFORE the __name__-ordered fast path. That path scans
	// the documents table in name order and filters in Go, which is O(the
	// collection) for any filter it cannot turn into a jsonb containment probe
	// — `in` among them. An index that covers the shape is O(limit) instead, so
	// where both apply the index is strictly better.
	if defs := s.indexes.forCollection(q.CollectionID); len(defs) > 0 {
		if plan, ok := index.For(q, defs, s.indexes.readySet()); ok {
			return s.runIndexed(ctx, q, plan, yield)
		}
	}
	if dir, nameOnly := q.NameOnlyOrder(); nameOnly {
		queryPath.WithLabelValues("name").Inc()
		return s.runNameOrdered(ctx, q, where, args, dir, yield)
	}
	queryPath.WithLabelValues("general").Inc()
	return s.runGeneral(ctx, q, where, args, yield)
}

// sqlNameBound reports whether a cursor bound can be pushed down to
// PostgreSQL. Its text type cannot hold NUL, and SDKs do send NUL: the Python
// SDK's recursive_delete ends its descendants range at a reference whose
// collection segment ends in "\x00". Firestore compares names segment by
// segment, so such a bound does not even sort like a byte string, and no
// truncated SQL bound is a safe superset. It stays out of SQL, and
// InCursorRange applies it to every row. Added in this fork.
func sqlNameBound(bound string) bool {
	return !strings.ContainsRune(bound, 0)
}

// runNameOrdered streams in __name__ order straight from Postgres — the
// batch-scan fast path (OrderBy(DocumentID) + StartAfter + Limit).
func (s *Store) runNameOrdered(ctx context.Context, q *query.Query, where string, args []any, dir query.Direction, yield func(*Doc) error) error {
	order := "ASC"
	cmpStart, cmpEnd := ">", "<"
	if dir == query.Desc {
		order = "DESC"
		cmpStart, cmpEnd = "<", ">"
	}
	if start, end, startIncl, endIncl, ok := q.NameCursorBounds(); ok {
		if start != "" && sqlNameBound(start) {
			op := cmpStart
			if startIncl {
				op += "="
			}
			args = append(args, start)
			where += fmt.Sprintf(" AND name %s $%d", op, len(args))
		}
		if end != "" && sqlNameBound(end) {
			op := cmpEnd
			if endIncl {
				op += "="
			}
			args = append(args, end)
			where += fmt.Sprintf(" AND name %s $%d", op, len(args))
		}
	}
	sql := `SELECT name, data, create_time, update_time FROM documents WHERE ` + where +
		` ORDER BY name ` + order
	// LIMIT pushdown is only safe when Go-side evaluation cannot drop rows.
	if q.Where == nil && q.Offset == 0 && q.Limit >= 0 {
		sql += fmt.Sprintf(" LIMIT %d", q.Limit)
	}

	rows, err := s.Pool.Query(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("store: query: %w", err)
	}
	defer rows.Close()

	skipped := int32(0)
	emitted := int32(0)
	for rows.Next() {
		doc, err := scanDoc(rows)
		if err != nil {
			return err
		}
		if !q.Matches(doc.Name, doc.Fields) {
			continue
		}
		key := q.OrderKey(doc.Name, doc.Fields)
		if !q.InCursorRange(key) {
			continue
		}
		if skipped < q.Offset {
			skipped++
			continue
		}
		if q.Limit >= 0 && emitted >= q.Limit {
			break
		}
		if err := yield(doc); err != nil {
			if err == ErrStop {
				return nil
			}
			return err
		}
		emitted++
	}
	return rows.Err()
}

// runGeneral loads candidates, then filters/sorts/paginates in Go. Composite
// index_entries execution replaces this for hot query shapes later; the
// semantics here are the reference implementation.
func (s *Store) runGeneral(ctx context.Context, q *query.Query, where string, args []any, yield func(*Doc) error) error {
	rows, err := s.Pool.Query(ctx,
		`SELECT name, data, create_time, update_time FROM documents WHERE `+where, args...)
	if err != nil {
		return fmt.Errorf("store: query: %w", err)
	}
	type entry struct {
		doc *Doc
		key []*pb.Value
	}
	var entries []entry
	for rows.Next() {
		doc, err := scanDoc(rows)
		if err != nil {
			rows.Close()
			return err
		}
		if !q.Matches(doc.Name, doc.Fields) {
			continue
		}
		key := q.OrderKey(doc.Name, doc.Fields)
		if !q.InCursorRange(key) {
			continue
		}
		entries = append(entries, entry{doc, key})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	sort.SliceStable(entries, func(i, j int) bool {
		return q.CompareKeys(entries[i].key, entries[j].key) < 0
	})

	start := int(q.Offset)
	if start > len(entries) {
		start = len(entries)
	}
	end := len(entries)
	if q.Limit >= 0 && start+int(q.Limit) < end {
		end = start + int(q.Limit)
	}
	for _, e := range entries[start:end] {
		if err := yield(e.doc); err != nil {
			if err == ErrStop {
				return nil
			}
			return err
		}
	}
	return nil
}

// RunCount executes the query counting matches, honoring limit and upTo.
func (s *Store) RunCount(ctx context.Context, q *query.Query, upTo int64) (int64, error) {
	// Filter-less, cursor-less counts (the "how many documents in this
	// collection" shape) reduce exactly to SQL count(*) — no need to stream
	// and decode every document.
	if q.Where == nil && q.Start == nil && q.End == nil && q.Offset == 0 {
		where, args := s.baseConditions(q)
		var n int64
		if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM documents WHERE `+where, args...).Scan(&n); err != nil {
			return 0, fmt.Errorf("store: count: %w", err)
		}
		if q.Limit >= 0 && n > int64(q.Limit) {
			n = int64(q.Limit)
		}
		if upTo > 0 && n > upTo {
			n = upTo
		}
		return n, nil
	}

	bounded := *q
	if upTo > 0 && (bounded.Limit < 0 || upTo < int64(bounded.Limit)) {
		bounded.Limit = int32(upTo)
	}
	var n int64
	err := s.RunQuery(ctx, &bounded, func(*Doc) error {
		n++
		return nil
	})
	return n, err
}

func (s *Store) baseConditions(q *query.Query) (string, []any) {
	if q.AllDescendants && q.CollectionID == "" {
		// Kindless scan under parent: every collection at any depth, as the
		// SDKs' recursive queries ask for. Added in this fork.
		return "name >= $1 AND name < $2", []any{q.Parent + "/", q.Parent + "0"}
	}
	if q.AllDescendants {
		// Collection-group scan under parent: any depth, matching collection id.
		return "collection_id = $1 AND name >= $2 AND name < $3",
			[]any{q.CollectionID, q.Parent + "/", q.Parent + "0"}
	}
	return "parent_path = $1 AND collection_id = $2", []any{q.Parent, q.CollectionID}
}

// containmentProbes extracts jsonb containment probe groups from an AND-only
// filter tree. Each group is a set of alternatives (OR) for one filter —
// numeric equality probes both the int64 and double encodings when the
// operand is integral. Returns ok=false when the tree contains OR (no safe
// narrowing).
func containmentProbes(f *pb.StructuredQuery_Filter) ([][][]byte, bool) {
	if f == nil {
		return nil, true
	}
	var out [][][]byte
	switch t := f.GetFilterType().(type) {
	case *pb.StructuredQuery_Filter_CompositeFilter:
		if t.CompositeFilter.GetOp() != pb.StructuredQuery_CompositeFilter_AND {
			return nil, false
		}
		for _, sub := range t.CompositeFilter.GetFilters() {
			probes, ok := containmentProbes(sub)
			if !ok {
				return nil, false
			}
			out = append(out, probes...)
		}
	case *pb.StructuredQuery_Filter_FieldFilter:
		ff := t.FieldFilter
		if ff.GetField().GetFieldPath() == "__name__" {
			return out, true // pseudo-field: not a data field, no containment probe
		}
		path, err := value.ParseFieldPath(ff.GetField().GetFieldPath())
		if err != nil {
			return out, true
		}
		switch ff.GetOp() {
		case pb.StructuredQuery_FieldFilter_EQUAL:
			if group := equalityProbes(path, ff.GetValue(), value.ContainmentJSON); group != nil {
				out = append(out, group)
			}
		case pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS:
			if group := equalityProbes(path, ff.GetValue(), value.ArrayContainmentJSON); group != nil {
				out = append(out, group)
			}
		}
	}
	return out, true
}

func equalityProbes(path value.FieldPath, operand *pb.Value, build func(value.FieldPath, *pb.Value) ([]byte, error)) [][]byte {
	if value.IsNaN(operand) {
		return nil
	}
	var group [][]byte
	add := func(v *pb.Value) {
		if p, err := build(path, v); err == nil {
			group = append(group, p)
		}
	}
	add(operand)
	// Firestore numeric equality crosses int64/double; probe both encodings
	// when the operand is exactly representable in the other type.
	//
	// Zero needs a third probe. The tagged encoding preserves the sign of a
	// double zero ("-0" is a distinct string from "0"), because a round trip
	// must return exactly what was written. Firestore equality does not
	// distinguish them — and neither do Compare or the sortkey, which both
	// collapse -0.0 to +0.0 — so containment is the one place the two
	// spellings can drift apart. Probing only the operand's own spelling makes
	// `where x == 0` miss every document that stored -0.0, and vice versa.
	//
	// Found by the differential fuzz against Firestore's emulator, not by
	// reasoning: `a == -0` returned one document where Firestore returned three.
	switch t := operand.GetValueType().(type) {
	case *pb.Value_IntegerValue:
		f := float64(t.IntegerValue)
		if int64(f) == t.IntegerValue && f != math.MaxInt64 {
			add(&pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: f}})
		}
		if t.IntegerValue == 0 {
			add(&pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: math.Copysign(0, -1)}})
		}
	case *pb.Value_DoubleValue:
		f := t.DoubleValue
		if f == math.Trunc(f) && f >= math.MinInt64 && f < math.MaxInt64 {
			add(&pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: int64(f)}})
		}
		if f == 0 {
			add(&pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: 0}})
			add(&pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: math.Copysign(0, -1)}})
		}
	}
	if len(group) == 0 {
		return nil
	}
	return group
}

// scanDoc reads one row from a documents SELECT.
func scanDoc(rows interface {
	Scan(dest ...any) error
}) (*Doc, error) {
	var (
		name                   string
		data                   []byte
		createTime, updateTime time.Time
	)
	if err := rows.Scan(&name, &data, &createTime, &updateTime); err != nil {
		return nil, err
	}
	fields, err := value.UnmarshalFields(data)
	if err != nil {
		return nil, fmt.Errorf("store: document %s: %w", name, err)
	}
	return &Doc{Name: name, Fields: fields, CreateTime: createTime.UTC(), UpdateTime: updateTime.UTC()}, nil
}

// runIndexed streams a query from a composite index.
//
// The scan walks index_entries in key order, which is exactly query order, so
// Postgres applies OFFSET and LIMIT and only the surviving documents are
// fetched. That is the whole point: cost becomes O(limit) instead of O(matching
// documents).
//
// Every fetched document is still re-checked with q.Matches. On a correct plan
// that check never fails — the index prefix already encodes the equality
// filters — so it is an assertion, not filtering. It is affordable precisely
// because only the limit-sized page is fetched, and a failure means the planner
// chose an index that does not actually serve the query. That is returned as an
// error rather than silently skipped: dropping the document would turn a
// planner bug into a quietly short result set, which is the failure mode this
// whole design exists to avoid.
func (s *Store) runIndexed(ctx context.Context, q *query.Query, plan *index.Plan, yield func(*Doc) error) error {
	s.indexedQueries.Add(1)
	queryPath.WithLabelValues("indexed").Inc()
	if len(plan.Ranges) > 1 {
		s.indexedMerged.Add(1)
		queryPath.WithLabelValues("merged").Inc()
	}
	if _, ok := plan.Def.ContainsPath(); ok {
		s.indexedContains.Add(1)
		queryPath.WithLabelValues("contains").Inc()
	}

	names, err := s.scanIndex(ctx, q, plan)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		return nil
	}

	docs, err := s.GetDocuments(ctx, names)
	if err != nil {
		return err
	}
	for _, n := range names {
		doc, ok := docs[n]
		if !ok {
			// An entry without its document means maintenance and the document
			// write diverged, which the shared transaction is supposed to make
			// impossible.
			return fmt.Errorf("store: index %s references missing document %s",
				index.EncodeID(plan.Def.ID()), n)
		}
		if !q.Matches(doc.Name, doc.Fields) {
			return fmt.Errorf("store: index %s returned %s which does not match the query — planner chose an index that does not serve it",
				index.EncodeID(plan.Def.ID()), n)
		}
		if err := yield(doc); err != nil {
			if err == ErrStop {
				return nil
			}
			return err
		}
	}
	return nil
}

// scanIndex returns the document names for a plan, already in query order and
// already paged.
//
// One range is the common case and pushes ordering, LIMIT and OFFSET entirely
// into Postgres. Several ranges — an `in` filter — cannot: each range sits in a
// different part of the index, so their union is not contiguous and Postgres
// cannot order across them by key. The merge happens here instead.
func (s *Store) scanIndex(ctx context.Context, q *query.Query, plan *index.Plan) ([]string, error) {
	switch len(plan.Ranges) {
	case 0:
		return nil, nil
	case 1:
		r := plan.Ranges[0]
		rows, err := s.scanRange(ctx, plan, r, q.Limit, q.Offset)
		if err != nil {
			return nil, err
		}
		names := make([]string, len(rows))
		for i, e := range rows {
			names[i] = e.name
		}
		return names, nil
	}

	// Multi-range. Each range is fetched only as deep as the page could
	// possibly reach — offset+limit rows — because a document beyond that
	// position in its own range cannot appear within the page after merging.
	// Total rows read is k*(offset+limit) rather than every matching document.
	perRange := int32(-1)
	if q.Limit >= 0 && !plan.Dedup {
		perRange = q.Limit + q.Offset
	}
	// With dedup on, per-range prefetching is unsound: duplicates collapse
	// during the merge, so a page can need more rows from a range than
	// offset+limit, and a bounded prefetch would silently return a short page.
	// Reading each range in full still avoids fetching the documents, which is
	// where the real cost is.
	streams := make([][]indexRow, 0, len(plan.Ranges))
	for _, r := range plan.Ranges {
		rows, err := s.scanRange(ctx, plan, r, perRange, 0)
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 {
			streams = append(streams, rows)
		}
	}
	return mergeRanges(streams, plan.Reversed, q.Limit, q.Offset, plan.Dedup), nil
}

// indexRow is one index entry: the document it points at, and the part of its
// key that carries the ordering.
type indexRow struct {
	name   string
	suffix []byte
}

func (s *Store) scanRange(ctx context.Context, plan *index.Plan, r index.Range, limit, offset int32) ([]indexRow, error) {
	order := "ASC"
	if plan.Reversed {
		order = "DESC"
	}
	// substring() extracts the ordering suffix in Postgres so the merge does
	// not have to re-derive it, and so the equality prefix — which differs in
	// both value and LENGTH between ranges — never takes part in comparisons.
	args := []any{plan.Def.ID(), r.Lo, r.SuffixFrom + 1}
	sql := `SELECT e.doc_name, substring(e.key from $3) FROM index_entries e
	        WHERE e.index_id = $1 AND e.key >= $2`
	if r.Hi != nil {
		args = append(args, r.Hi)
		sql += fmt.Sprintf(" AND e.key < $%d", len(args))
	}
	sql += " ORDER BY e.key " + order
	if limit >= 0 {
		args = append(args, int64(limit))
		sql += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	if offset > 0 {
		args = append(args, int64(offset))
		sql += fmt.Sprintf(" OFFSET $%d", len(args))
	}

	rows, err := s.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("store: index scan: %w", err)
	}
	defer rows.Close()
	var out []indexRow
	for rows.Next() {
		var e indexRow
		if err := rows.Scan(&e.name, &e.suffix); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// mergeRanges interleaves already-sorted streams into one query-ordered list.
//
// Comparison is on the ordering suffix, never the whole key: the ranges differ
// precisely in their equality prefix, so comparing whole keys would order by
// the in-list value rather than by what the query asked to order by — grouping
// results by value instead of interleaving them, which is wrong and looks
// plausible.
func mergeRanges(streams [][]indexRow, reversed bool, limit, offset int32, dedup bool) []string {
	var seen map[string]bool
	if dedup {
		seen = map[string]bool{}
	}
	pos := make([]int, len(streams))
	var out []string
	skipped := int32(0)
	for {
		best := -1
		for i, st := range streams {
			if pos[i] >= len(st) {
				continue
			}
			if best == -1 {
				best = i
				continue
			}
			c := bytes.Compare(st[pos[i]].suffix, streams[best][pos[best]].suffix)
			if reversed {
				c = -c
			}
			if c < 0 {
				best = i
			}
		}
		if best == -1 {
			return out
		}
		row := streams[best][pos[best]]
		pos[best]++
		if seen != nil {
			// array-contains-any: one document can satisfy several values, so
			// it appears once per matching element. Firestore returns it once.
			if seen[row.name] {
				continue
			}
			seen[row.name] = true
		}
		if skipped < offset {
			skipped++
			continue
		}
		out = append(out, row.name)
		if limit >= 0 && int32(len(out)) >= limit {
			return out
		}
	}
}

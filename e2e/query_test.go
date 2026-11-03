package e2e

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

func seedQueryDocs(t *testing.T, client *firestore.Client) *firestore.CollectionRef {
	t.Helper()
	ctx := context.Background()
	coll := client.Collection("q")
	docs := map[string]map[string]any{
		"a": {"n": int64(1), "s": "alpha", "tags": []any{"x", "y"}, "group": "g1", "mix": int64(7)},
		"b": {"n": int64(2), "s": "beta", "tags": []any{"y"}, "group": "g1", "mix": "text"},
		"c": {"n": int64(3), "s": "gamma", "group": "g2", "mix": nil},
		"d": {"s": "delta", "group": "g2"}, // no "n": excluded by orderBy(n)
	}
	for id, data := range docs {
		if _, err := coll.Doc(id).Set(ctx, data); err != nil {
			t.Fatal(err)
		}
	}
	return coll
}

func ids(t *testing.T, it *firestore.DocumentIterator) []string {
	t.Helper()
	var out []string
	for {
		snap, err := it.Next()
		if err == iterator.Done {
			return out
		}
		if err != nil {
			t.Fatalf("iterate: %v", err)
		}
		out = append(out, snap.Ref.ID)
	}
}

func TestSDKQueries(t *testing.T) {
	startKord(t)
	client := newClient(t)
	ctx := context.Background()
	coll := seedQueryDocs(t, client)

	t.Run("range with type-bucket exclusion", func(t *testing.T) {
		// mix > 0 must match only the numeric mix, never the string or null.
		got := ids(t, coll.Where("mix", ">", 0).Documents(ctx))
		if len(got) != 1 || got[0] != "a" {
			t.Fatalf("mix>0 = %v, want [a]", got)
		}
	})

	t.Run("equality and chained filters", func(t *testing.T) {
		got := ids(t, coll.Where("group", "==", "g1").Where("n", ">=", 2).Documents(ctx))
		if len(got) != 1 || got[0] != "b" {
			t.Fatalf("got %v, want [b]", got)
		}
	})

	t.Run("in and not-equal", func(t *testing.T) {
		got := ids(t, coll.Where("s", "in", []any{"alpha", "gamma"}).OrderBy("s", firestore.Asc).Documents(ctx))
		if fmt.Sprint(got) != "[a c]" {
			t.Fatalf("in = %v", got)
		}
		// != excludes null mix (c) and matches only existing non-equal values.
		got = ids(t, coll.Where("mix", "!=", "text").Documents(ctx))
		if len(got) != 1 || got[0] != "a" {
			t.Fatalf("mix != text = %v, want [a]", got)
		}
	})

	t.Run("array-contains and any", func(t *testing.T) {
		got := ids(t, coll.Where("tags", "array-contains", "x").Documents(ctx))
		if len(got) != 1 || got[0] != "a" {
			t.Fatalf("array-contains = %v", got)
		}
		got = ids(t, coll.Where("tags", "array-contains-any", []any{"y", "zz"}).OrderBy(firestore.DocumentID, firestore.Asc).Documents(ctx))
		if fmt.Sprint(got) != "[a b]" {
			t.Fatalf("array-contains-any = %v", got)
		}
	})

	t.Run("order limit and missing-field exclusion", func(t *testing.T) {
		got := ids(t, coll.OrderBy("n", firestore.Desc).Documents(ctx))
		if fmt.Sprint(got) != "[c b a]" { // d has no n → excluded
			t.Fatalf("orderBy n desc = %v", got)
		}
		got = ids(t, coll.OrderBy("n", firestore.Desc).Limit(2).Documents(ctx))
		if fmt.Sprint(got) != "[c b]" {
			t.Fatalf("limit = %v", got)
		}
	})

	t.Run("snapshot cursor StartAfter", func(t *testing.T) {
		snap, err := coll.Doc("c").Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got := ids(t, coll.OrderBy("n", firestore.Desc).StartAfter(snap).Documents(ctx))
		if fmt.Sprint(got) != "[b a]" {
			t.Fatalf("startAfter(snapshot) = %v", got)
		}
	})

	t.Run("value cursors and offset", func(t *testing.T) {
		got := ids(t, coll.OrderBy("n", firestore.Asc).StartAt(2).EndBefore(3).Documents(ctx))
		if fmt.Sprint(got) != "[b]" {
			t.Fatalf("startAt/endBefore = %v", got)
		}
		got = ids(t, coll.OrderBy("n", firestore.Asc).Offset(1).Limit(1).Documents(ctx))
		if fmt.Sprint(got) != "[b]" {
			t.Fatalf("offset/limit = %v", got)
		}
	})

	t.Run("limitToLast", func(t *testing.T) {
		snaps, err := coll.OrderBy("n", firestore.Asc).LimitToLast(2).Documents(ctx).GetAll()
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, s := range snaps {
			got = append(got, s.Ref.ID)
		}
		if fmt.Sprint(got) != "[b c]" {
			t.Fatalf("limitToLast = %v", got)
		}
	})

	t.Run("projection and keys-only", func(t *testing.T) {
		it := coll.Where("s", "==", "alpha").Select("s").Documents(ctx)
		snap, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		data := snap.Data()
		if len(data) != 1 || data["s"] != "alpha" {
			t.Fatalf("projection = %v", data)
		}
		it = coll.Where("s", "==", "alpha").Select().Documents(ctx)
		snap, err = it.Next()
		if err != nil {
			t.Fatal(err)
		}
		if len(snap.Data()) != 0 || snap.Ref.ID != "a" {
			t.Fatalf("keys-only = %v (%s)", snap.Data(), snap.Ref.ID)
		}
	})

	t.Run("empty result", func(t *testing.T) {
		got := ids(t, coll.Where("s", "==", "nope").Documents(ctx))
		if len(got) != 0 {
			t.Fatalf("want empty, got %v", got)
		}
	})

	t.Run("documentID prefix range scan", func(t *testing.T) {
		// The Where(DocumentID, ">=", prefix) / "<" prefix+"" pattern
		// used for per-user cache families like {uid}_{date}_{seq}.
		pcoll := client.Collection("prefixed")
		for _, id := range []string{"u1_2026-01-01_0", "u1_2026-01-02_1", "u2_2026-01-01_0"} {
			if _, err := pcoll.Doc(id).Set(ctx, map[string]any{"v": id}); err != nil {
				t.Fatal(err)
			}
		}
		got := ids(t, pcoll.
			Where(firestore.DocumentID, ">=", "u1_").
			Where(firestore.DocumentID, "<", "u1_").
			Documents(ctx))
		if fmt.Sprint(got) != "[u1_2026-01-01_0 u1_2026-01-02_1]" {
			t.Fatalf("prefix scan = %v", got)
		}
		// With an explicit orderBy(DocumentID) + limit, exercising pushdown.
		got = ids(t, pcoll.
			Where(firestore.DocumentID, ">=", "u1_").
			OrderBy(firestore.DocumentID, firestore.Desc).Limit(2).
			Documents(ctx))
		if fmt.Sprint(got) != "[u2_2026-01-01_0 u1_2026-01-02_1]" {
			t.Fatalf("name-ordered scan = %v", got)
		}
		// DocumentID 'in' chunks (batched key lookups via query).
		got = ids(t, pcoll.
			Where(firestore.DocumentID, "in", []string{"u1_2026-01-01_0", "u2_2026-01-01_0", "missing"}).
			Documents(ctx))
		if fmt.Sprint(got) != "[u1_2026-01-01_0 u2_2026-01-01_0]" {
			t.Fatalf("documentID in = %v", got)
		}
	})

	t.Run("collection group", func(t *testing.T) {
		for _, p := range []string{"p1", "p2"} {
			if _, err := client.Collection("cgparent").Doc(p).Collection("items").Doc("i").Set(ctx, map[string]any{"p": p}); err != nil {
				t.Fatal(err)
			}
		}
		got := ids(t, client.CollectionGroup("items").Documents(ctx))
		if len(got) != 2 {
			t.Fatalf("collection group = %v", got)
		}
		got = ids(t, client.CollectionGroup("items").Where("p", "==", "p2").Documents(ctx))
		if len(got) != 1 {
			t.Fatalf("filtered collection group = %v", got)
		}
	})

	t.Run("count aggregation", func(t *testing.T) {
		all := coll.Query
		res, err := all.NewAggregationQuery().WithCount("total").Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n := countOf(t, res, "total"); n != 4 {
			t.Fatalf("count = %d, want 4", n)
		}
		filtered := coll.Where("group", "==", "g1")
		res, err = filtered.NewAggregationQuery().WithCount("total").Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n := countOf(t, res, "total"); n != 2 {
			t.Fatalf("filtered count = %d, want 2", n)
		}
	})
}

func countOf(t *testing.T, res firestore.AggregationResult, alias string) int64 {
	t.Helper()
	v, ok := res[alias]
	if !ok {
		t.Fatalf("aggregation result missing alias %q: %v", alias, res)
	}
	pbVal, ok := v.(interface{ GetIntegerValue() int64 })
	if !ok {
		t.Fatalf("aggregation value has unexpected type %T", v)
	}
	return pbVal.GetIntegerValue()
}

func TestSDKTransactions(t *testing.T) {
	startKord(t)
	client := newClient(t)
	ctx := context.Background()

	t.Run("contended increments are exact", func(t *testing.T) {
		ref := client.Collection("txn").Doc("counter")
		if _, err := ref.Set(ctx, map[string]any{"count": int64(0)}); err != nil {
			t.Fatal(err)
		}
		// Modified in this fork: 40 contenders (was 20). Transactions lock what
		// they read, as Firestore's do, so all of them commit within the
		// SDK's default retries on every backend, not only on a fast one.
		const workers = 40
		var wg sync.WaitGroup
		errs := make(chan error, workers)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
					snap, err := tx.Get(ref)
					if err != nil {
						return err
					}
					n, _ := snap.Data()["count"].(int64)
					return tx.Set(ref, map[string]any{"count": n + 1})
				})
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("transaction failed: %v", err)
			}
		}
		snap, _ := ref.Get(ctx)
		if got := snap.Data()["count"]; got != int64(workers) {
			t.Fatalf("count = %v, want %d (lost updates!)", got, workers)
		}
	})

	t.Run("create-if-absent races resolve exactly once", func(t *testing.T) {
		ref := client.Collection("txn").Doc("create-race")
		const workers = 5
		var wg sync.WaitGroup
		errs := make(chan error, workers)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
					snap, err := tx.Get(ref)
					if err != nil && !snap.Exists() {
						// Missing: first writer creates with count 1. The read
						// of the missing doc must still be version-checked.
						return tx.Set(ref, map[string]any{"count": int64(1)})
					}
					if err != nil {
						return err
					}
					n, _ := snap.Data()["count"].(int64)
					return tx.Set(ref, map[string]any{"count": n + 1})
				})
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("transaction failed: %v", err)
			}
		}
		snap, _ := ref.Get(ctx)
		if got := snap.Data()["count"]; got != int64(workers) {
			t.Fatalf("count = %v, want %d (create race mishandled)", got, workers)
		}
	})

	t.Run("read-only transaction validates", func(t *testing.T) {
		ref := client.Collection("txn").Doc("ro")
		if _, err := ref.Set(ctx, map[string]any{"v": int64(1)}); err != nil {
			t.Fatal(err)
		}
		err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
			_, err := tx.Get(ref)
			return err
		})
		if err != nil {
			t.Fatalf("read-only transaction: %v", err)
		}
	})
}

// Added in this fork: queries inside transactions. Workers claim open jobs by
// querying for one and updating it in the same transaction, the way job
// queues built on Firestore do. Every job is claimed exactly once.
func TestSDKTransactionQueryClaims(t *testing.T) {
	startKord(t)
	client := newClient(t)
	ctx := context.Background()
	jobs := client.Collection("claims")
	const jobCount, workers = 10, 30
	for i := 0; i < jobCount; i++ {
		if _, err := jobs.Doc(fmt.Sprintf("job%02d", i)).Set(ctx, map[string]any{"status": "open", "n": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	var (
		mu      sync.Mutex
		claimed = map[string]int{}
		wg      sync.WaitGroup
	)
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var got string
			err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
				got = ""
				docs, err := tx.Documents(jobs.Where("status", "==", "open").OrderBy("n", firestore.Asc).Limit(1)).GetAll()
				if err != nil || len(docs) == 0 {
					return err
				}
				got = docs[0].Ref.ID
				return tx.Update(docs[0].Ref, []firestore.Update{{Path: "status", Value: fmt.Sprintf("worker-%d", w)}})
			})
			if err == nil && got != "" {
				mu.Lock()
				claimed[got]++
				mu.Unlock()
			}
			errs <- err
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("claim transaction failed: %v", err)
		}
	}
	if len(claimed) != jobCount {
		t.Fatalf("claimed %d distinct jobs, want %d: %v", len(claimed), jobCount, claimed)
	}
	for id, n := range claimed {
		if n != 1 {
			t.Fatalf("job %s claimed %d times (double claim)", id, n)
		}
	}
}

// Added in this fork: ListDocuments, which the server SDKs use for
// CollectionRef.DocumentRefs / list_documents, paged by the SDK.
func TestSDKListDocuments(t *testing.T) {
	startKord(t)
	client := newClient(t)
	ctx := context.Background()
	coll := client.Collection("listed")
	const count = 25
	for i := 0; i < count; i++ {
		if _, err := coll.Doc(fmt.Sprintf("d%02d", i)).Set(ctx, map[string]any{"i": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.Collection("other").Doc("x").Set(ctx, map[string]any{"i": int64(0)}); err != nil {
		t.Fatal(err)
	}
	var ids []string
	it := coll.DocumentRefs(ctx)
	for {
		ref, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("DocumentRefs: %v", err)
		}
		ids = append(ids, ref.ID)
	}
	if len(ids) != count {
		t.Fatalf("listed %d documents, want %d: %v", len(ids), count, ids)
	}
	for i, id := range ids {
		if want := fmt.Sprintf("d%02d", i); id != want {
			t.Fatalf("ids[%d] = %s, want %s (name order)", i, id, want)
		}
	}
}

// Added in this fork: name cursors containing NUL. The Python SDK's
// recursive_delete bounds its descendants query with a reference ending in
// "\x00/…"; PostgreSQL text cannot hold NUL, so such bounds must not reach SQL
// as-is. NUL sorts below every other character.
func TestSDKNameCursorWithNUL(t *testing.T) {
	startKord(t)
	client := newClient(t)
	ctx := context.Background()
	coll := client.Collection("nulcursor")
	for _, id := range []string{"a", "ab", "b"} {
		if _, err := coll.Doc(id).Set(ctx, map[string]any{"id": id}); err != nil {
			t.Fatal(err)
		}
	}
	ids := func(q firestore.Query) []string {
		t.Helper()
		docs, err := q.Documents(ctx).GetAll()
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		out := make([]string, len(docs))
		for i, d := range docs {
			out[i] = d.Ref.ID
		}
		return out
	}
	byName := coll.OrderBy(firestore.DocumentID, firestore.Asc)
	if got := ids(byName.EndAt("a\x00")); fmt.Sprint(got) != "[a]" {
		t.Fatalf("EndAt(a\\x00) = %v, want [a]", got)
	}
	if got := ids(byName.StartAfter("a\x00")); fmt.Sprint(got) != "[ab b]" {
		t.Fatalf("StartAfter(a\\x00) = %v, want [ab b]", got)
	}
	if got := ids(byName.StartAt("a\x00").EndBefore("b")); fmt.Sprint(got) != "[ab]" {
		t.Fatalf("StartAt(a\\x00).EndBefore(b) = %v, want [ab]", got)
	}
}

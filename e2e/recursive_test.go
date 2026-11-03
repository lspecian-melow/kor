// Added in this fork: the query behind the Python SDK's recursive_delete.
//
// Query.recursive() lists every document below one collection, at any depth:
// an all-descendants selector with an empty collection id under the database
// root, ordered by __name__, bounded by a start reference at the collection's
// minimum id and an end reference whose collection segment ends in "\x00".
package e2e

import (
	"context"
	"io"
	"os"
	"sort"
	"testing"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRecursiveDescendantsQuery(t *testing.T) {
	startKord(t)
	client := newClient(t)
	ctx := context.Background()
	write := func(path string) {
		t.Helper()
		if _, err := client.Doc(path).Set(ctx, map[string]any{"p": path}); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []string{
		"generated_documents/doc1",
		"generated_documents/doc1/revisions/r1",
		"generated_documents/doc1/revisions/r2",
		"generated_documents/doc1/revisions/r1/notes/n1",
		"generated_documents/doc1/revisionsx/other",
		"generated_documents/doc2/revisions/r9",
	} {
		write(p)
	}

	conn, err := grpc.NewClient(os.Getenv("FIRESTORE_EMULATOR_HOST"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	root := "projects/kor-e2e/databases/(default)/documents"
	coll := root + "/generated_documents/doc1/revisions"
	ref := func(s string) *pb.Value { return &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: s}} }
	byName := &pb.StructuredQuery_FieldReference{FieldPath: "__name__"}
	stream, err := pb.NewFirestoreClient(conn).RunQuery(ctx, &pb.RunQueryRequest{
		Parent: root,
		QueryType: &pb.RunQueryRequest_StructuredQuery{StructuredQuery: &pb.StructuredQuery{
			From:    []*pb.StructuredQuery_CollectionSelector{{CollectionId: "", AllDescendants: true}},
			OrderBy: []*pb.StructuredQuery_Order{{Field: byName, Direction: pb.StructuredQuery_ASCENDING}},
			StartAt: &pb.Cursor{Values: []*pb.Value{ref(coll + "/__id-9223372036854775808__")}, Before: true},
			EndAt:   &pb.Cursor{Values: []*pb.Value{ref(coll + "\x00/__id-9223372036854775808__")}, Before: false},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("RunQuery: %v", err)
		}
		if d := resp.GetDocument(); d != nil {
			got = append(got, d.GetName()[len(root)+1:])
		}
	}
	sort.Strings(got)
	want := []string{
		"generated_documents/doc1/revisions/r1",
		"generated_documents/doc1/revisions/r1/notes/n1",
		"generated_documents/doc1/revisions/r2",
	}
	if len(got) != len(want) {
		t.Fatalf("descendants = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("descendants = %v, want %v", got, want)
		}
	}
}

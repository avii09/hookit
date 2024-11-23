package input

import (
	"context"

	"cloud.google.com/go/firestore"
)

func ReadFirebase(client *firestore.Client, collection string) ([]map[string]interface{}, error) {
	var data []map[string]interface{}
	iter := client.Collection(collection).Documents(context.Background())
	for {
		doc, err := iter.Next()
		if err != nil {
			break
		}
		data = append(data, doc.Data())
	}
	return data, nil
}

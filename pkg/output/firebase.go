package output

import (
	"context"

	"cloud.google.com/go/firestore"
)

// WriteFirebase writes the transformed data to a Firebase collection.
func WriteFirebase(client *firestore.Client, collection string, data []map[string]interface{}) error {
	for _, row := range data {
		_, _, err := client.Collection(collection).Add(context.Background(), row)
		if err != nil {
			return err
		}
	}
	return nil
}

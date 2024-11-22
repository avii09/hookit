package transform

func ApplyFirebaseTransformations(data []map[string]interface{}, mapping MappingRules) []map[string]interface{} {
	for _, row := range data {
		// Change the name field
		if _, exists := row["name"]; exists {
			row["name"] = mapping.NameChange
		}
		// Add new fields
		for key, value := range mapping.AddField {
			row[key] = value
		}
	}
	return data
}

package ai

import (
	"encoding/json"
	"regexp"
	"strings"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/jsonclean"
	"github.com/AndrewK4758/shared_utils/workflowparser"
)

var whitespaceRegex = regexp.MustCompile(`\s+`)

// NormalizeVisionOutput cleans, parses, and normalizes vision model JSON outputs according to schema contracts.
func NormalizeVisionOutput(responseStr string, req *pb.ExecuteTaskRequest) string {
	cleaned := jsonclean.CleanJSON(responseStr)

	schemaObj := extractSchemaMapSafely(req)
	if schemaObj == nil {
		return cleaned
	}

	var parsed any
	if err := json.Unmarshal([]byte(cleaned), &parsed); err != nil {
		return cleaned
	}

	normalized := normalizeBySchema(parsed, schemaObj)

	resBytes, err := json.Marshal(normalized)
	if err != nil {
		return cleaned
	}
	return string(resBytes)
}

func extractSchemaMapSafely(req *pb.ExecuteTaskRequest) (schema map[string]any) {
	if req == nil || req.Payload == nil {
		return nil
	}

	if fields := req.Payload.GetFields(); fields != nil {
		if ncVal, ok := fields["node_config"]; ok && ncVal.GetStructValue() != nil {
			if expVal, ok := ncVal.GetStructValue().GetFields()["expected_schema_struct"]; ok && expVal.GetStructValue() != nil {
				return expVal.GetStructValue().AsMap()
			}
		}
	}

	defer func() {
		if r := recover(); r != nil {
			schema = nil
		}
	}()

	parser := workflowparser.NewTaskRequestParser(req)
	payload := parser.MustGetExecuteTaskPayload()
	if payload != nil && payload.NodeConfig != nil && payload.NodeConfig.ExpectedSchemaStruct != nil {
		return payload.NodeConfig.ExpectedSchemaStruct.AsMap()
	}
	return nil
}

func normalizeBySchema(val any, schema map[string]any) any {
	if schema == nil {
		return normalizeScalar(val, "")
	}

	schemaType, _ := schema["type"].(string)

	switch schemaType {
	case "object":
		return normalizeObject(val, schema)
	case "array":
		return normalizeArray(val, schema)
	case "boolean":
		return normalizeBoolean(val)
	case "string":
		return normalizeString(val)
	case "number", "integer":
		return normalizeNumber(val)
	default:
		return normalizeGeneric(val, schema)
	}
}

func normalizeObject(val any, schema map[string]any) any {
	objMap, ok := val.(map[string]any)
	if !ok {
		return nil
	}

	props, _ := schema["properties"].(map[string]any)
	if props == nil {
		res := make(map[string]any, len(objMap))
		for k, v := range objMap {
			res[k] = normalizeScalar(v, "")
		}
		return res
	}

	sanitized := make(map[string]any, len(props))
	for propName, propSchemaVal := range props {
		propSchema, _ := propSchemaVal.(map[string]any)
		rawVal, exists := objMap[propName]
		if !exists {
			sanitized[propName] = nil
			continue
		}
		sanitized[propName] = normalizeBySchema(rawVal, propSchema)
	}

	return sanitized
}

func normalizeArray(val any, schema map[string]any) any {
	arr, ok := val.([]any)
	if !ok {
		return nil
	}

	itemsSchema, _ := schema["items"].(map[string]any)

	sanitizedArr := make([]any, len(arr))
	for i, item := range arr {
		sanitizedArr[i] = normalizeBySchema(item, itemsSchema)
	}
	return sanitizedArr
}

func normalizeBoolean(val any) any {
	if val == nil {
		return nil
	}
	if b, ok := val.(bool); ok {
		return b
	}
	if str, ok := val.(string); ok {
		trimmed := strings.TrimSpace(strings.ToLower(str))
		switch trimmed {
		case "true", "yes", "1", "[x]", "[x ]", "[ x]", "checked", "selected":
			return true
		case "false", "no", "0", "[]", "[ ]", "unchecked", "unselected":
			return false
		case "null", "blank", "n/a", "none", "", "indeterminate":
			return nil
		}
	}
	return nil
}

func normalizeString(val any) any {
	if val == nil {
		return nil
	}
	str, ok := val.(string)
	if !ok {
		return nil
	}
	return cleanString(str)
}

func normalizeNumber(val any) any {
	if val == nil {
		return nil
	}
	if str, ok := val.(string); ok {
		if cleanString(str) == nil {
			return nil
		}
	}
	return val
}

func normalizeGeneric(val any, schema map[string]any) any {
	if props, ok := schema["properties"].(map[string]any); ok && len(props) > 0 {
		return normalizeObject(val, schema)
	}
	if items, ok := schema["items"].(map[string]any); ok && items != nil {
		return normalizeArray(val, schema)
	}
	return normalizeScalar(val, "")
}

func normalizeScalar(val any, targetType string) any {
	if val == nil {
		return nil
	}
	if b, ok := val.(bool); ok {
		return b
	}
	if str, ok := val.(string); ok {
		return cleanString(str)
	}
	if obj, ok := val.(map[string]any); ok {
		res := make(map[string]any, len(obj))
		for k, v := range obj {
			res[k] = normalizeScalar(v, "")
		}
		return res
	}
	if arr, ok := val.([]any); ok {
		res := make([]any, len(arr))
		for i, v := range arr {
			res[i] = normalizeScalar(v, "")
		}
		return res
	}
	return val
}

func cleanString(str string) any {
	trimmed := strings.TrimSpace(str)
	if trimmed == "" || strings.EqualFold(trimmed, "null") {
		return nil
	}

	if strings.Contains(trimmed, "\n") || strings.Contains(trimmed, "\r") || strings.Contains(trimmed, "  ") {
		trimmed = whitespaceRegex.ReplaceAllString(trimmed, " ")
		trimmed = strings.TrimSpace(trimmed)
	}

	return trimmed
}

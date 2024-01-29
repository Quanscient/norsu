package match

import "strings"

func Normalize(aType matchType, prop string) string {
	if aType == matchTypeColumn {
		return strings.ToLower(strings.ReplaceAll(prop, "_", ""))
	}

	return strings.ToLower(prop)
}

package reviews

import "github.com/pemistahl/lingua-go"

type LanguageIdentifier struct {
	detector lingua.LanguageDetector
}

func NewLanguageIdentifier() *LanguageIdentifier {
	languages := []lingua.Language{
		lingua.English,
		lingua.Spanish,
		lingua.French,
	}

	detector := lingua.NewLanguageDetectorBuilder().FromLanguages(languages...).Build()
	return &LanguageIdentifier{
		detector: detector,
	}
}

func (l *LanguageIdentifier) IsEnglish(text string) bool {
	language, reliable := l.detector.DetectLanguageOf(text)
	if !reliable {
		return false
	}
	return language == lingua.English
}

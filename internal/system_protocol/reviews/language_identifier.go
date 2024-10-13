package reviews

import "github.com/pemistahl/lingua-go"

type LanguageIdentifier struct {
	detector lingua.LanguageDetector
}

func NewLanguageIdentifier() *LanguageIdentifier {
	detector := lingua.NewLanguageDetectorBuilder().FromAllLanguages().WithLowAccuracyMode().Build()
	return &LanguageIdentifier{
		detector: detector,
	}
}

func (l *LanguageIdentifier) IsEnglish(text string) bool {
	language, _ := l.detector.DetectLanguageOf(text)
	return language == lingua.English
}

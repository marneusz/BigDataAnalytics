import nltk
import contractions

class TextNormalizer:
    
    def __init__(self):
        nltk.download('words')
        self.corpus_words = set(nltk.corpus.words.words())
        self.stop_words = set(nltk.corpus.stopwords.words('english'))
        self.lemmatizer = nltk.stem.WordNetLemmatizer()
    
    def normalize(self, text):
        proc_text = " ".join(contractions.fix(w).lower() for w in text.split())
        proc_text = re.sub(r'[^a-zA-Z\s]', '', proc_text)
        proc_text = re.sub("\s\s+", " ", proc_text)
        proc_text = " ".join(self.lemmatizer.lemmatize(w) for w in nltk.word_tokenize(proc_text)
                          if w in self.corpus_words and w not in self.stop_words)
        return proc_text.strip()

import spacy
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import logging

logger = logging.getLogger(__name__)

# Initialize spaCy and VADER
nlp = spacy.load("en_core_web_sm")
analyzer = SentimentIntensityAnalyzer()

def clean_text(text):
    doc = nlp(text)
    cleaned_tokens = []
    
    for token in doc:
        # Only keep alphabetic tokens, remove stopwords and punctuation
        if token.is_alpha and not token.is_stop:
            cleaned_tokens.append(token.text.lower())
    
    return ' '.join(cleaned_tokens)

def analyze_sentiments(reviews):
    sentiments = []
    
    for review in reviews:
        review_text = review['review_text']
        
        # Clean the review text using spaCy
        cleaned_text = clean_text(review_text)
        
        # Perform sentiment analysis with VADER on the cleaned text
        sentiment_score = analyzer.polarity_scores(cleaned_text)['compound']
        
        # Rescale VADER score from [-1, 1] to [0, 10]
        scaled_score = (sentiment_score + 1) * 5  # Transforms the score to [0, 10]
        sentiments.append(scaled_score)
    
    # Calculate the average sentiment
    if sentiments:
        average_sentiment = sum(sentiments) / len(sentiments)
    else:
        average_sentiment = 0.0
    
    return average_sentiment

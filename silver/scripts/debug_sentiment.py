from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

reviews = [
    "I'm obsessed with Mario's pepperoni slices! Crunchy on the outside, chewy on the inside. 10/10",
    "Just had the best margherita pizza of my life at Mario's! Fresh mozzarella and basil were a game-changer.",
    "We got the veggie lovers' platter and it was amazing! Great selection of pizzas for vegetarians and veggies alike.",
    "Worst pizza experience ever! Mario's Pizzeria had subpar crust and dry sauce.",
]

print("VADER SCORE BREAKDOWN")
print("=" * 70)

for r in reviews:
    scores = analyzer.polarity_scores(r)
    print(f"\nReview: {r[:80]}...")
    print(f"  Negative: {scores['neg']:.3f}")
    print(f"  Neutral:  {scores['neu']:.3f}")
    print(f"  Positive: {scores['pos']:.3f}")
    print(f"  Compound: {scores['compound']:.3f}")
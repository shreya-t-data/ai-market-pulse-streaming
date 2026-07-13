from nltk.sentiment import SentimentIntensityAnalyzer

sia = SentimentIntensityAnalyzer()


def score(text):
    return sia.polarity_scores(text)["compound"]


def test_positive_headline_scores_positive():
    assert score("Company beats earnings, investors love the results") > 0


def test_negative_headline_scores_negative():
    assert score("Company posts terrible losses, investors are furious") < 0


def test_neutral_headline_scores_near_zero():
    assert abs(score("Company realeases quarterly report")) < 0.3

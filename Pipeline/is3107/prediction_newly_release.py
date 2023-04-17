from .predict_genre import predict_genre_op
from .predict_popularity import predict_popularity_op

def prediction_newly_release_op():
    predict_popularity_op()
    print("POPULARITY PREDICTED")

    predict_genre_op()
    print("GENRE PREDICTED")
from recommendations import dbase, recommender

db = dbase()
reco = recommender(db)
print reco.user_similarity_distance(1, 2, 'Pearson')
print reco.user_similarity_distance(1, 2, 'Euclidian')
print reco.top_matches(1)
print reco.get_recommendations(7, 3)
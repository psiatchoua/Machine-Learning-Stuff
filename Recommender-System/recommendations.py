# coding=utf-8
import sys
import time
import logging
import datetime
import psycopg2
from math import sqrt


# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create file handler and set level to DEBUG
hFile = logging.FileHandler("/home/patrice/Documents/Logs/recommender.log")
hFile.setLevel(logging.DEBUG)

# create console handler with a higher log level
hCons = logging.StreamHandler()
hCons.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to file handler
hFile.setFormatter(formatter)

# add formatter to console handler
hCons.setFormatter(formatter)

# add handlers to logger
logger.addHandler(hFile)
logger.addHandler(hCons)

class dbase:
	''' Utility class to handle communication with db'''
	def __init__(self):
		self.db = self.connect()
		self.cur = self.db.cursor()

	def connect(self):
		try:
			db = psycopg2.connect(database="postgres",host="localhost",port=5432,user="postgres",password="")
			db.autocommit = True
			return (db)
		except:
			logger.error('unable to connect to the database')

	def commit(self, errorMsg):
		try:
			self.db.commit()
		except:
			logger.error('Unexpected error:, ' + str(sys.exc_info()[0]) + '. Application reported: ' + errorMsg)

	def execute(self, queryStr, queryParams, errorMsg):
		try:
			if len(queryParams) == 0:
				self.cur.execute(queryStr)
			else:
				self.cur.execute(queryStr, queryParams)
		except:
			logger.error('Unexpected error:, ' + str(sys.exc_info()[0]) + '. Application reported: ' + errorMsg)

	def fetchall(self):
		return(self.cur.fetchall())

	def fetchone(self):
		return(self.cur.fetchone())

	def close(self):
		return self.db.close()

class recommender:
	''' Utility class for the recommender system'''
	def __init__(self, db):
		self.db = db

	def __get_euclidian_dist(self, x, y):
		sum_of_squares = sum([pow(x[item] - y[item],2) for item in x.keys() if item in y.keys()])
		return 1 / (1 + sqrt(sum_of_squares)) # add 1 to avoid dividing by zero and inverse to have high score for simalar user

	def __get_pearson_corr_coef(self, x, y):
		''' http://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient '''
		x_mean = float(sum([x[item] for item in x.keys()])) / len(x) # no need to check if len(x) is 0 as this function if call only when both x and y have common elements
		y_mean = float(sum([y[item] for item in y.keys()])) / len(y)

		num = sum([(x[item] - x_mean) * (y[item] - y_mean) for item in x.keys() if item in y.keys()])
		x_den = sqrt(sum([ pow(x[item] - x_mean, 2) for item in x.keys()]))
		y_den = sqrt(sum([ pow(y[item] - y_mean, 2) for item in y.keys()]))
		den = x_den * y_den

		return 0 if den == 0 else float(num) / den


	def __sim_distance(self, user1, user2, similarity_method):
		''' helper function to calculate the distance between two users '''
		result = 0
		try:
			query = '''select user_id, item_id, rating from reco.critics where user_id in ( 
				''' + str(user1) + ''',''' + str(user2) + ''')'''
			
			self.db.execute(query, {}, "Error while selecting data in internal method __sim_distance")
			rows = self.db.fetchall()
			user1_prefs = {}
			user2_prefs = {}

			for row in rows:
				if int(row[0]) == user1:
					user1_prefs[row[1]] = float(row[2])
				else:
					user2_prefs[row[1]] = float(row[2])

			if similarity_method.lower() == 'euclidian':
				result = self.__get_euclidian_dist(user1_prefs, user2_prefs)
			elif similarity_method.lower() == 'pearson': #pearson correlation or centered cosine similarity
				result = self.__get_pearson_corr_coef(user1_prefs, user2_prefs)

			return result;

		except:
			logger.error('Unexpected error:, ' + str(sys.exc_info()[0]) + ' in internal method __sim_distance')

	def user_similarity_distance(self, user1, user2, similarity_method):
		''' returns a distance-based similarity score for user1 and user2 (user to user variant)
		:param user1 is a user 1 id 
		:param user2 is a user 2 id
		:returns a value between 0 and 1 where 1 means identical preferences
		'''
		try:
			query = ''' with common_prefs as (
			select distinct item_id from reco.critics where user_id = %s
			 intersect 
			select distinct item_id from reco.critics where user_id = %s
			 )
			select count(*) from common_prefs;
			'''

			self.db.execute(query, (str(user1), str(user2)), "Error select from user_similarity_distance")
			nb_common_prefs = self.db.fetchone()

			if nb_common_prefs and nb_common_prefs > 0:
				return self.__sim_distance(user1, user2, similarity_method)
			else:
				return 0
		except:
			logger.error('Unexpected error:, ' + str(sys.exc_info()[0]) + ' from user_similarity_distance')

	def top_matches(self, user, n=3, similarity_method = 'Pearson'):
		''' return the best matches for a user.
		:param user is a user id 
		:param n is the number of max results to return, default is 3
		:param similarity_method is the similarity method, default is pearson
		:returns the top matches
		'''
		result = []
		try:
			query = 'select distinct user_id from reco.critics where user_id != %s'
			self.db.execute(query, (str(user)), "Error while querying data in top_matches")
			rows = self.db.fetchall()
			others = []
			for row in rows:
				others.append(row[0])

			if others:
				scores = [(item, self.user_similarity_distance(user, int(item), similarity_method)) for item in others]
				if scores:
					sorted_scores = sorted(scores, key=lambda score: score[1], reverse=True) # sort by similarity_score, desc
					result = sorted_scores[0:n]

			return result
		except:
			logger.error('Unexpected error:, ' + str(sys.exc_info()[0]) + ' from top_matches')

	def get_recommendations(self, user_id, n=2, similarity_method= 'Pearson'):
		''' return the best items to recommend to a particular user
		a matrix completion, we need to estimated the rating a user would give to a previously unseen movie
		:param user_id is a user id
		:param n is the number of max resumts to return, default is 2
		:param similarity_method is the similarity method, default is pearson
		:returns the recommended items
		'''
		result = []
		try:
			# get the unseen movies from the db
			query = '''with x as (
			select m.item_id
			from reco.items m
			left join reco.critics c 
			on m.item_id = c.item_id and c.user_id = %s
			where c.item_id is null
			)
			select * from reco.critics where item_id in (select item_id from x);'''

			self.db.execute(query, (str(user_id)), "Error querying data in get_recommendations")
			rows = self.db.fetchall()
			critics = []
			for row in rows:
				critics.append((row[0], row[1], row[2])) #(user_id, item_id, rating)
			
			if critics:	
				users = set([x[0] for x in critics])
				movies = set([x[1] for x in critics])

			sim_users = {item: self.user_similarity_distance(user_id, int(item), similarity_method) for item in users}

			estimated_rating = []
			for movie in movies:
				total = 0
				sim_sum = 0				
				for user in users:
					# did the user rated the movie?
					rating_filter = filter(lambda item : item[0] == int(user) and item[1] == int(movie), critics)
					if rating_filter:
						rating = rating_filter[0][2]
						sim_sum += sim_users[user]
						total += float(rating) * float(sim_users[user])
				estimated_rating.append((movie, float(total) / sim_sum))

			if estimated_rating:
				sorted_res = sorted(estimated_rating, key=lambda rating: rating[1], reverse=True)
				result = sorted_res[0:n]

			return result
		except:
			logger.error('Unexpected error:, ' + str(sys.exc_info()[0]) + ' from get_recommendations')


def main():
	db = None
	try:
		db = dbase()
		reco = recommender(db)
		print reco.user_similarity_distance(1, 2, 'Pearson')
		print reco.user_similarity_distance(1, 2, 'Euclidian')
		print reco.top_matches(1)
		print reco.get_recommendations(7, 3)
	except:
		logger.error('Unexpected error in main(): ' + str(sys.exc_info()[0]))
	
	finally:
		if db:
			db.close()

if __name__ == "__main__":
	main()
 